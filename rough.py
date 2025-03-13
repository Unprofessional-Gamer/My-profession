# Main function to process files
def process_files(_, buckets_info, dataset, project_id, bq_dataset_id):
    records = {}
    dataset_info = dataset_mapping.get(dataset, {})
    prefix = dataset_info.get("prefix", "")
    bq_table_map = dataset_info.get("tables", {})

    for zone, bucket_name in buckets_info.items():
        files = list_files_in_folder(bucket_name, folder_path)  # Use global folder_path

        for file in files:
            filename = file.split("/")[-1]
            if not filename.startswith(prefix) or filename.endswith(("schema.csv", "metadata.csv")):
                continue

            table_name_key = filename.replace(prefix, "").replace(".csv", "")
            bq_table_name = bq_table_map.get(table_name_key, "")

            if not bq_table_name:
                continue  # Skip if no BQ table found

            bq_schema = get_bq_schema(project_id, bq_dataset_id, bq_table_name)

            skip_header = (zone == "ANAL")
            record_count, column_count, column_sums = get_record_count_and_sums(bucket_name, file, zone, skip_header, bq_schema)
            source_count = metadata_count(bucket_name, file) if zone == "RAW" else 0

            pick_date = file.split("/")[-2]
            folder_date = f"{pick_date[:4]}-{pick_date[4:6]}-{pick_date[6:]}"
            processed_time = datetime.now().strftime("%d/%m/%Y T %H:%M:%S")

            if filename not in records:
                records[filename] = {
                    "DATASET": dataset,
                    "FILE_DATE": folder_date,
                    "PROCESSED_DATE_TIME": processed_time,
                    "FILENAME": filename,
                    "SOURCE_COUNT": source_count,
                    "RAW_RECORDS": 0, "CERT_RECORDS": 0, "ANAL_RECORDS": 0,
                    "RAW_FAILED_RECORDS": 0, "CERT_FAILED_RECORDS": 0, "ANAL_FAILED_RECORDS": 0,
                    "RAW_COLUMN": 0, "CERT_COLUMN": 0, "ANAL_COLUMN": 0,
                    "ANAL_col_sums": [],
                    "BQ_STATUS": "", "BQ_FAILED": 0, "REASON": ""
                }

            if zone == "RAW":
                records[filename].update({"RAW_RECORDS": record_count, "RAW_COLUMN": column_count, "RAW_FAILED_RECORDS": source_count - record_count})
            elif zone == "CERT":
                records[filename].update({"CERT_RECORDS": record_count, "CERT_COLUMN": column_count, "CERT_FAILED_RECORDS": records[filename]["RAW_RECORDS"] - record_count})
            elif zone == "ANAL":
                records[filename].update({"ANAL_RECORDS": record_count, "ANAL_COLUMN": column_count, "ANAL_FAILED_RECORDS": records[filename]["CERT_RECORDS"] - record_count, "ANAL_col_sums": column_sums})

                # BigQuery consistency check moved here
                client = storage.Client(project_id)
                fs = GcsIO(client)
                blob_list_ana = client.list_blobs(bucket_name)

                for blob_ana in blob_list_ana:
                    ana_filename = blob_ana.name.split('/')[-1]
                    if dataset not in ana_filename:
                        continue

                    with fs.open(f"gs://{bucket_name}/{blob_ana.name}", mode='r') as pf:
                        ana_lines = pf.readlines()
                    ana_count = len(ana_lines) - 1  # Excluding header

                    QUERY = f"SELECT count(*) as count FROM `{project_id}.{bq_table_name}`"
                    bq_client = bigquery.Client(project_id)
                    query_job = bq_client.query(QUERY)
                    bq_count = next(query_job.result()).count

                    bq_status = "Match" if bq_count == ana_count else "Not Match"
                    bq_failed_count = abs(ana_count - bq_count)
                    reason = f"{bq_status}: ana_records({ana_count}) vs. BQ_count({bq_count})"

                    if filename in records:
                        records[filename].update({"BQ_STATUS": bq_status, "BQ_FAILED": bq_failed_count, "REASON": reason})

    return list(records.values())
