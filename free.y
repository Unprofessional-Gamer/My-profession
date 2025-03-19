def process_files(_, buckets_info, folder_path, dataset, project_id, bq_dataset_id):
    records = {}
    dataset_info = dataset_mapping.get(dataset, {})

    bq_table_name = None  # Ensure it's initialized

    for zone, bucket_name in buckets_info.items():
        files = list_files_in_folder(bucket_name, folder_path)

        for file in files:
            filename = file.split("/")[-1]  # Extract only the filename

            # 🔹 Remove the prefix from the filename to match dataset_mapping
            cleaned_filename = filename.replace("PreEmbargoed_Land_Rover_", "")

            # 🔹 Check against cleaned filenames
            if cleaned_filename not in dataset_info.get("files", []):
                print(f"Skipping {filename} because it is not in dataset_mapping")
                continue

            # 🔹 Fetch BQ table name from cleaned filename
            bq_table_name = dataset_info.get(cleaned_filename, {}).get("bq_table")

            if not bq_table_name:
                print(f"Skipping {filename} because no BQ table is mapped.")
                continue

            print(f"Processing file: {filename} with BQ table: {bq_table_name}")

            bq_schema = get_bq_schema(project_id, bq_dataset_id, bq_table_name)
            skip_header = (zone == "ANALYTIC")
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
                    "RAW_RECORDS": 0,
                    "CERT_RECORDS": 0,
                    "ANALYTIC_RECORDS": 0,
                    "RAW_FAILED_RECORDS": 0,
                    "CERT_FAILED_RECORDS": 0,
                    "ANALYTIC_FAILED_RECORDS": 0,
                    "RAW_COLUMN": 0,
                    "CERT_COLUMN": 0,
                    "ANALYTIC_COLUMN": 0,
                    "ANALYTIC_col_sums": [],
                    "BQ_STATUS": "",
                    "BQ_FAILED": 0,
                    "REASON": "",
                }

            if zone == "RAW":
                records[filename].update(
                    {
                        "RAW_RECORDS": record_count,
                        "RAW_COLUMN": column_count,
                        "RAW_FAILED_RECORDS": source_count - record_count,
                    }
                )
            elif zone == "CERT":
                records[filename].update(
                    {
                        "CERT_RECORDS": record_count,
                        "CERT_COLUMN": column_count,
                        "CERT_FAILED_RECORDS": records[filename]["RAW_RECORDS"] - record_count,
                    }
                )
            elif zone == "ANALYTIC":
                records[filename].update(
                    {
                        "ANALYTIC_RECORDS": record_count,
                        "ANALYTIC_COLUMN": column_count,
                        "ANALYTIC_FAILED_RECORDS": records[filename]["CERT_RECORDS"] - record_count,
                        "ANALYTIC_col_sums": column_sums,
                    }
                )

    if bq_table_name:
        records = analytical_to_bq_checking(buckets_info["ANALYTIC"], dataset, project_id, records, bq_table_name)

    return list(records.values())
