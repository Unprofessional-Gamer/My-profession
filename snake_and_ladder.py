from google.cloud import storage, bigquery
import pandas as pd
from io import StringIO
from datetime import datetime
import apache_beam as beam
import logging
import sys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.gcsio import GcsIO

# Consolidated schema for BigQuery
consolidated_schema = {
    "fields": [
        {"name": "DATASET", "type": "STRING", "mode": "NULLABLE"},
        {"name": "FILE_DATE", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PROCESSED_DATE_TIME", "type": "STRING", "mode": "NULLABLE"},
        {"name": "FILENAME", "type": "STRING", "mode": "NULLABLE"},
        {"name": "SOURCE_COUNT", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "RAW_RECORDS", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "RAW_FAILED_RECORDS", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "CERT_RECORDS", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "CERT_FAILED_RECORDS", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ANAL_RECORDS", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ANAL_FAILED_RECORDS", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "RAW_COLUMN", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "CERT_COLUMN", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ANAL_COLUMN", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ANAL_col_sums", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "column_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sum_value", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "BQ_STATUS", "type": "STRING", "mode": "NULLABLE"},
        {"name": "BQ_FAILED", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "REASON", "type": "STRING", "mode": "NULLABLE"},
        {"name": "price_action", "type": "FLOAT", "mode": "NULLABLE"}  # New column
    ]
}

# Mapping of dataset names to file prefixes
dataset_to_prefix = {
    "BLACKBOOK": "CPRVAL",
    "REDBOOK": "LPRVAL",
    "GOLDBOOK": "CPRRVN",
    "MONITOR": "LPRRVN",
    "MONITOR_USED_CAR": "CPRRVU",
    "MONITOR_USED_LCV": "LPRRVU",
    "PRE_EMBARGO_J": "PRE_EMBARGO_J",
    "PRE_EMBARGO_K": "PRE_EMBARGO_K"
}

# Mapping of dataset names to BigQuery table names
dataset_to_bq_table = {
    "BLACKBOOK": "BLACKBOOK_BQ_TABLE",
    "REDBOOK": "REDBOOK_BQ_TABLE",
    "GOLDBOOK": "GOLDBOOK_BQ_TABLE",
    "MONITOR": "MONITOR_BQ_TABLE",
    "MONITOR_USED_CAR": "MONITOR_USED_CAR_BQ_TABLE",
    "MONITOR_USED_LCV": "MONITOR_USED_LCV_BQ_TABLE",
    "PRE_EMBARGO_J": {
        "files": ["CapDer.csv", "NDVGenericStatus.csv", "NVDModelYear.csv", "Capvehicles.csv"],
        "CapDer.csv": {"bq_table": "PRE_EMBARGO_J_CAPDER"},
        "NDVGenericStatus.csv": {"bq_table": "PRE_EMBARGO_J_NDVGENERICSTATUS"},
        "NVDModelYear.csv": {"bq_table": "PRE_EMBARGO_J_NVDMODELYEAR"},
        "Capvehicles.csv": {"bq_table": "PRE_EMBARGO_J_CAPVEHICLES"}
    },
    "PRE_EMBARGO_K": {
        "files": ["CapDer.csv", "NDVGenericStatus.csv", "NVDModelYear.csv", "Capvehicles.csv"],
        "CapDer.csv": {"bq_table": "PRE_EMBARGO_K_CAPDER"},
        "NDVGenericStatus.csv": {"bq_table": "PRE_EMBARGO_K_NDVGENERICSTATUS"},
        "NVDModelYear.csv": {"bq_table": "PRE_EMBARGO_K_NVDMODELYEAR"},
        "Capvehicles.csv": {"bq_table": "PRE_EMBARGO_K_CAPVEHICLES"}
    }
}

# Function to get schema from BigQuery
def get_bq_schema(project_id, dataset_id, table_id):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)
    schema = table.schema
    return {field.name: field.field_type for field in schema}

def get_bq_column_names(bq_schema, exclude_columns=[]):
    return [col for col in bq_schema if col not in exclude_columns]

# Function to get the total records from the metadata file
def metadata_count(bucket_name, metadata_file):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    metadatafile = metadata_file[:-4] + ".metadata.csv"
    blob = bucket.blob(metadatafile)
    content = blob.download_as_text()

    df = pd.read_csv(StringIO(content))

    total_records = df[df['Key'] == 'Total Records']['Value'].values[0]
    return int(total_records)

# Function to list CSV files in the GCS folder
def list_files_in_folder(bucket_name, folder_path, dataset):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    files = [blob.name for blob in blobs if blob.name.endswith(".csv")]
    return files

def check_received_folder(project_id, raw_bucket):
    client = storage.Client(project=project_id)
    received_dates = []
    received_path = "abcn/recived_path/files"
    received_list = client.list_blobs(raw_bucket, prefix=received_path)
    for blob in received_list:
        if blob.name.endswith('.csv') and 'metadata' not in blob.name and 'schema' not in blob.name:
            received_dates.append(blob.name.split('/')[-2])
            return max(set(received_dates))

# Function to read CSV file from GCS, excluding specific columns, and get record count and column sums
def get_record_count(bucket_name, file_path, zone, skip_header, bq_schema):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()

    if zone in ["RAW", "CERT"]:
        column_names = get_bq_column_names(bq_schema, exclude_columns=['MONTH'])
    else:
        column_names = get_bq_column_names(bq_schema)

    df = pd.read_csv(StringIO(content), header=None, names=column_names, low_memory=False)
    column_count = len(df.columns)

    if zone in ["RAW", "CERT"]:
        record_count = len(df)
    else:
        record_count = len(df) - skip_header
    return record_count, column_count

# Function to dynamically get column sums based on BigQuery schema and CSV data
def get_col_sum_dynamic(bucket_name, file_path, skip_header, zone, bq_schema):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()

    # Read CSV into pandas DataFrame
    df = pd.read_csv(StringIO(content), header=0 if skip_header else None, low_memory=False)

    numeric_columns = df.select_dtypes(include=["int64", "float64"]).columns
    column_sums = []

    # Only calculate sums for ANAL zone
    if zone == "ANAL":
        for col in numeric_columns:
            if col in bq_schema and bq_schema[col] in ["INTEGER", "FLOAT"]:
                column_sums.append({
                    "column_name": col,
                    "sum_value": str(df[col].sum())
                })

    return column_sums

# Function to process the analytical zone and compare records with BigQuery
def analytic_to_bq_checking(ana_bucket_name, dataset, folder_base, project_id, records, bq_table_name):
    # Initialize GCS and BigQuery clients
    client = storage.Client(project_id)
    fs = GcsIO(client)
    ana_bucket = client.bucket(ana_bucket_name)
    blob_list_ana = client.list_blobs(ana_bucket, prefix=f"EXTERNAL/MFVS/PRICING/CAP-HPI/{dataset}/{folder_base}/RECEIVED/{month_folder}")

    # Loop through the blobs in the bucket
    for blob_ana in blob_list_ana:
        filename = blob_ana.name.split('/')[-1]

        try:
            # Check if the file name contains the specified substrings
            if any(x in blob_ana.name for x in [dataset]):
                with fs.open(f"gs://{ana_bucket_name}/{blob_ana.name}", mode='r') as pf:
                    ana_lines = pf.readlines()

                # Count the number of lines in the file (assuming the first line is a header)
                if dataset not in ["CAP_NVD_CAR", "CAP_NVD_LCV", "PRE_EMBARGO_J", "PRE_EMBARGO_K"]:
                    ana_count = len(ana_lines) - 1
                    logging.info(f"File lines for {filename} is {ana_count}")

                    # Extract month column from the file name and format it for the query
                    month_col = datetime.datetime.strptime(blob_ana.name.split('/')[-2], '%Y%m%d').strftime('%Y-%m-%d')

                    # Query the correct BigQuery table
                    QUERY = f"""
                        SELECT count(*) as count FROM `{bq_table_name}`
                        WHERE MONTH = '{month_col}'
                    """
                    bq_client = bigquery.Client(project_id)
                    query_job = bq_client.query(QUERY)  # API request
                    rows = query_job.result()

                    # Compare record counts
                    for row in rows:
                        bq_count = row.count
                        logging.info(f"BQ table count for {month_col} is {bq_count}")

                        if bq_count == ana_count:
                            bq_status = "Match"
                            bq_failed_count = 0
                            mismatch = ana_count - bq_count
                            reason = (f"{bq_status} : Bcoz ana_records({ana_count}) == BQ_count({bq_count}) = mismatch({mismatch})")
                            logging.info(f"Data count matches for {filename} in analytical zone.")
                        else:
                            bq_status = "Not Match"
                            bq_failed_count = abs(ana_count - bq_count)
                            mismatch = ana_count - bq_count
                            reason = (f"{bq_status} : Bcoz ana_records({ana_count}) != BQ_count({bq_count}) = mismatch({mismatch})")
                            logging.info(f"Data count does not match for {filename} in analytical zone.")

                        # Update the records dictionary with BQ comparison result
                        if filename in records:
                            records[filename]["BQ_STATUS"] = bq_status
                            records[filename]["BQ_FAILED"] = bq_failed_count
                            records[filename]["REASON"] = reason

        except Exception as e:
            # Log the error with the filename
            logging.error(f"Error processing Analytical zone file: {filename} from {dataset}: {str(e)}")
            raise

    return records

# Main processing function that integrates all components
def process_files(buckets_info, folder_path, dataset, project_id, dataset_id):
    records = {}
    file_prefix = dataset_to_prefix.get(dataset, "")

    # Get the list of files and their corresponding BigQuery table names
    if dataset in ["PRE_EMBARGO_J", "PRE_EMBARGO_K"]:
        file_bq_mapping = dataset_to_bq_table[dataset]
        files_to_match = file_bq_mapping["files"]  # Files to match (e.g., ['CapDer.csv', 'NDVGenericStatus.csv', ...])
    else:
        files_to_match = None  # For other datasets, process all files

    for zone, bucket_name in buckets_info.items():
        files_in_folder = list_files_in_folder(bucket_name, folder_path, dataset)

        for file_path in files_in_folder:
            filename = file_path.split("/")[-1]

            # Skip files that don't match the criteria
            if filename.endswith(("schema.csv", "metadata.csv")):
                continue

            # For PRE_EMBARGO_J or PRE_EMBARGO_K, check if the file ends with any of the specified filenames
            if dataset in ["PRE_EMBARGO_J", "PRE_EMBARGO_K"]:
                matched_file = None
                for file_to_match in files_to_match:
                    if filename.endswith(file_to_match):
                        matched_file = file_to_match
                        break

                if not matched_file:
                    logging.warning(f"Skipping file {filename} as it does not match any specified filenames for dataset {dataset}.")
                    continue

                # Get the corresponding BigQuery table name
                bq_table_name = file_bq_mapping[matched_file]["bq_table"]
            else:
                # For other datasets, use the default BigQuery table name
                bq_table_name = dataset_to_bq_table.get(dataset, "")

            # Fetch the schema for the corresponding BigQuery table
            bq_schema = get_bq_schema(project_id, dataset_id, bq_table_name)

            # Common processing for all zones
            skip_header = (zone == "ANAL")
            record_count, column_count = get_record_count(bucket_name, file_path, zone, skip_header, bq_schema)

            # For PRE_EMBARGO_J or PRE_EMBARGO_K, use raw zone record count as source count
            if dataset in ["PRE_EMBARGO_J", "PRE_EMBARGO_K"] and zone == "RAW":
                source_count = record_count
            elif zone == "RAW":
                source_count = metadata_count(bucket_name, file_path)

            # File metadata
            pick_date = file_path.split("/")[-2]
            folder_date = f"{pick_date[:4]}-{pick_date[4:6]}-{pick_date[6:]}"
            processed_time = datetime.now().strftime("%d/%m/%Y T %H:%M:%S")

            if filename not in records:
                records[filename] = {
                    "DATASET": dataset,
                    "FILE_DATE": folder_date,
                    "PROCESSED_DATE_TIME": processed_time,
                    "FILENAME": filename,
                    "SOURCE_COUNT": source_count if zone == "RAW" else 0,
                    "RAW_RECORDS": 0, "CERT_RECORDS": 0, "ANAL_RECORDS": 0,
                    "RAW_FAILED_RECORDS": 0, "CERT_FAILED_RECORDS": 0, "ANAL_FAILED_RECORDS": 0,
                    "RAW_COLUMN": 0, "CERT_COLUMN": 0, "ANAL_COLUMN": 0,
                    "ANAL_col_sums": [],
                    "BQ_STATUS": "", "BQ_FAILED": 0, "REASON": "",
                    "price_action": 0.0
                }

            # Zone-specific processing
            if zone == "RAW":
                records[filename].update({
                    "RAW_RECORDS": record_count,
                    "RAW_COLUMN": column_count,
                    "SOURCE_COUNT": source_count,
                    "RAW_FAILED_RECORDS": records[filename]["SOURCE_COUNT"] - record_count
                })
            elif zone == "CERT":
                records[filename].update({
                    "CERT_RECORDS": record_count,
                    "CERT_COLUMN": column_count,
                    "CERT_FAILED_RECORDS": records[filename]["RAW_RECORDS"] - record_count
                })
            elif zone == "ANAL":
                records[filename].update({
                    "ANAL_RECORDS": record_count,
                    "ANAL_COLUMN": column_count,
                    "ANAL_FAILED_RECORDS": records[filename]["CERT_RECORDS"] - record_count,
                    "ANAL_col_sums": get_col_sum_dynamic(bucket_name, file_path, skip_header, zone, bq_schema)
                })

    # Perform analytical zone checking for each file
    if dataset in ["PRE_EMBARGO_J", "PRE_EMBARGO_K"]:
        for filename, record in records.items():
            matched_file = None
            for file_to_match in files_to_match:
                if filename.endswith(file_to_match):
                    matched_file = file_to_match
                    break

            if matched_file:
                bq_table_name = file_bq_mapping[matched_file]["bq_table"]
                records = analytic_to_bq_checking(buckets_info["ANAL"], dataset, folder_base, project_id, records, bq_table_name)
    else:
        bq_table_name = dataset_to_bq_table.get(dataset, "")
        if bq_table_name:
            records = analytic_to_bq_checking(buckets_info["ANAL"], dataset, folder_base, project_id, records, bq_table_name)
    return list(records.values())


# Beam pipeline to process and write to BigQuery
def run_pipeline(project_id, folder_path, raw_bucket, cert_bucket, anal_bucket, bq_dataset_id, bq_table_name, dataset):
    options = PipelineOptions(
        project=project_id,
        runner="DataflowRunner",
        region="europe-west2",
        job_name=f"reconciliation_{dataset}".lower(),
        staging_location=f"gs://{raw_bucket}/staging",
        temp_location=f"gs://{raw_bucket}/temp",
        num_workers=2,
        max_num_workers=8,
        use_public_ips=False,
        autoscaling_algorithm="THROUGHPUT_BASED",
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        records = process_files(
            {"RAW": raw_bucket, "CERT": cert_bucket, "ANAL": anal_bucket},
            folder_path,
            dataset,
            project_id,
            bq_dataset_id,
            bq_table_name
        )
        output = p | beam.Create(records)
        output | beam.io.WriteToBigQuery(table=bq_table_name, dataset=bq_dataset_id, schema=consolidated_schema, project=project_id, custom_gcs_temp_location=f"gs://{raw_bucket}/temp", write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == "__main__":
    # Determine the dataset from command-line argument, defaulting to "BLACKBOOK"
    dataset = sys.argv[1] if len(sys.argv) > 1 else "REDBOOK"

    # Use "DAILY" folder for specific datasets and "MONTHLY" for others
    folder_base = "DAILY" if dataset in ["CAP_NVD_CAR", "CAP_NVD_LCV", "PRE_EMBARGO_J", "PRE_EMBARGO_LR"] else "MONTHLY"

    project_id = "your_project_id"
    bq_dataset_id = "your_dataset_id"
    bq_table_name = "consolidated_record_report"

    # Bucket IDs
    raw_bucket = "raw_001uejeu_uej"
    cert_bucket = "cert_001uejeu_uej"
    anal_bucket = "anal_001uejeu_uej"

    month_folder = check_received_folder(project_id, raw_bucket)
    folder_path = f"EXTERNAL/MFVS/PRICING/CAP-HPI/{dataset}/{folder_base}/RECEIVED/{month_folder}/"

    print(f"Processing files for dataset: {dataset} in folder: {folder_path}")
    print("************************ Recon")