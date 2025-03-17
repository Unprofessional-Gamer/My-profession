from google.cloud import storage, bigquery
from io import StringIO
import os
import pandas as pd
import sys
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import schema.book_schema as reco

# Mapping dataset names, filename prefixes, and BigQuery table names
dataset_mapping = {
    "PRE_EMBARGO": {
        "prefix": "Pre_emabrgo_land_rover_",
        "tables": {
            "Capder": "PRE_EMBARGO_LR_CAPDER",
            "Price": "PRE_EMBARGO_LR_PRICE",
            "OPTION": "PRE_EMBARGO_LR_OPTION"
        }
    },
    "BLACKBOOK": {
        "prefix": "CPRVAL",
        "tables": {"": "ALL_CAP_CPRVAL"}
    },
    "REDBOOK": {
        "prefix": "LPRVAL",
        "tables": {"": "ALL_CAP_LPRVAL"}
    }
}

def load_env_config():
    import os
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())

# Function to get schema from BigQuery and record count
def get_bq_schema_and_count(project_id, dataset_id, table_id):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    try:
        table = bq_client.get_table(table_ref)
        schema = {field.name: field.field_type for field in table.schema}
    except Exception as e:
        logging.error(f"Error getting table schema: {e}")
        return None, None

    try:
        query = f"SELECT COUNT(*) as count FROM `{project_id}.{dataset_id}.{table_id}`"
        query_job = bq_client.query(query)
        bq_count = next(query_job.result()).count
    except Exception as e:
        logging.error(f"Error querying BigQuery: {e}")
        return schema, None

    return schema, bq_count

# Function to get column names from BigQuery schema
def get_bq_column_names(bq_schema, exclude_columns=[]):
    return [col for col in bq_schema if col not in exclude_columns]

# Function to get metadata record count
def metadata_count(bucket_name, metadata_file):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    metadatafile = metadata_file[:-4] + ".metadata.csv"
    blob = bucket.blob(metadatafile)
    if not blob.exists():
        return 0

    content = blob.download_as_text()
    df = pd.read_csv(StringIO(content))
    return int(df[df['Key'] == 'Total Records']['Value'].values[0])

def check_received_folder(project_id, raw_bucket, cap_hpi_path, dataset, folder_base):
    client = storage.Client(project=project_id)
    received_dates = []
    received_path = f'{cap_hpi_path}/{dataset}/{folder_base}/Files/'
    received_list = client.list_blobs(raw_bucket, prefix=received_path)
    for blob in received_list:
        if blob.name.endswith('.csv') and 'metadata' not in blob.name and 'schema' not in blob.name:
            received_dates.append(blob.name.split('/')[-2])
            return max(set(received_dates))
    return None  # Return None if no suitable file is found

# Function to list CSV files in the GCS folder
def list_files_in_folder(bucket_name, folder_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    files = [blob.name for blob in blobs if blob.name.endswith(".csv")]
    return files

# Function to read CSV file and get record count & column sums
def get_record_count_and_sums(bucket_name, file_path, zone, skip_header, bq_schema):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()

    column_names = get_bq_column_names(bq_schema)
    df = pd.read_csv(StringIO(content), header=None, names=column_names, low_memory=False)

    column_count = len(df.columns)
    record_count = len(df) - (1 if skip_header else 0)

    # Compute column sums if it's ANAL zone
    column_sums = []
    if zone == "ANAL":
        numeric_columns = df.select_dtypes(include=["int64", "float64"]).columns
        column_sums = [{"column_name": col, "sum_value": str(df[col].sum())} for col in numeric_columns if col in bq_schema]

    return record_count, column_count, column_sums

# Main function to process files
def process_files(_, buckets_info, folder_path, dataset, project_id, bq_dataset_id):
    records = []
    dataset_info = dataset_mapping.get(dataset, {})
    prefix = dataset_info.get("prefix", "")
    bq_table_map = dataset_info.get("tables", {})

    for zone, bucket_name in buckets_info.items():
        files = list_files_in_folder(bucket_name, folder_path)

        for file in files:
            filename = file.split("/")[-1]
            if not filename.startswith(prefix) or filename.endswith(("schema.csv", "metadata.csv")):
                continue

            table_name_key = filename.replace(prefix, "").replace(".csv", "").lower()
            bq_table_name = bq_table_map.get(table_name_key, "")

            if not bq_table_name:
                logging.warning(f"No BigQuery table mapping found for file: {filename}")
                continue  # Skip this file if table name is not found

            bq_schema, bq_count = get_bq_schema_and_count(project_id, bq_dataset_id, bq_table_name)

            if bq_schema is None:
                logging.warning(f"Skipping {filename} due to missing schema")
                continue

            skip_header = (zone == "ANAL")
            record_count, column_count, column_sums = get_record_count_and_sums(bucket_name, file, zone, skip_header, bq_schema)
            source_count = metadata_count(bucket_name, file) if zone == "RAW" else 0

            pick_date = file.split("/")[-2]
            folder_date = f"{pick_date[:4]}-{pick_date[4:6]}-{pick_date[6:]}"
            processed_time = datetime.now().strftime("%d/%m/%Y T %H:%M:%S")

            record = {
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
                record.update({"RAW_RECORDS": record_count, "RAW_COLUMN": column_count, "RAW_FAILED_RECORDS": source_count - record_count})
            elif zone == "CERT":
                record.update({"CERT_RECORDS": record_count, "CERT_COLUMN": column_count, "CERT_FAILED_RECORDS": record["RAW_RECORDS"] - record_count})
            elif zone == "ANAL":
                record.update({"ANAL_RECORDS": record_count, "ANAL_COLUMN": column_count, "ANAL_FAILED_RECORDS": record["CERT_RECORDS"] - record_count, "ANAL_col_sums": column_sums})

                # Perform BQ consistency check here
                if bq_count is not None:
                    status = "Match" if bq_count == record_count else "Not Match"
                    failed_count = abs(bq_count - record_count)
                    reason = f"{status}: ana_records({record_count}) vs. BQ_count({bq_count})"
                    record.update({
                        "BQ_STATUS": status,
                        "BQ_FAILED": failed_count,
                        "REASON": reason
                    })
                else:
                    record.update({"BQ_STATUS": "Failed", "REASON": "Could not retrieve BQ count"})

            records.append(record)
    return records

# Run Apache Beam pipeline
def run_pipeline(buckets_info, folder_path, dataset, project_id, bq_dataset_id, recon_table):
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (p | beam.Impulse()
           | beam.Map(process_files, buckets_info, folder_path, dataset, project_id, bq_dataset_id)
           | beam.io.WriteToBigQuery(
               table=recon_table,
               schema="SCHEMA_AUTODETECT",
               write_disposition=BigQueryDisposition.WRITE_APPEND))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_env_config()
    dataset = sys.argv[1] if len(sys.argv) > 1 else "PRE_EMBARGO"
    folder_base = "DAILY" if dataset in ["PRE_EMBARGO", "PRE_EMBARGO_J"] else "MONTHLY"
    project_id = os.environ.get('PROJECT')
    raw_bucket = os.environ.get('RAW_ZONE')
    cert_bucket = os.environ.get('CERT_ZONE')
    analytic_bucket = os.environ.get('ANALYTIC_ZONE')
    bq_dataset_id = os.environ.get('BQ_DATASET')
    cap_hpi_path = os.environ.get("CAP_PATH")
    table_id = os.environ.get("RECON_TABLE")  # Make sure RECON_TABLE is properly set
    recon_table = f"{project_id}:{bq_dataset_id}.{table_id}"
    buckets_info = {"RAW": raw_bucket, "CERT": cert_bucket, "ANAL": analytic_bucket}

    # Call check_received_folder correctly
    month_folder = check_received_folder(project_id, raw_bucket, cap_hpi_path, dataset, folder_base)

    if month_folder:
        folder_path = f"{cap_hpi_path}/{folder_base}/Files/{month_folder}/"
    else:
        logging.warning(f"No valid folder found for dataset {dataset}. Exiting.")
        sys.exit(1)

    logging.info(f"Using folder path: {folder_path}")
    run_pipeline(buckets_info, folder_path, dataset, project_id, bq_dataset_id, recon_table)
