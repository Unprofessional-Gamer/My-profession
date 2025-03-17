from google.cloud import storage, bigquery
from io import StringIO
import pandas as pd
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

# Dataset mapping configuration
DATASET_MAPPING = {
    "PRE_EMBARGO": {
        "prefix": "Pre_emabrgo_land_rover_",
        "tables": {
            "Capder": "PRE_EMBARGO_LR_CAPDER",
            "Price": "PRE_EMBARGO_LR_PRICE",
            "OPTION": "PRE_EMBARGO_LR_OPTION"
        }
    }
}

# Function to fetch schema and record count from BigQuery
def get_bq_schema_and_count(project_id, dataset_id, table_id):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)
    schema = {field.name: field.field_type for field in table.schema}
    query_job = bq_client.query(f"SELECT COUNT(*) AS count FROM `{project_id}.{dataset_id}.{table_id}`")
    record_count = next(query_job.result()).count
    return schema, record_count

# Function to process a single file
def process_file(bucket_name, file_path, project_id, dataset_name, bq_dataset_id):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()

    # Extract filename and table mapping
    filename = file_path.split("/")[-1]
    dataset_info = DATASET_MAPPING[dataset_name]
    prefix = dataset_info["prefix"]
    table_key = filename.replace(prefix, "").replace(".csv", "")
    bq_table_name = dataset_info["tables"].get(table_key)

    if not bq_table_name:
        return None  # Skip if no matching BigQuery table

    # Fetch schema and record count from BigQuery
    bq_schema, bq_record_count = get_bq_schema_and_count(project_id, bq_dataset_id, bq_table_name)

    # Read CSV file into DataFrame
    column_names = list(bq_schema.keys())
    df = pd.read_csv(StringIO(content), names=column_names)

    # Compute record count and column sums
    record_count = len(df)
    column_sums = [{"column": col, "sum": str(df[col].sum())} for col in df.select_dtypes(include=["int64", "float64"]).columns]

    # Prepare reconciliation data
    file_date = datetime.now().strftime("%Y-%m-%d")
    processed_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    bq_status = "Match" if record_count == bq_record_count else "Not Match"
    
    return {
        "DATASET": dataset_name,
        "FILE_DATE": file_date,
        "PROCESSED_DATE_TIME": processed_time,
        "FILENAME": filename,
        "SOURCE_COUNT": record_count,
        "RAW_RECORDS": 0,
        "CERT_RECORDS": 0,
        "ANALYTIC_RECORDS": record_count,
        "RAW_FAILED_RECORDS": 0,
        "CERT_FAILED_RECORDS": 0,
        "ANALYTIC_FAILED_RECORDS": 0,
        "RAW_COLUMN": 0,
        "CERT_COLUMN": 0,
        "ANALYTIC_COLUMN": len(df.columns),
        "ANALYTIC_col_sums": column_sums,
        "BQ_STATUS": bq_status,
        "BQ_FAILED": abs(record_count - bq_record_count),
        "REASON": f"{bq_status}: file_records({record_count}) vs. BQ_count({bq_record_count})"
    }

# Function to process all files in a folder
def process_files(bucket_name, folder_path, project_id, dataset_name, bq_dataset_id):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_path)
    
    records = []
    
    for blob in blobs:
        if blob.name.endswith(".csv") and not blob.name.endswith(("schema.csv", "metadata.csv")):
            record = process_file(bucket_name, blob.name, project_id, dataset_name, bq_dataset_id)
            if record:
                records.append(record)
    
    return records

# Apache Beam pipeline to write records to BigQuery
def run_pipeline():

