from google.cloud import storage, bigquery
from io import StringIO
import os
import pandas as pd
import sys
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.options.pipeline_options import PipelineOptions, WorkerOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.io.textio import ReadFromText
from apache_beam.transforms.util import BatchElements
import csv
import io

recon_consilidated_schema = {
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
        {"name": "ANALYTIC_RECORDS", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ANALYTIC_FAILED_RECORDS", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "RAW_COLUMN", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "CERT_COLUMN", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ANALYTIC_COLUMN", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "BQ_STATUS", "type": "STRING", "mode": "NULLABLE"},
        {"name": "BQ_FAILED", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ANALYTIC_col_sums", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "column_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sum_value", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "REASON", "type": "STRING", "mode": "NULLABLE"}
    ]
}

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
    "CAP_NVD_LCV": {
        "prefix": "LIGHTS_",
        "tables": {
            "Capder": "CAP_LCV_CAPDER",
            "Price": "CAP_LCV_PRICE",
            "OPTION": "CAP_LCV_OPTION"
        }
    },
    "CAP_NVD_CAR": {
        "prefix": "CAR_",
        "tables": {
            "Capder": "CAP_CAR_CAPDER",
            "Price": "CAP_CAR_PRICE",
            "OPTION": "CAP_CAR_OPTION"
        }
    },
    "REDBOOK": {
        "prefix": "LPRVAL",
        "tables": {"": "ALL_CAP_LPRVAL"}
    }
}

def load_env_config():
    import os
    from dotenv import load_dotenv, find_dotenv
    load_dotenv()

# Function to get schema from BigQuery
def get_bq_schema(project_id, dataset_id, table_id):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)
    schema = table.schema
    return {field.name: field.field_type for field in schema}

# Function to get column names from BigQuery schema
def get_bq_column_names(bq_schema, exclude_columns=[]):
    return [col for col in bq_schema if col not in exclude_columns]

# Function to get metadata record count - optimized to use streaming
def metadata_count(bucket_name, metadata_file):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    metadatafile = metadata_file[:-4] + ".metadata.csv"
    blob = bucket.blob(metadatafile)
    if not blob.exists():
        return 0

    content = blob.download_as_text()
    for line in content.splitlines():
        if 'Total Records' in line:
            return int(line.split(',')[1])
    return 0

# Function to check received folder - optimized
def check_received_folder(project_id, raw_bucket, cap_hpi_path, dataset, folder_base):
    client = storage.Client(project=project_id)
    received_dates = []
    received_path = f'{cap_hpi_path}/{dataset}/{folder_base}/Files/'
    received_list = client.list_blobs(raw_bucket, prefix=received_path)
    for blob in received_list:
        if blob.name.endswith('.csv') and 'metadata' not in blob.name and 'schema' not in blob.name:
            received_dates.append(blob.name.split('/')[-2])
    return max(set(received_dates)) if received_dates else None

# Function to list CSV files in the GCS folder - optimized
def list_files_in_folder(bucket_name, folder_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    files = [blob.name for blob in blobs if blob.name.endswith(".csv")]
    return files

# Stream-based record counting and column sum calculation
def process_csv_stream(bucket_name, file_path, zone, skip_header, bq_schema):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    
    # Get column names from schema
    column_names = get_bq_column_names(bq_schema)
    
    # Initialize counters
    record_count = 0
    column_count = len(column_names)
    column_sums = {col: 0 for col in column_names}
    
    # Stream the file content
    content = blob.download_as_text()
    csv_reader = csv.reader(io.StringIO(content))
    
    # Skip header if needed
    if skip_header:
        next(csv_reader, None)
    
    # Process each row
    for row in csv_reader:
        record_count += 1
        
        # Calculate column sums for ANAL zone
        if zone == "ANAL" and len(row) == len(column_names):
            for i, col in enumerate(column_names):
                try:
                    if bq_schema[col] in ["INTEGER", "FLOAT", "NUMERIC"]:
                        column_sums[col] += float(row[i])
                except (ValueError, IndexError):
                    pass
    
    # Format column sums for output
    formatted_sums = []
    if zone == "ANAL":
        formatted_sums = [{"column_name": col, "sum_value": str(column_sums[col])} 
                         for col in column_names if col in bq_schema]
    
    return record_count, column_count, formatted_sums

# Beam DoFn for processing files
class ProcessFileDoFn(beam.DoFn):
    def __init__(self, project_id, bq_dataset_id, dataset):
        self.project_id = project_id
        self.bq_dataset_id = bq_dataset_id
        self.dataset = dataset
        self.dataset_info = dataset_mapping.get(dataset, {})
        self.prefix = self.dataset_info.get("prefix", "")
        self.bq_table_map = self.dataset_info.get("tables", {})
        self.records = {}
    
    def process(self, element):
        file_path, zone, bucket_name = element
        
        filename = file_path.split("/")[-1]
        if not filename.startswith(self.prefix) or filename.endswith(("schema.csv", "metadata.csv")):
            return
        
        table_name_key = filename.replace(self.prefix, "").replace(".csv", "")
        bq_table_name = self.bq_table_map.get(table_name_key, "")
        
        if not bq_table_name:
            return
        
        bq_schema = get_bq_schema(self.project_id, self.bq_dataset_id, bq_table_name)
        
        skip_header = (zone == "ANALYTIC")
        record_count, column_count, column_sums = process_csv_stream(bucket_name, file_path, zone, skip_header, bq_schema)
        source_count = metadata_count(bucket_name, file_path) if zone == "RAW" else 0
        
        pick_date = file_path.split("/")[-2]
        folder_date = f"{pick_date[:4]}-{pick_date[4:6]}-{pick_date[6:]}"
        processed_time = datetime.now().strftime("%d/%m/%Y T %H:%M:%S")
        
        if filename not in self.records:
            self.records[filename] = {
                "DATASET": self.dataset,
                "FILE_DATE": folder_date,
                "PROCESSED_DATE_TIME": processed_time,
                "FILENAME": filename,
                "SOURCE_COUNT": source_count,
                "RAW_RECORDS": 0, "CERT_RECORDS": 0, "ANALYTIC_RECORDS": 0,
                "RAW_FAILED_RECORDS": 0, "CERT_FAILED_RECORDS": 0, "ANALYTIC_FAILED_RECORDS": 0,
                "RAW_COLUMN": 0, "CERT_COLUMN": 0, "ANALYTIC_COLUMN": 0,
                "ANALYTIC_col_sums": [],
                "BQ_STATUS": "", "BQ_FAILED": 0, "REASON": ""
            }
        
        if zone == "RAW":
            self.records[filename].update({
                "RAW_RECORDS": record_count, 
                "RAW_COLUMN": column_count, 
                "RAW_FAILED_RECORDS": source_count - record_count
            })
        elif zone == "CERT":
            self.records[filename].update({
                "CERT_RECORDS": record_count, 
                "CERT_COLUMN": column_count, 
                "CERT_FAILED_RECORDS": self.records[filename]["RAW_RECORDS"] - record_count
            })
        elif zone == "ANALYTIC":
            # Get BigQuery count
            QUERY = f"SELECT count(*) as count FROM `{self.project_id}.{bq_table_name}`"
            bq_client = bigquery.Client(self.project_id)
            query_job = bq_client.query(QUERY)
            bq_count = next(query_job.result()).count
            ana_count = record_count
            bq_status = "Match" if bq_count == ana_count else "Not Match"
            bq_failed_count = abs(ana_count - bq_count)
            reason = f"{bq_status}: ana_records({ana_count}) vs. BQ_count({bq_count})"
            
            self.records[filename].update({
                "ANALYTIC_RECORDS": record_count, 
                "ANALYTIC_COLUMN": column_count, 
                "ANALYTIC_FAILED_RECORDS": self.records[filename]["CERT_RECORDS"] - record_count, 
                "ANALYTIC_col_sums": column_sums, 
                "BQ_STATUS": bq_status, 
                "BQ_FAILED": bq_failed_count, 
                "REASON": reason
            })
    
    def finish_bundle(self):
        for record in self.records.values():
            yield record

# Beam DoFn for preparing file paths
class PrepareFilePathsDoFn(beam.DoFn):
    def process(self, element):
        folder_path, zone, bucket_name = element
        files = list_files_in_folder(bucket_name, folder_path)
        for file_path in files:
            yield (file_path, zone, bucket_name)

# Run Apache Beam pipeline
def run_pipeline(project_id, bq_dataset_id, dataset, folder_path, buckets_info, table_id):
    # Configure worker options to optimize memory usage
    worker_options = WorkerOptions(
        machine_type='n1-standard-4',  # Adjust based on your needs
        num_workers=2,                 # Adjust based on your needs
        max_num_workers=5,             # Adjust based on your needs
        disk_size_gb=50,               # Adjust based on your needs
        worker_harness_container_image='apache/beam_python3.7_sdk:2.30.0'  # Use a specific version
    )
    
    options = PipelineOptions()
    options.view_as(WorkerOptions).update_from_options(worker_options)
    
    with beam.Pipeline(options=options) as p:
        # Create a PCollection of (folder_path, zone, bucket_name) tuples
        bucket_info_pcoll = p | beam.Create([
            (folder_path, zone, bucket_name) 
            for zone, bucket_name in buckets_info.items()
        ])
        
        # Expand to file paths
        file_paths = (bucket_info_pcoll 
                     | beam.ParDo(PrepareFilePathsDoFn()))
        
        # Process files and generate records
        records = (file_paths 
                  | beam.ParDo(ProcessFileDoFn(project_id, bq_dataset_id, dataset)))
        
        # Write to BigQuery
        (records 
         | beam.io.WriteToBigQuery(
             table=f"{project_id}:{bq_dataset_id}.{table_id}", 
             schema=recon_consilidated_schema,
             create_disposition=BigQueryDisposition.CREATE_NEVER,
             write_disposition=BigQueryDisposition.WRITE_APPEND))

if __name__ == "__main__":
    env_config = load_env_config()
    dataset = sys.argv[1] if len(sys.argv) > 1 else "PRE_EMBARGO_LR"
    folder_base = "DAILY" if dataset in ["PRE_EMBARGO_LR","PRE_EMBARGO_J","CAP_NVD_CAR","CAP_NVD_LCV"] else "MONTHLY"

    project_id = env_config.get('PROJECT')
    raw_bucket = env_config.get('RAW_ZONE')
    cert_bucket = env_config.get('CERT_ZONE')
    analytic_bucket = env_config.get('ANALYTIC_ZONE')
    bq_dataset_id = env_config.get('BQ_DATASET')
    cap_hpi_path = env_config.get("CAP_PATH")
    table_id = env_config.get("RECON_TABLE")
    
    # Get the latest month folder
    month_folder = check_received_folder(project_id, raw_bucket, cap_hpi_path, dataset, folder_base)
    folder_path = f"{cap_hpi_path}/{folder_base}/"
    
    # Define bucket information
    buckets_info = {
        "RAW": raw_bucket, 
        "CERT": cert_bucket, 
        "ANALYTIC": analytic_bucket
    }
    
    # Run the optimized pipeline
    run_pipeline(project_id, bq_dataset_id, dataset, folder_path, buckets_info, table_id)
