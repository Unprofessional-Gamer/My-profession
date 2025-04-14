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
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.io.textio import ReadFromText
import csv
import json

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
    load_dotenv

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
    return max(set(received_dates)) if received_dates else None

# Beam DoFn classes for processing
class GetFileInfoFn(beam.DoFn):
    def __init__(self, dataset, prefix, bq_table_map):
        self.dataset = dataset
        self.prefix = prefix
        self.bq_table_map = bq_table_map
        
    def process(self, file_path):
        filename = file_path.split("/")[-1]
        if not filename.startswith(self.prefix) or filename.endswith(("schema.csv", "metadata.csv")):
            return
            
        table_name_key = filename.replace(self.prefix, "").replace(".csv", "")
        bq_table_name = self.bq_table_map.get(table_name_key, "")
        
        if not bq_table_name:
            return
            
        pick_date = file_path.split("/")[-2]
        folder_date = f"{pick_date[:4]}-{pick_date[4:6]}-{pick_date[6:]}"
        
        yield {
            "file_path": file_path,
            "filename": filename,
            "folder_date": folder_date,
            "bq_table_name": bq_table_name
        }

class ProcessFileFn(beam.DoFn):
    def __init__(self, zone, project_id, bq_dataset_id, dataset):
        self.zone = zone
        self.project_id = project_id
        self.bq_dataset_id = bq_dataset_id
        self.dataset = dataset
        
    def process(self, file_info):
        file_path = file_info["file_path"]
        filename = file_info["filename"]
        folder_date = file_info["folder_date"]
        bq_table_name = file_info["bq_table_name"]
        
        # Get schema for this file
        bq_schema = get_bq_schema(self.project_id, self.bq_dataset_id, bq_table_name)
        column_names = get_bq_column_names(bq_schema)
        
        # Process file in chunks using Beam's native file reading
        storage_client = storage.Client()
        bucket_name = file_path.split("/")[0]
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        # Get record count and column info
        record_count = 0
        column_sums = {}
        
        # Read file in chunks to avoid memory issues
        content = blob.download_as_text()
        reader = csv.reader(StringIO(content))
        
        # Skip header if needed
        skip_header = (self.zone == "ANALYTIC")
        if skip_header:
            next(reader, None)
            
        # Process rows
        for row in reader:
            record_count += 1
            
            # For ANALYTIC zone, calculate column sums
            if self.zone == "ANALYTIC":
                for i, col_name in enumerate(column_names):
                    if i < len(row):
                        try:
                            value = float(row[i])
                            column_sums[col_name] = column_sums.get(col_name, 0) + value
                        except (ValueError, TypeError):
                            pass
        
        # Format column sums for output
        formatted_sums = []
        if self.zone == "ANALYTIC":
            for col_name, sum_value in column_sums.items():
                formatted_sums.append({
                    "column_name": col_name,
                    "sum_value": str(sum_value)
                })
        
        # Get source count for RAW zone
        source_count = 0
        if self.zone == "RAW":
            source_count = metadata_count(bucket_name, file_path)
        
        # Get BigQuery count for ANALYTIC zone
        bq_count = 0
        bq_status = ""
        bq_failed_count = 0
        reason = ""
        
        if self.zone == "ANALYTIC":
            QUERY = f"SELECT count(*) as count FROM `{self.project_id}.{bq_table_name}`"
            bq_client = bigquery.Client(self.project_id)
            query_job = bq_client.query(QUERY)
            bq_count = next(query_job.result()).count
            ana_count = record_count
            bq_status = "Match" if bq_count == ana_count else "Not Match"
            bq_failed_count = abs(ana_count - bq_count)
            reason = f"{bq_status}: ana_records({ana_count}) vs. BQ_count({bq_count})"
        
        # Create record
        processed_time = datetime.now().strftime("%d/%m/%Y T %H:%M:%S")
        
        record = {
            "DATASET": self.dataset,
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
            "ANALYTIC_col_sums": formatted_sums,
            "BQ_STATUS": bq_status,
            "BQ_FAILED": bq_failed_count,
            "REASON": reason
        }
        
        # Update zone-specific fields
        if self.zone == "RAW":
            record.update({
                "RAW_RECORDS": record_count,
                "RAW_COLUMN": len(column_names),
                "RAW_FAILED_RECORDS": source_count - record_count
            })
        elif self.zone == "CERT":
            record.update({
                "CERT_RECORDS": record_count,
                "CERT_COLUMN": len(column_names),
                "CERT_FAILED_RECORDS": 0  # Will be updated in the next step
            })
        elif self.zone == "ANALYTIC":
            record.update({
                "ANALYTIC_RECORDS": record_count,
                "ANALYTIC_COLUMN": len(column_names),
                "ANALYTIC_FAILED_RECORDS": 0,  # Will be updated in the next step
                "BQ_STATUS": bq_status,
                "BQ_FAILED": bq_failed_count,
                "REASON": reason
            })
        
        yield record

class CombineRecordsFn(beam.CombineFn):
    def create_accumulator(self):
        return {}
    
    def add_input(self, accumulator, record):
        filename = record["FILENAME"]
        
        if filename not in accumulator:
            accumulator[filename] = record
        else:
            # Update existing record with new zone data
            if record["RAW_RECORDS"] > 0:
                accumulator[filename]["RAW_RECORDS"] = record["RAW_RECORDS"]
                accumulator[filename]["RAW_COLUMN"] = record["RAW_COLUMN"]
                accumulator[filename]["RAW_FAILED_RECORDS"] = record["RAW_FAILED_RECORDS"]
                accumulator[filename]["SOURCE_COUNT"] = record["SOURCE_COUNT"]
            
            if record["CERT_RECORDS"] > 0:
                accumulator[filename]["CERT_RECORDS"] = record["CERT_RECORDS"]
                accumulator[filename]["CERT_COLUMN"] = record["CERT_COLUMN"]
                accumulator[filename]["CERT_FAILED_RECORDS"] = record["CERT_FAILED_RECORDS"]
            
            if record["ANALYTIC_RECORDS"] > 0:
                accumulator[filename]["ANALYTIC_RECORDS"] = record["ANALYTIC_RECORDS"]
                accumulator[filename]["ANALYTIC_COLUMN"] = record["ANALYTIC_COLUMN"]
                accumulator[filename]["ANALYTIC_FAILED_RECORDS"] = record["ANALYTIC_FAILED_RECORDS"]
                accumulator[filename]["ANALYTIC_col_sums"] = record["ANALYTIC_col_sums"]
                accumulator[filename]["BQ_STATUS"] = record["BQ_STATUS"]
                accumulator[filename]["BQ_FAILED"] = record["BQ_FAILED"]
                accumulator[filename]["REASON"] = record["REASON"]
        
        return accumulator
    
    def merge_accumulators(self, accumulators):
        result = {}
        for acc in accumulators:
            for filename, record in acc.items():
                if filename not in result:
                    result[filename] = record
                else:
                    # Update existing record with new zone data
                    if record["RAW_RECORDS"] > 0:
                        result[filename]["RAW_RECORDS"] = record["RAW_RECORDS"]
                        result[filename]["RAW_COLUMN"] = record["RAW_COLUMN"]
                        result[filename]["RAW_FAILED_RECORDS"] = record["RAW_FAILED_RECORDS"]
                        result[filename]["SOURCE_COUNT"] = record["SOURCE_COUNT"]
                    
                    if record["CERT_RECORDS"] > 0:
                        result[filename]["CERT_RECORDS"] = record["CERT_RECORDS"]
                        result[filename]["CERT_COLUMN"] = record["CERT_COLUMN"]
                        result[filename]["CERT_FAILED_RECORDS"] = record["CERT_FAILED_RECORDS"]
                    
                    if record["ANALYTIC_RECORDS"] > 0:
                        result[filename]["ANALYTIC_RECORDS"] = record["ANALYTIC_RECORDS"]
                        result[filename]["ANALYTIC_COLUMN"] = record["ANALYTIC_COLUMN"]
                        result[filename]["ANALYTIC_FAILED_RECORDS"] = record["ANALYTIC_FAILED_RECORDS"]
                        result[filename]["ANALYTIC_col_sums"] = record["ANALYTIC_col_sums"]
                        result[filename]["BQ_STATUS"] = record["BQ_STATUS"]
                        result[filename]["BQ_FAILED"] = record["BQ_FAILED"]
                        result[filename]["REASON"] = record["REASON"]
        
        return result
    
    def extract_output(self, accumulator):
        # Calculate failed records
        for filename, record in accumulator.items():
            if record["CERT_RECORDS"] > 0 and record["RAW_RECORDS"] > 0:
                record["CERT_FAILED_RECORDS"] = record["RAW_RECORDS"] - record["CERT_RECORDS"]
            
            if record["ANALYTIC_RECORDS"] > 0 and record["CERT_RECORDS"] > 0:
                record["ANALYTIC_FAILED_RECORDS"] = record["CERT_RECORDS"] - record["ANALYTIC_RECORDS"]
        
        return list(accumulator.values())

# Run Apache Beam pipeline
def run_pipeline(project_id, raw_bucket, cert_bucket, analytic_bucket, folder_path, dataset, bq_dataset_id, table_id):
    options = PipelineOptions()
    
    # Get dataset info
    dataset_info = dataset_mapping.get(dataset, {})
    prefix = dataset_info.get("prefix", "")
    bq_table_map = dataset_info.get("tables", {})
    
    # Define bucket mappings
    buckets_info = {
        "RAW": raw_bucket,
        "CERT": cert_bucket,
        "ANALYTIC": analytic_bucket
    }
    
    with beam.Pipeline(options=options) as p:
        # Create a PCollection for each zone
        all_records = []
        
        for zone, bucket_name in buckets_info.items():
            # Create a pattern to match files in the bucket
            pattern = f"gs://{bucket_name}/{folder_path}*.csv"
            
            # Read files and process them
            records = (
                p 
                | f"ReadFiles_{zone}" >> beam.io.fileio.MatchFiles(pattern)
                | f"GetFileInfo_{zone}" >> beam.ParDo(GetFileInfoFn(dataset, prefix, bq_table_map))
                | f"ProcessFiles_{zone}" >> beam.ParDo(ProcessFileFn(zone, project_id, bq_dataset_id, dataset))
            )
            
            all_records.append(records)
        
        # Combine all records
        combined_records = (
            all_records
            | "FlattenRecords" >> beam.Flatten()
            | "CombineRecords" >> beam.CombineGlobally(CombineRecordsFn())
        )
        
        # Write to BigQuery
        combined_records | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            table=f"{project_id}:{bq_dataset_id}.{table_id}",
            schema=recon_consilidated_schema,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND
        )

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
    
    # Get the latest folder date
    month_folder = check_received_folder(project_id, raw_bucket, cap_hpi_path, dataset, folder_base)
    folder_path = f"{cap_hpi_path}/{folder_base}/"
    
    # Run the optimized pipeline
    run_pipeline(project_id, raw_bucket, cert_bucket, analytic_bucket, folder_path, dataset, bq_dataset_id, table_id)
