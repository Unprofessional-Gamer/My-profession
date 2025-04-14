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
        {"name": "ANALYTIC_COL_SUMS", "type": "RECORD", "mode": "REPEATED", "fields": [
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
    return {
        'PROJECT': os.getenv('PROJECT'),
        'RAW_ZONE': os.getenv('RAW_ZONE'),
        'CERT_ZONE': os.getenv('CERT_ZONE'),
        'ANALYTIC_ZONE': os.getenv('ANALYTIC_ZONE'),
        'BQ_DATASET': os.getenv('BQ_DATASET'),
        'CAP_PATH': os.getenv('CAP_PATH'),
        'RECON_TABLE': os.getenv('RECON_TABLE')
    }

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

# Beam DoFn for listing files in folder
class ListFilesInFolderFn(beam.DoFn):
    def __init__(self, bucket_name, folder_path):
        self.bucket_name = bucket_name
        self.folder_path = folder_path
        
    def process(self, element):
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=self.folder_path)
        for blob in blobs:
            if blob.name.endswith(".csv"):
                yield blob.name

# Beam DoFn for getting record count and sums
class GetRecordCountAndSumsFn(beam.DoFn):
    def __init__(self, bucket_name, zone, skip_header, bq_schema):
        self.bucket_name = bucket_name
        self.zone = zone
        self.skip_header = skip_header
        self.bq_schema = bq_schema
        
    def process(self, file_path):
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(file_path)
        content = blob.download_as_text()

        column_names = get_bq_column_names(self.bq_schema)
        df = pd.read_csv(StringIO(content), header=None, names=column_names, low_memory=False)

        column_count = len(df.columns)
        record_count = len(df) - (1 if self.skip_header else 0)

        # Compute column sums if it's ANAL zone
        column_sums = []
        if self.zone == "ANAL":
            numeric_columns = df.select_dtypes(include=["int64", "float64"]).columns
            column_sums = [{"column_name": col, "sum_value": str(df[col].sum())} for col in numeric_columns if col in self.bq_schema]

        yield {
            'file_path': file_path,
            'record_count': record_count,
            'column_count': column_count,
            'column_sums': column_sums
        }

# Beam DoFn for processing files
class ProcessFilesFn(beam.DoFn):
    def __init__(self, dataset, project_id, bq_dataset_id, buckets_info):
        self.dataset = dataset
        self.project_id = project_id
        self.bq_dataset_id = bq_dataset_id
        self.buckets_info = buckets_info
        self.records = {}
        
    def process(self, element):
        dataset_info = dataset_mapping.get(self.dataset, {})
        prefix = dataset_info.get("prefix", "")
        bq_table_map = dataset_info.get("tables", {})
        
        file_path = element['file_path']
        zone = element['zone']
        bucket_name = self.buckets_info[zone]
        
        filename = file_path.split("/")[-1]
        if not filename.startswith(prefix) or filename.endswith(("schema.csv", "metadata.csv")):
            return
            
        if self.dataset == "CAP_NVD_CAR" and filename.startswith("CAR_") and filename.endswith(".csv"):
            parts = filename.replace(prefix, "").replace(".csv", "").split("_")
            table_name_key = parts[-1]
        elif self.dataset == "CAP_NVD_LCV" and filename.startswith("LIGHTS_") and filename.endswith(".csv"):
            parts = filename.replace(prefix, "").replace(".csv", "").split("_")
            table_name_key = parts[-1]
        else:
            table_name_key = filename.replace(prefix, "").replace(".csv", "")
            
        bq_table_name = bq_table_map.get(table_name_key, "")
        
        if not bq_table_name:
            return  # Skip this file if table name is not found
            
        bq_schema = get_bq_schema(self.project_id, self.bq_dataset_id, bq_table_name)
        skip_header = (zone == "ANALYTIC")
        
        record_count = element['record_count']
        column_count = element['column_count']
        column_sums = element['column_sums']
        
        source_count = metadata_count(bucket_name, file_path) if zone == "RAW" else 0
        
        bq_status = ""
        bq_failed_count = 0
        reason = ""
        
        if zone == 'ANALYTIC':
            QUERY = f"SELECT count(*) as count FROM `{self.project_id}.{bq_table_name}`"
            bq_client = bigquery.Client(self.project_id)
            query_job = bq_client.query(QUERY)
            bq_count = next(query_job.result()).count
            ana_count = record_count
            bq_status = "Match" if bq_count == ana_count else "Not Match"
            bq_failed_count = abs(ana_count - bq_count)
            reason = f"{bq_status}: ana_records({ana_count}) vs. BQ_count({bq_count})"
            
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
                "ANALYTIC_COL_SUMS": [],
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
            self.records[filename].update({
                "ANALYTIC_RECORDS": record_count, 
                "ANALYTIC_COLUMN": column_count, 
                "ANALYTIC_FAILED_RECORDS": self.records[filename]["CERT_RECORDS"] - record_count, 
                "ANALYTIC_COL_SUMS": column_sums, 
                "BQ_STATUS": bq_status, 
                "BQ_FAILED": bq_failed_count, 
                "REASON": reason
            })
            
        # Only yield when we have processed all zones for a file
        if all(zone in self.records[filename] for zone in ["RAW", "CERT", "ANALYTIC"]):
            yield self.records[filename]
            # Remove the record to free memory
            del self.records[filename]

# Run Apache Beam pipeline
def run_pipeline(dataset, folder_path, project_id, bq_dataset_id, table_id, buckets_info):
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        # Create a PCollection for each zone
        for zone, bucket_name in buckets_info.items():
            # List files in folder
            files = (p | f"Create_{zone}" >> beam.Create([None])
                      | f"ListFiles_{zone}" >> beam.ParDo(ListFilesInFolderFn(bucket_name, folder_path)))
            
            # Get record count and sums
            file_stats = (files | f"GetStats_{zone}" >> beam.ParDo(
                GetRecordCountAndSumsFn(
                    bucket_name, 
                    zone, 
                    skip_header=(zone == "ANALYTIC"), 
                    bq_schema=get_bq_schema(project_id, bq_dataset_id, "dummy_table")  # Will be updated in ProcessFilesFn
                )
            ))
            
            # Add zone information
            file_stats_with_zone = (file_stats | f"AddZone_{zone}" >> beam.Map(
                lambda x, z=zone: {**x, 'zone': z}
            ))
            
            # Process files
            processed_records = (file_stats_with_zone | f"ProcessFiles_{zone}" >> beam.ParDo(
                ProcessFilesFn(dataset, project_id, bq_dataset_id, buckets_info)
            ))
            
            # Write to BigQuery
            (processed_records | f"WriteToBQ_{zone}" >> beam.io.WriteToBigQuery(
                table=f"{project_id}:{bq_dataset_id}.{table_id}",
                schema=recon_consilidated_schema,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            ))

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
    
    buckets_info = {
        "RAW": raw_bucket,
        "CERT": cert_bucket,
        "ANALYTIC": analytic_bucket
    }
    
    month_folder = check_received_folder(project_id, raw_bucket, cap_hpi_path, dataset, folder_base)
    folder_path = f"{cap_hpi_path}/{folder_base}/"

    run_pipeline(dataset, folder_path, project_id, bq_dataset_id, table_id, buckets_info)
