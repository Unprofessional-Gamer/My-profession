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
from apache_beam.io import ReadFromText

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

class ProcessFileDoFn(beam.DoFn):
    def __init__(self, buckets_info, dataset, project_id, bq_dataset_id):
        self.buckets_info = buckets_info
        self.dataset = dataset
        self.project_id = project_id
        self.bq_dataset_id = bq_dataset_id
        self.dataset_info = dataset_mapping.get(dataset, {})
        self.prefix = self.dataset_info.get("prefix", "")
        self.bq_table_map = self.dataset_info.get("tables", {})

    def process(self, file_path):
        filename = file_path.split("/")[-1]
        if not filename.startswith(self.prefix) or filename.endswith(("schema.csv", "metadata.csv")):
            return

        # Determine table name key based on dataset
        if self.dataset in ["CAP_NVD_CAR", "CAP_NVD_LCV"]:
            parts = filename.replace(self.prefix, "").replace(".csv", "").split("_")
            table_name_key = parts[-1]
        else:
            table_name_key = filename.replace(self.prefix, "").replace(".csv", "")
        
        bq_table_name = self.bq_table_map.get(table_name_key, "")
        if not bq_table_name:
            return

        # Get bucket name based on file path
        bucket_name = None
        zone = None
        for z, b in self.buckets_info.items():
            if f"/{z}/" in file_path:
                bucket_name = b
                zone = z
                break
        
        if not bucket_name:
            return

        # Get BQ schema
        bq_schema = get_bq_schema(self.project_id, self.bq_dataset_id, bq_table_name)
        column_names = get_bq_column_names(bq_schema)

        # Process file
        skip_header = (zone == "ANALYTIC")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        content = blob.download_as_text()

        df = pd.read_csv(StringIO(content), header=None, names=column_names, low_memory=False)
        column_count = len(df.columns)
        record_count = len(df) - (1 if skip_header else 0)

        # Compute column sums if it's ANAL zone
        column_sums = []
        if zone == "ANALYTIC":
            numeric_columns = df.select_dtypes(include=["int64", "float64"]).columns
            column_sums = [{"column_name": col, "sum_value": str(df[col].sum())} for col in numeric_columns if col in bq_schema]

        # Get source count (metadata count for RAW zone in CAP_NVD datasets)
        source_count = 0
        if zone == "RAW" and self.dataset in ["CAP_NVD_CAR", "CAP_NVD_LCV"]:
            source_count = metadata_count(bucket_name, file_path)

        # Get BQ count for ANALYTIC zone
        bq_status = ""
        bq_failed_count = 0
        reason = ""
        if zone == 'ANALYTIC':
            QUERY = f"SELECT count(*) as count FROM `{self.project_id}.{self.bq_dataset_id}.{bq_table_name}`"
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

        result = {
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
            result.update({
                "RAW_RECORDS": record_count,
                "RAW_COLUMN": column_count,
                "RAW_FAILED_RECORDS": source_count - record_count if source_count > 0 else 0
            })
        elif zone == "CERT":
            result.update({
                "CERT_RECORDS": record_count,
                "CERT_COLUMN": column_count,
                "CERT_FAILED_RECORDS": 0  # Will be updated in a later pass
            })
        elif zone == "ANALYTIC":
            result.update({
                "ANALYTIC_RECORDS": record_count,
                "ANALYTIC_COLUMN": column_count,
                "ANALYTIC_FAILED_RECORDS": 0,  # Will be updated in a later pass
                "ANALYTIC_col_sums": column_sums,
                "BQ_STATUS": bq_status,
                "BQ_FAILED": bq_failed_count,
                "REASON": reason
            })

        yield result

def run_pipeline(project_id, buckets_info, folder_paths, dataset, bq_dataset_id, table_id):
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        # Read files from all buckets and process them
        all_results = []
        for zone, bucket_name in buckets_info.items():
            files = (p 
                    | f'ListFiles_{zone}' >> beam.Create([f"{folder_paths[zone]}"])
                    | f'ExpandFiles_{zone}' >> beam.FlatMap(lambda path: [f"gs://{buckets_info[zone]}/{f}" for f in storage.Client().list_blobs(bucket_name, prefix=path) if f.endswith('.csv')])
                    | f'ProcessFiles_{zone}' >> beam.ParDo(ProcessFileDoFn(buckets_info, dataset, project_id, bq_dataset_id))
                   )
            all_results.append(files)
        
        # Combine all results
        combined_results = (all_results 
                           | beam.Flatten()
                           | beam.CombinePerKey(lambda x: x)  # Combine by filename
                           | beam.Map(lambda x: self.merge_records(x[1]))  # Merge records for same file
                          )
        
        # Write to BigQuery
        combined_results | 'WriteToBigQuery' >> WriteToBigQuery(
            table=f"{project_id}:{bq_dataset_id}.{table_id}",
            schema=recon_consilidated_schema,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND
        )

def merge_records(records):
    """Merge records from different zones for the same file"""
    merged = {}
    for record in records:
        filename = record['FILENAME']
        if filename not in merged:
            merged[filename] = record
        else:
            # Update with values from current record
            for key, value in record.items():
                if value:  # Only update if value is not empty/zero
                    merged[filename][key] = value
    
    # Calculate failed records between zones
    for filename, record in merged.items():
        if record['RAW_RECORDS'] and record['CERT_RECORDS']:
            record['CERT_FAILED_RECORDS'] = record['RAW_RECORDS'] - record['CERT_RECORDS']
        if record['CERT_RECORDS'] and record['ANALYTIC_RECORDS']:
            record['ANALYTIC_FAILED_RECORDS'] = record['CERT_RECORDS'] - record['ANALYTIC_RECORDS']
    
    return list(merged.values())

if __name__ == "__main__":
    load_env_config()
    dataset = sys.argv[1] if len(sys.argv) > 1 else "PRE_EMBARGO_LR"
    folder_base = "DAILY" if dataset in ["PRE_EMBARGO_LR","PRE_EMBARGO_J"] else "MONTHLY"

    project_id = os.getenv('PROJECT')
    raw_bucket = os.getenv('RAW_ZONE')
    cert_bucket = os.getenv('CERT_ZONE')
    analytic_bucket = os.getenv('ANALYTIC_ZONE')
    bq_dataset_id = os.getenv('BQ_DATASET')
    cap_hpi_path = os.getenv("CAP_PATH")
    table_id = os.getenv("RECON_TABLE")
    
    month_folder = check_received_folder(project_id, raw_bucket, cap_hpi_path, dataset, folder_base)
    if not month_folder:
        raise ValueError("No valid month folder found")
    
    buckets_info = {
        "RAW": raw_bucket,
        "CERT": cert_bucket,
        "ANALYTIC": analytic_bucket
    }
    
    folder_paths = {
        "RAW": f"{cap_hpi_path}/{dataset}/{folder_base}/Files/{month_folder}/",
        "CERT": f"{cap_hpi_path}/{dataset}/{folder_base}/Files/{month_folder}/",
        "ANALYTIC": f"{cap_hpi_path}/{dataset}/{folder_base}/Files/{month_folder}/"
    }
    
    run_pipeline(project_id, buckets_info, folder_paths, dataset, bq_dataset_id, table_id)
