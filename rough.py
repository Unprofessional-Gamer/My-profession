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
    load_dotenv(find_dotenv())
    return {
        'PROJECT': os.getenv('PROJECT'),
        'RAW_ZONE': os.getenv('RAW_ZONE'),
        'CERT_ZONE': os.getenv('CERT_ZONE'),
        'ANALYTIC_ZONE': os.getenv('ANALYTIC_ZONE'),
        'BQ_DATASET': os.getenv('BQ_DATASET'),
        'CAP_PATH': os.getenv('CAP_PATH'),
        'RECON_TABLE': os.getenv('RECON_TABLE')
    }

def get_bq_schema(project_id, dataset_id, table_id):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)
    schema = table.schema
    return {field.name: field.field_type for field in schema}

def get_bq_column_names(bq_schema, exclude_columns=[]):
    return [col for col in bq_schema if col not in exclude_columns]

class MetadataCountFn(beam.DoFn):
    def process(self, element, bucket_name):
        file_path = element
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        metadatafile = file_path[:-4] + ".metadata.csv"
        blob = bucket.blob(metadatafile)
        
        if not blob.exists():
            yield (file_path, 0)
            return

        content = blob.download_as_text()
        df = pd.read_csv(StringIO(content))
        count = int(df[df['Key'] == 'Total Records']['Value'].values[0])
        yield (file_path, count)

class ProcessFileFn(beam.DoFn):
    def __init__(self, project_id, bq_dataset_id, dataset_info):
        self.project_id = project_id
        self.bq_dataset_id = bq_dataset_id
        self.dataset_info = dataset_info

    def process(self, element, zone, bucket_name):
        file_path, source_count = element
        filename = file_path.split("/")[-1]
        prefix = self.dataset_info.get("prefix", "")
        bq_table_map = self.dataset_info.get("tables", {})
        
        if not filename.startswith(prefix) or filename.endswith(("schema.csv", "metadata.csv")):
            return

        if self.dataset_info.get("name") in ["CAP_NVD_CAR", "CAP_NVD_LCV"] and filename.endswith(".csv"):
            parts = filename.replace(prefix, "").replace(".csv", "").split("_")
            table_name_key = parts[-1]
        else:
            table_name_key = filename.replace(prefix, "").replace(".csv", "")
            
        bq_table_name = bq_table_map.get(table_name_key, "")
        if not bq_table_name:
            return

        bq_schema = get_bq_schema(self.project_id, self.bq_dataset_id, bq_table_name)
        column_names = get_bq_column_names(bq_schema)
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        content = blob.download_as_text()
        
        skip_header = (zone == "ANALYTIC")
        df = pd.read_csv(StringIO(content), header=None, names=column_names, low_memory=False)
        
        column_count = len(df.columns)
        record_count = len(df) - (1 if skip_header else 0)
        
        column_sums = []
        if zone == "ANALYTIC":
            numeric_columns = df.select_dtypes(include=["int64", "float64"]).columns
            column_sums = [{"column_name": col, "sum_value": str(df[col].sum())} 
                          for col in numeric_columns if col in bq_schema]

        pick_date = file_path.split("/")[-2]
        folder_date = f"{pick_date[:4]}-{pick_date[4:6]}-{pick_date[6:]}"
        processed_time = datetime.now().strftime("%d/%m/%Y T %H:%M:%S")

        result = {
            "DATASET": self.dataset_info.get("name"),
            "FILE_DATE": folder_date,
            "PROCESSED_DATE_TIME": processed_time,
            "FILENAME": filename,
            "SOURCE_COUNT": source_count if zone == "RAW" else 0,
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
            "REASON": ""
        }

        if zone == "RAW":
            result.update({
                "RAW_RECORDS": record_count,
                "RAW_COLUMN": column_count,
                "RAW_FAILED_RECORDS": source_count - record_count if source_count else 0
            })
        elif zone == "CERT":
            result.update({
                "CERT_RECORDS": record_count,
                "CERT_COLUMN": column_count,
                "CERT_FAILED_RECORDS": 0  # Will be updated in the combine step
            })
        elif zone == "ANALYTIC":
            bq_client = bigquery.Client(self.project_id)
            QUERY = f"SELECT count(*) as count FROM `{self.project_id}.{self.bq_dataset_id}.{bq_table_name}`"
            query_job = bq_client.query(QUERY)
            bq_count = next(query_job.result()).count
            ana_count = record_count
            bq_status = "Match" if bq_count == ana_count else "Not Match"
            bq_failed_count = abs(ana_count - bq_count)
            reason = f"{bq_status}: ana_records({ana_count}) vs. BQ_count({bq_count})"
            
            result.update({
                "ANALYTIC_RECORDS": record_count,
                "ANALYTIC_COLUMN": column_count,
                "ANALYTIC_FAILED_RECORDS": 0,  # Will be updated in the combine step
                "ANALYTIC_col_sums": column_sums,
                "BQ_STATUS": bq_status,
                "BQ_FAILED": bq_failed_count,
                "REASON": reason
            })

        yield (filename, (zone, result))

class CombineRecordsFn(beam.CombineFn):
    def create_accumulator(self):
        return {
            "DATASET": "",
            "FILE_DATE": "",
            "PROCESSED_DATE_TIME": "",
            "FILENAME": "",
            "SOURCE_COUNT": 0,
            "RAW_RECORDS": 0,
            "RAW_FAILED_RECORDS": 0,
            "CERT_RECORDS": 0,
            "CERT_FAILED_RECORDS": 0,
            "ANALYTIC_RECORDS": 0,
            "ANALYTIC_FAILED_RECORDS": 0,
            "RAW_COLUMN": 0,
            "CERT_COLUMN": 0,
            "ANALYTIC_COLUMN": 0,
            "ANALYTIC_col_sums": [],
            "BQ_STATUS": "",
            "BQ_FAILED": 0,
            "REASON": ""
        }

    def add_input(self, accumulator, element):
        filename, (zone, record) = element
        
        if not accumulator["FILENAME"]:
            accumulator.update({
                "DATASET": record["DATASET"],
                "FILE_DATE": record["FILE_DATE"],
                "PROCESSED_DATE_TIME": record["PROCESSED_DATE_TIME"],
                "FILENAME": filename,
                "SOURCE_COUNT": record["SOURCE_COUNT"]
            })
        
        if zone == "RAW":
            accumulator.update({
                "RAW_RECORDS": record["RAW_RECORDS"],
                "RAW_COLUMN": record["RAW_COLUMN"],
                "RAW_FAILED_RECORDS": record["RAW_FAILED_RECORDS"]
            })
        elif zone == "CERT":
            accumulator.update({
                "CERT_RECORDS": record["CERT_RECORDS"],
                "CERT_COLUMN": record["CERT_COLUMN"],
                "CERT_FAILED_RECORDS": accumulator["RAW_RECORDS"] - record["CERT_RECORDS"]
            })
        elif zone == "ANALYTIC":
            accumulator.update({
                "ANALYTIC_RECORDS": record["ANALYTIC_RECORDS"],
                "ANALYTIC_COLUMN": record["ANALYTIC_COLUMN"],
                "ANALYTIC_FAILED_RECORDS": accumulator["CERT_RECORDS"] - record["ANALYTIC_RECORDS"],
                "ANALYTIC_col_sums": record["ANALYTIC_col_sums"],
                "BQ_STATUS": record["BQ_STATUS"],
                "BQ_FAILED": record["BQ_FAILED"],
                "REASON": record["REASON"]
            })
        
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            for key in acc:
                if isinstance(acc[key], list):
                    if key not in merged or not merged[key]:
                        merged[key] = acc[key]
                elif isinstance(acc[key], (int, float)):
                    merged[key] += acc[key]
                else:
                    if not merged[key]:
                        merged[key] = acc[key]
        return merged

    def extract_output(self, accumulator):
        return accumulator

def list_files(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs if blob.name.endswith(".csv") and not blob.name.endswith(("metadata.csv", "schema.csv"))]

def run_pipeline(argv=None):
    env_config = load_env_config()
    dataset_name = sys.argv[1] if len(sys.argv) > 1 else "PRE_EMBARGO"
    folder_base = "DAILY" if dataset_name in ["PRE_EMBARGO"] else "MONTHLY"
    
    project_id = env_config['PROJECT']
    raw_bucket = env_config['RAW_ZONE']
    cert_bucket = env_config['CERT_ZONE']
    analytic_bucket = env_config['ANALYTIC_ZONE']
    bq_dataset_id = env_config['BQ_DATASET']
    cap_hpi_path = env_config['CAP_PATH']
    table_id = env_config['RECON_TABLE']
    
    dataset_info = {
        "name": dataset_name,
        **dataset_mapping.get(dataset_name, {})
    }
    
    folder_path = f"{cap_hpi_path}/{folder_base}/"
    
    pipeline_options = PipelineOptions()
    
    with beam.Pipeline(options=pipeline_options) as p:
        # List files in RAW zone and get metadata counts for CAP_NVD_CAR and CAP_NVD_LCV
        raw_files = (
            p 
            | "List RAW Files" >> beam.Create(list_files(raw_bucket, folder_path))
            | "Filter RAW Files" >> beam.Filter(
                lambda x: x.split("/")[-1].startswith(dataset_info.get("prefix", ""))
        )
        
        if dataset_name in ["CAP_NVD_CAR", "CAP_NVD_LCV"]:
            raw_with_counts = (
                raw_files
                | "Get Metadata Counts" >> beam.ParDo(MetadataCountFn(), raw_bucket)
            )
        else:
            raw_with_counts = (
                raw_files
                | "Add Zero Count" >> beam.Map(lambda x: (x, 0))
            )
        
        # Process RAW files
        raw_records = (
            raw_with_counts
            | "Process RAW Files" >> beam.ParDo(
                ProcessFileFn(project_id, bq_dataset_id, dataset_info), "RAW", raw_bucket)
        )
        
        # Process CERT files
        cert_records = (
            p 
            | "List CERT Files" >> beam.Create(list_files(cert_bucket, folder_path))
            | "Filter CERT Files" >> beam.Filter(
                lambda x: x.split("/")[-1].startswith(dataset_info.get("prefix", "")))
            | "Add Zero Count CERT" >> beam.Map(lambda x: (x, 0))
            | "Process CERT Files" >> beam.ParDo(
                ProcessFileFn(project_id, bq_dataset_id, dataset_info), "CERT", cert_bucket)
        )
        
        # Process ANALYTIC files
        analytic_records = (
            p 
            | "List ANALYTIC Files" >> beam.Create(list_files(analytic_bucket, folder_path))
            | "Filter ANALYTIC Files" >> beam.Filter(
                lambda x: x.split("/")[-1].startswith(dataset_info.get("prefix", "")))
            | "Add Zero Count ANALYTIC" >> beam.Map(lambda x: (x, 0))
            | "Process ANALYTIC Files" >> beam.ParDo(
                ProcessFileFn(project_id, bq_dataset_id, dataset_info), "ANALYTIC", analytic_bucket)
        )
        
        # Combine all records
        all_records = (
            (raw_records, cert_records, analytic_records) 
            | "Flatten Records" >> beam.Flatten()
            | "Combine by Filename" >> beam.CombinePerKey(CombineRecordsFn())
            | "Get Values" >> beam.Map(lambda x: x[1])
        )
        
        # Write to BigQuery
        _ = (
            all_records
            | "Write to BigQuery" >> WriteToBigQuery(
                table=f"{project_id}:{bq_dataset_id}.{table_id}",
                schema=recon_consilidated_schema,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
