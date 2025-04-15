from google.cloud import storage, bigquery
from io import StringIO
import os
import pandas as pd
import sys
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.io.textio import ReadFromText
from apache_beam.io.gcp.gcsio import GcsIO

# ------- Schema and Dataset Mapping --------

recon_consilidated_schema = {...}  # keep your existing schema as-is

dataset_mapping = {...}  # keep your existing mapping

def load_env_config():
    from dotenv import load_dotenv, find_dotenv
    load_dotenv()
    return os.environ

def get_bq_schema(project_id, dataset_id, table_id):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)
    schema = table.schema
    return {field.name: field.field_type for field in schema}

def get_bq_column_names(bq_schema, exclude_columns=[]):
    return [col for col in bq_schema if col not in exclude_columns]

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

def get_record_count_and_sums(bucket_name, file_path, zone, skip_header, bq_schema):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()
    column_names = get_bq_column_names(bq_schema)
    df = pd.read_csv(StringIO(content), header=None, names=column_names, low_memory=False)
    column_count = len(df.columns)
    record_count = len(df) - (1 if skip_header else 0)
    column_sums = []
    if zone == "ANAL":
        numeric_columns = df.select_dtypes(include=["int64", "float64"]).columns
        column_sums = [{"column_name": col, "sum_value": str(df[col].sum())} for col in numeric_columns if col in bq_schema]
    return record_count, column_count, column_sums

# -------- Apache Beam DoFn --------

class ProcessFilesFn(beam.DoFn):
    def __init__(self, project_id, dataset, folder_path, bq_dataset_id, buckets_info):
        self.project_id = project_id
        self.dataset = dataset
        self.folder_path = folder_path
        self.bq_dataset_id = bq_dataset_id
        self.buckets_info = buckets_info

    def process(self, file_metadata):
        from apache_beam.io.gcp.gcsio import parse_gcs_path

        file_path = file_metadata.path
        bucket_name, blob_path = parse_gcs_path(file_path)

        dataset_info = dataset_mapping.get(self.dataset, {})
        prefix = dataset_info.get("prefix", "")
        bq_table_map = dataset_info.get("tables", {})

        zone = None
        for z, bkt in self.buckets_info.items():
            if bkt == bucket_name:
                zone = z
                break

        filename = blob_path.split("/")[-1]
        if not filename.startswith(prefix) or filename.endswith(("schema.csv", "metadata.csv")):
            return

        if self.dataset in ["CAP_NVD_CAR", "CAP_NVD_LCV"]:
            parts = filename.replace(prefix, "").replace(".csv", "").split("_")
            table_name_key = parts[-1]
        else:
            table_name_key = filename.replace(prefix, "").replace(".csv", "")

        bq_table_name = bq_table_map.get(table_name_key, "")
        if not bq_table_name:
            return

        bq_schema = get_bq_schema(self.project_id, self.bq_dataset_id, bq_table_name)
        skip_header = (zone == "ANALYTIC")
        record_count, column_count, column_sums = get_record_count_and_sums(bucket_name, blob_path, zone, skip_header, bq_schema)

        if self.dataset in ["CAP_NVD_CAR", "CAP_NVD_LCV"] and zone == "RAW":
            source_count = metadata_count(bucket_name, blob_path)
        elif zone == "RAW":
            source_count = record_count
        else:
            source_count = 0

        if zone == 'ANALYTIC':
            QUERY = f"SELECT count(*) as count FROM `{self.project_id}.{self.bq_dataset_id}.{bq_table_name}`"
            bq_client = bigquery.Client(self.project_id)
            query_job = bq_client.query(QUERY)
            bq_count = next(query_job.result()).count
            ana_count = record_count
            bq_status = "Match" if bq_count == ana_count else "Not Match"
            bq_failed_count = abs(ana_count - bq_count)
            reason = f"{bq_status}: ana_records({ana_count}) vs. BQ_count({bq_count})"
        else:
            bq_status = ""
            bq_failed_count = 0
            reason = ""

        pick_date = blob_path.split("/")[-2]
        folder_date = f"{pick_date[:4]}-{pick_date[4:6]}-{pick_date[6:]}"
        processed_time = datetime.now().strftime("%d/%m/%Y T %H:%M:%S")

        output = {
            "DATASET": self.dataset,
            "FILE_DATE": folder_date,
            "PROCESSED_DATE_TIME": processed_time,
            "FILENAME": filename,
            "SOURCE_COUNT": source_count,
            "RAW_RECORDS": record_count if zone == "RAW" else 0,
            "CERT_RECORDS": record_count if zone == "CERT" else 0,
            "ANALYTIC_RECORDS": record_count if zone == "ANALYTIC" else 0,
            "RAW_FAILED_RECORDS": source_count - record_count if zone == "RAW" else 0,
            "CERT_FAILED_RECORDS": 0,
            "ANALYTIC_FAILED_RECORDS": 0,
            "RAW_COLUMN": column_count if zone == "RAW" else 0,
            "CERT_COLUMN": column_count if zone == "CERT" else 0,
            "ANALYTIC_COLUMN": column_count if zone == "ANALYTIC" else 0,
            "ANALYTIC_col_sums": column_sums if zone == "ANALYTIC" else [],
            "BQ_STATUS": bq_status,
            "BQ_FAILED": bq_failed_count,
            "REASON": reason
        }

        yield output

# --------- Beam Pipeline Runner ---------

def run_pipeline(project_id, dataset, folder_path, bq_dataset_id, table_id, buckets_info):
    options = PipelineOptions()

    with beam.Pipeline(options=options) as pipeline:
        for zone, bucket_name in buckets_info.items():
            full_path = f"gs://{bucket_name}/{folder_path}**/*.csv"
            (
                pipeline
                | f"MatchFiles-{zone}" >> MatchFiles(full_path)
                | f"ProcessFiles-{zone}" >> beam.ParDo(
                    ProcessFilesFn(project_id, dataset, folder_path, bq_dataset_id, buckets_info))
                | f"WriteToBQ-{zone}" >> WriteToBigQuery(
                    table=f"{project_id}:{bq_dataset_id}.{table_id}",
                    schema=recon_consilidated_schema,
                    create_disposition=BigQueryDisposition.CREATE_NEVER,
                    write_disposition=BigQueryDisposition.WRITE_APPEND)
            )

# ------------ Main Driver -------------

if __name__ == "__main__":
    env_config = load_env_config()
    dataset = sys.argv[1] if len(sys.argv) > 1 else "PRE_EMBARGO"
    folder_base = "DAILY" if dataset in ["PRE_EMBARGO","CAP_NVD_CAR","CAP_NVD_LCV"] else "MONTHLY"
    project_id = env_config.get('PROJECT')
    raw_bucket = env_config.get('RAW_ZONE')
    cert_bucket = env_config.get('CERT_ZONE')
    analytic_bucket = env_config.get('ANALYTIC_ZONE')
    bq_dataset_id = env_config.get('BQ_DATASET')
    cap_hpi_path = env_config.get("CAP_PATH")
    table_id = env_config.get("RECON_TABLE")
    folder_path = f"{cap_hpi_path}/{folder_base}/"

    buckets_info = {"RAW": raw_bucket, "CERT": cert_bucket, "ANALYTIC": analytic_bucket}

    run_pipeline(project_id, dataset, folder_path, bq_dataset_id, table_id, buckets_info)
