import logging
from datetime import datetime
from google.cloud import storage, bigquery
from io import StringIO
import pandas as pd
import apache_beam as beam
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

# Dataset mapping with updated keys
dataset_mapping = {
    "PRE_EMBARGO_LR": {
        "files": [
            "PreEmbargoed_Land_Rover_CAPVehicles.csv",
            "PreEmbargoed_Land_Rover_CapDer.csv",
            "PreEmbargoed_Land_Rover_NVDGenericStatus.csv",
            "PreEmbargoed_Land_Rover_NVDModelYear.csv",
            "PreEmbargoed_Land_Rover_NVDOptions.csv",
            "PreEmbargoed_Land_Rover_NVDPrices.csv",
            "PreEmbargoed_Land_Rover_NVDStandardEquipment.csv",
            "PreEmbargoed_Land_Rover_NVDTechnical.csv"
        ],
        "PreEmbargoed_Land_Rover_CAPVehicles.csv": {"bq_table": "PRE_EMBARGO_LR_CAPVEHICLES"},
        "PreEmbargoed_Land_Rover_CapDer.csv": {"bq_table": "PRE_EMBARGO_LR_CAPDER"},
        "PreEmbargoed_Land_Rover_NVDGenericStatus.csv": {"bq_table": "PRE_EMBARGO_LR_NDVGENERICSTATUS"},
        "PreEmbargoed_Land_Rover_NVDModelYear.csv": {"bq_table": "PRE_EMBARGO_LR_NVDMODELYEAR"},
        "PreEmbargoed_Land_Rover_NVDOptions.csv": {"bq_table": "PRE_EMBARGO_LR_NVDOPTIONS"},
        "PreEmbargoed_Land_Rover_NVDPrices.csv": {"bq_table": "PRE_EMBARGO_LR_NVDPRICES"},
        "PreEmbargoed_Land_Rover_NVDStandardEquipment.csv": {"bq_table": "PRE_EMBARGO_LR_NVDSTANDARDEQUIPMENT"},
        "PreEmbargoed_Land_Rover_NVDTechnical.csv": {"bq_table": "PRE_EMBARGO_LR_NVDTECHNICAL"}
    }
}

# Function to get schema from BigQuery
def get_bq_schema(project_id, dataset_id, table_id):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)
    schema = table.schema
    return {field.name: field.field_type for field in schema}

# Function to list CSV files in the GCS folder
def list_files_in_folder(bucket_name, folder_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    files = [blob.name for blob in blobs if blob.name.endswith(".csv")]
    logging.info(f"Found {len(files)} files in bucket {bucket_name}, folder {folder_path}: {files}")
    return files

# Main function to process files
def process_files(_, buckets_info, folder_path, dataset, project_id, bq_dataset_id):
    records = {}
    dataset_info = dataset_mapping.get(dataset, {})
    prefix = "PreEmbargoed_Land_Rover_"
    bq_table_map = dataset_info.get("tables", {})

    for zone, bucket_name in buckets_info.items():
        logging.info(f"Processing zone: {zone}, bucket: {bucket_name}")
        files = list_files_in_folder(bucket_name, folder_path)

        for file in files:
            filename = file.split("/")[-1]
            if not filename.startswith(prefix) or filename.endswith(("schema.csv", "metadata.csv")):
                logging.info(f"Skipping file: {filename} (does not match prefix or is metadata/schema)")
                continue

            table_name_key = filename.replace(prefix, "").replace(".csv", "")
            logging.info(f"Processing file: {filename}, table_name_key: {table_name_key}")

            bq_table_name = bq_table_map.get(table_name_key, "")
            if not bq_table_name:
                logging.warning(f"No table mapping found for file: {filename}")
                continue

            logging.info(f"Found table mapping: {bq_table_name} for file: {filename}")
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
                    "RAW_RECORDS": 0, "CERT_RECORDS": 0, "ANALYTIC_RECORDS": 0,
                    "RAW_FAILED_RECORDS": 0, "CERT_FAILED_RECORDS": 0,