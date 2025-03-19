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

# Updated dataset mapping for proper file-to-table resolution
dataset_mapping = {
    "PRE_EMBARGO_LR": {
        "files": ["CapDer.csv", "NDVGenericStatus.csv", "NVDModelYear.csv", "Capvehicles.csv"],
        "CapDer.csv": {"bq_table": "PRE_EMBARGO_LR_CAPDER"},
        "NDVGenericStatus.csv": {"bq_table": "PRE_EMBARGO_LR_NDVGENERICSTATUS"},
        "NVDModelYear.csv": {"bq_table": "PRE_EMBARGO_LR_NVDMODELYEAR"},
        "Capvehicles.csv": {"bq_table": "PRE_EMBARGO_LR_CAPVEHICLES"},
    },
    "PRE_EMBARGO_J": {
        "files": ["CapDer.csv", "NDVGenericStatus.csv", "NVDModelYear.csv", "Capvehicles.csv"],
        "CapDer.csv": {"bq_table": "PRE_EMBARGO_J_CAPDER"},
        "NDVGenericStatus.csv": {"bq_table": "PRE_EMBARGO_J_NDVGENERICSTATUS"},
        "NVDModelYear.csv": {"bq_table": "PRE_EMBARGO_J_NVDMODELYEAR"},
        "Capvehicles.csv": {"bq_table": "PRE_EMBARGO_J_CAPVEHICLES"},
    },
}

# Function to get BQ schema
def get_bq_schema(project_id, dataset_id, table_id):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)
    schema = table.schema
    return {field.name: field.field_type for field in schema}

# Function to check data consistency with BigQuery
def analytical_to_bq_checking(ana_bucket_name, dataset, project_id, records, bq_table_name):
    client = storage.Client(project=project_id)
    fs = GcsIO(client)
    ana_bucket = client.bucket(ana_bucket_name)
    blob_list_ana = client.list_blobs(ana_bucket)

    for blob_ana in blob_list_ana:
        filename = blob_ana.name.split("/")[-1]
        if dataset not in filename:
            continue

        with fs.open(f"gs://{ana_bucket_name}/{blob_ana.name}", mode="r") as pf:
            ana_lines = pf.readlines()
        ana_count = len(ana_lines) - 1  # Exclude header

        QUERY = f"SELECT count(*) as count FROM `{project_id}.{bq_table_name}`"
        bq_client = bigquery.Client(project_id)
        query_job = bq_client.query(QUERY)
        bq_count = next(query_job.result()).count

        bq_status = "Match" if bq_count == ana_count else "Not Match"
        bq_failed_count = abs(ana_count - bq_count)
        reason = f"{bq_status}: ana_records({ana_count}) vs. BQ_count({bq_count})"

        if filename in records:
            records[filename].update(
                {"BQ_STATUS": bq_status, "BQ_FAILED": bq_failed_count, "REASON": reason}
            )

    return records

# Main function to process files
def process_files(_, buckets_info, folder_path, dataset, project_id, bq_dataset_id):
    records = {}
    dataset_info = dataset_mapping.get(dataset, {})

    for zone, bucket_name in buckets_info.items():
        files = list_files_in_folder(bucket_name, folder_path)

        for file in files:
            filename = file.split("/")[-1]
            if filename not in dataset_info.get("files", []):
                continue  # Skip files not in the dataset mapping

            bq_table_name = dataset_info.get(filename, {}).get("bq_table")
            if not bq_table_name:
                continue  # Skip if no BQ table is mapped

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
                    "REASON": "",
                }

            if zone == "RAW":
                records[filename].update(
                    {"RAW_RECORDS": record_count, "RAW_COLUMN": column_count, "RAW_FAILED_RECORDS": source_count - record_count}
                )
            elif zone == "CERT":
                records[filename].update(
                    {"CERT_RECORDS": record_count, "CERT_COLUMN": column_count, "CERT_FAILED_RECORDS": records[filename]["RAW_RECORDS"] - record_count}
                )
            elif zone == "ANALYTIC":
                records[filename].update(
                    {"ANALYTIC_RECORDS": record_count, "ANALYTIC_COLUMN": column_count, "ANALYTIC_FAILED_RECORDS": records[filename]["CERT_RECORDS"] - record_count, "ANALYTIC_col_sums": column_sums}
                )

    # Ensure the BQ checking function is called with a valid table
    if bq_table_name:
        records = analytical_to_bq_checking(buckets_info["ANALYTIC"], dataset, project_id, records, bq_table_name)

    return list(records.values())

