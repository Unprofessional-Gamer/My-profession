from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import sys
from io import StringIO
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Function to list CSV files in the GCS dataset folder
def list_files_in_folder(bucket_name, folder_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    return [blob.name for blob in blobs if blob.name.endswith('.csv')]

# Function to read CSV files from GCS
def read_csv_file_from_gcs(bucket_name, file_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()
    return pd.read_csv(StringIO(content), header=None)

# Function to process each file and count errors
def process_file(bucket_name, file, dataset, zone):
    try:
        df = read_csv_file_from_gcs(bucket_name, file)
        if df.empty:
            return None

        record_count = len(df)
        pick_date = file.split('/')[-1]
        folder_date = f"{pick_date[:4]}-{pick_date[4:6]}-{pick_date[6:8]}"
        processed_date = datetime.now().strftime("%Y-%m-%d")
        processed_time = datetime.now().strftime("%H:%M-%S")
        filename = file.split('/')[-1]

        # Initialize error counts
        error_counts = {
            'Failed_null_check': 0,
            'Failed_CapCode_value': 0,
            'Failed_CapId_value': 0,
            'Failed_year_column_value': 0,
            'Failed_plate_column_value': 0,
            'Failed_Volume_check': 0,
            'Failed_unique_check': 0,
        }

        # Count errors in the dataset
        for _, row in df.iterrows():
            for cell in row:
                if isinstance(cell, str):
                    for key in error_counts.keys():
                        if key.replace('_', ' ') in cell:
                            error_counts[key] += 1

        return {
            'ZONE': zone,
            'DATASET': dataset,
            'FILE_DATE': folder_date,
            'PROCESSED_DATE': processed_date,
            'PROCESSED_TIME': processed_time,
            'FILENAME': filename,
            'RECORDS': record_count,
            **error_counts
        }
    except pd.errors.EmptyDataError:
        return None

# Beam pipeline to process all files in a dataset
def run_pipeline(project_id, raw_zone_bucket, certify_zone_bucket, folder_path, bq_dataset_id, bq_table_name, dataset):
    options = PipelineOptions(
        project=project_id,
        job_name=f'reconciliation-{dataset}'.lower(),
        runner="DataflowRunner",
        region='your-region',
        staging_location=f'gs://{raw_zone_bucket}/staging',
        temp_location=f'gs://{raw_zone_bucket}/temp',
        num_workers=1,
        max_num_workers=4,
        use_public_ips=False,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        # Read all files from raw and certify zones
        raw_files = list_files_in_folder(raw_zone_bucket, folder_path)
        certify_files = list_files_in_folder(certify_zone_bucket, folder_path)

        all_files = [(raw_zone_bucket, file, dataset, 'RAW') for file in raw_files] + \
                    [(certify_zone_bucket, file, dataset, 'CERTIFY') for file in certify_files]

        consolidated_data = (
            p
            | "Create file list" >> beam.Create(all_files)
            | "Process files" >> beam.Map(lambda args: process_file(*args))
            | "Filter non-empty results" >> beam.Filter(lambda record: record is not None)
        )

        # Write to BigQuery
        consolidated_data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=f'{project_id}:{bq_dataset_id}.{bq_table_name}',
            schema='ZONE:STRING, DATASET:STRING, FILE_DATE:DATE, PROCESSED_DATE:DATE, PROCESSED_TIME:STRING, '
                   'FILENAME:STRING, RECORDS:INTEGER, Failed_null_check:INTEGER, Failed_CapCode_value:INTEGER, '
                   'Failed_CapId_value:INTEGER, Failed_year_column_value:INTEGER, Failed_plate_column_value:INTEGER, '
                   'Failed_Volume_check:INTEGER, Failed_unique_check:INTEGER',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == "__main__":
    dataset = sys.argv[1] if len(sys.argv) > 1 else 'BLACKBOOK'

    project_id = 'your_project_id'
    raw_zone_bucket = 'your_raw_zone_bucket'
    certify_zone_bucket = 'your_certify_zone_bucket'
    folder_path = 'data_folder_path/'
    bq_dataset_id = 'your_dataset_id'
    bq_table_name = 'consolidated_record_count_report'

    print("************************ Recon Started for Rawzone and Certify Zone ********************")
    run_pipeline(project_id, raw_zone_bucket, certify_zone_bucket, folder_path, bq_dataset_id, bq_table_name, dataset)
    print("************************ Recon Finished for Rawzone and Certify Zone ********************")
