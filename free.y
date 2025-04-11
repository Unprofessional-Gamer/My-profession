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
from google.api_core.exceptions import NotFound, GoogleAPIError
import csv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    "PRE_EMBARGO": {
        "prefix": "Pre_emabrgo_land_rover_",
        "tables": {
            "Capder": "PRE_EMBARGO_LR_CAPDER",
            "Price": "PRE_EMBARGO_LR_PRICE",
            "OPTION": "PRE_EMBARGO_LR_OPTION"
        }},
    "CAP_NVD_LCV": {
        "prefix": "LIGHTS_",
        "tables": {
            "Capder": "CAP_LCV_CAPDER",
            "Price": "CAP_LCV_PRICE",
            "OPTION": "CAP_LCV_OPTION"
        }},
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
    try:
        bq_client = bigquery.Client(project=project_id)
        table_ref = bq_client.dataset(dataset_id).table(table_id)
        table = bq_client.get_table(table_ref)
        schema = table.schema
        return {field.name: field.field_type for field in schema}
    except NotFound:
        raise Exception(f"Table {dataset_id}.{table_id} not found in project {project_id}")
    except GoogleAPIError as e:
        raise Exception(f"BigQuery API error: {e}")

# Function to get column names from BigQuery schema
def get_bq_column_names(bq_schema, exclude_columns=[]):
    return [col for col in bq_schema if col not in exclude_columns]

# Function to get metadata record count
def metadata_count(bucket_name, metadata_file):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        metadatafile = metadata_file[:-4] + ".metadata.csv"
        blob = bucket.blob(metadatafile)
        if not blob.exists():
            return 0

        content = blob.download_as_text(encoding='iso-8859-1')
        df = pd.read_csv(StringIO(content))
        if 'Key' in df.columns and 'Value' in df.columns and 'Total Records' in df['Key'].values:
            return int(df[df['Key'] == 'Total Records']['Value'].values[0])
        return 0
    except Exception as e:
        logging.error(f"Error getting metadata count for {metadata_file}: {str(e)}")
        return 0

def check_received_folder(project_id, raw_bucket, cap_hpi_path, dataset, folder_base):
    try:
        client = storage.Client(project=project_id)
        received_dates = []
        received_path = f'{cap_hpi_path}/{dataset}/{folder_base}/Files/'
        received_list = client.list_blobs(raw_bucket, prefix=received_path)
        for blob in received_list:
            if blob.name.endswith('.csv') and 'metadata' not in blob.name and 'schema' not in blob.name:
                received_dates.append(blob.name.split('/')[-2])
        return max(set(received_dates)) if received_dates else None
    except Exception as e:
        logging.error(f"Error checking received folder: {str(e)}")
        return None

# Function to list CSV files in the GCS folder
def list_files_in_folder(bucket_name, folder_path, dataset):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=folder_path)
        
        # Access the files list properly from dataset_mapping
        target_files = dataset_mapping.get(dataset, {}).get("files", [])
        if not target_files:
            logging.warning(f"No target files found for dataset {dataset}")
            return []
            
        files = []
        for blob in blobs:
            if blob.name.endswith('.csv'):
                # Check if any target file string is in the blob name
                if any(x in blob.name for x in target_files):
                    files.append(blob.name)
        
        logging.info(f"Found {len(files)} matching files in {bucket_name}/{folder_path}")
        return files
    except Exception as e:
        logging.error(f"Error listing files in bucket {bucket_name}: {str(e)}")
        return []

# Function to read CSV file and get record count & column sums
def get_record_count_and_sums(bucket_name, file_path, zone, skip_header, bq_schema=None):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        if not blob.exists():
            logging.warning(f"File {file_path} does not exist in {zone} zone")
            return 0, 0, []
            
        content = blob.download_as_text(encoding='iso-8859-1')
        
        quotechar = '"' if zone == "RAW" else "'"

        # If we have a schema, use it to name columns
        if bq_schema:
            column_names = get_bq_column_names(bq_schema)
            df = pd.read_csv(StringIO(content), header=0 if skip_header else None, 
                            names=column_names, low_memory=False, delimiter=',', 
                            quotechar=quotechar, quoting=csv.QUOTE_ALL, 
                            encoding='iso-8859-1')
        else:
            df = pd.read_csv(StringIO(content), header=0 if skip_header else None, 
                            low_memory=False, delimiter=',', quotechar=quotechar, 
                            quoting=csv.QUOTE_ALL, encoding='iso-8859-1')
        
        column_count = len(df.columns)
        record_count = len(df) - (1 if skip_header else 0)
        
        # Compute column sums if it's ANALYTIC zone
        column_sums = []
        if zone == "ANALYTIC":
            numeric_columns = df.select_dtypes(include=["int64", "float64"]).columns
            column_sums = [{"column_name": str(col), "sum_value": str(df[col].sum())} 
                          for col in numeric_columns]
        
        logging.info(f"File {file_path} in {zone} zone has {record_count} records and {column_count} columns")
        return record_count, column_count, column_sums
    except Exception as e:
        logging.error(f"Error getting record count for {file_path} in {zone} zone: {str(e)}")
        return 0, 0, []

# Function to check data consistency with BigQuery
def check_bq_status(ana_bucket_name, dataset, file_path, project_id, bq_table_name):
    try:
        filename = file_path.split('/')[-1]
        
        client = storage.Client(project_id)
        fs = GcsIO(client)
        
        gcs_file_path = f"gs://{ana_bucket_name}/{file_path}"
        logging.info(f"Checking BigQuery status for {gcs_file_path}")
        
        if not fs.exists(gcs_file_path):
            logging.warning(f"File {gcs_file_path} does not exist")
            return "File Not Found", 0, f"File {gcs_file_path} does not exist"
            
        with fs.open(gcs_file_path, mode='r') as pf:
            ana_lines = pf.readlines()
            ana_count = len(ana_lines) - 1 if ana_lines else 0
            
            QUERY = f"""
                SELECT COUNT(*) as count FROM `{project_id}.{dataset}.{bq_table_name}`
            """
            
            bq_client = bigquery.Client(project_id)
            query_job = bq_client.query(QUERY)
            rows = query_job.result()
            
            bq_failed_count = 0
            bq_count = 0
            
            for row in rows:
                bq_count = row.count
                if bq_count == ana_count:
                    bq_status = "Match"
                    reason = f"{bq_status}: Bcoz ana_records({ana_count}) == BQ_count({bq_count})"
                else:
                    bq_status = "Not Match"
                    bq_failed_count = abs(ana_count - bq_count)
                    reason = f"{bq_status}: Bcoz ana_records({ana_count}) != BQ_count({bq_count}) = mismatch({bq_failed_count})"
                
            logging.info(f"BQ check for {filename}: {bq_status} (Ana: {ana_count}, BQ: {bq_count})")
            return bq_status, bq_failed_count, reason
    except Exception as e:
        logging.error(f"Error checking BQ status for {file_path}: {str(e)}")
        return "Error", 0, str(e)

# Beam DoFn for processing files
class ProcessFileFn(beam.DoFn):
    def __init__(self, dataset, buckets_info, project_id, bq_dataset_id):
        self.dataset = dataset
        self.buckets_info = buckets_info
        self.project_id = project_id
        self.bq_dataset_id = bq_dataset_id
        self.records = {}
        
    def setup(self):
        # Initialize clients in the setup method to avoid serialization issues
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        
    def process(self, file_info):
        """Process a single file across all zones"""
        try:
            raw_bucket = self.buckets_info["RAW"]
            cert_bucket = self.buckets_info["CERT"]
            anal_bucket = self.buckets_info["ANALYTIC"]
            
            # Get basic file info
            file_path = file_info
            filename = file_path.split("/")[-1]
            
            logging.info(f"Processing file: {filename}")
            
            # File metadata - handle potential index errors
            try:
                pick_date = file_path.split("/")[-2]
                folder_date = f"{pick_date[:4]}-{pick_date[4:6]}-{pick_date[6:]}"
            except (IndexError, ValueError):
                folder_date = datetime.now().strftime("%Y-%m-%d")
                logging.warning(f"Could not parse date from file path {file_path}, using current date")
                
            processed_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Initialize record
            record = {
                "DATASET": self.dataset,
                "FILE_DATE": folder_date,
                "PROCESSED_DATE_TIME": processed_time,
                "FILENAME": filename,
                "SOURCE_COUNT": 0,
                "RAW_RECORDS": 0, "CERT_RECORDS": 0, "ANALYTIC_RECORDS": 0,
                "RAW_FAILED_RECORDS": 0, "CERT_FAILED_RECORDS": 0, "ANALYTIC_FAILED_RECORDS": 0,
                "RAW_COLUMN": 0, "CERT_COLUMN": 0, "ANALYTIC_COLUMN": 0,
                "ANALYTIC_col_sums": [],
                "BQ_STATUS": "", "BQ_FAILED": 0, "REASON": ""
            }
            
            # Get dataset info
            dataset_info = dataset_mapping.get(self.dataset, {})
            prefix = dataset_info.get("prefix", "")
            bq_table_map = dataset_info.get("tables", {})
            
            # Extract table name key based on dataset and filename pattern
            if self.dataset == "CAP_NVD_LCV" and filename.startswith("LIGHTS_"):
                # For CAP_NVD_LCV, extract the part after the date (e.g., "Capder" from "LIGHTS_2025-03-21_Capder.csv")
                parts = filename.replace(prefix, "").replace(".csv", "").split("_")
                if len(parts) >= 3:  # Ensure we have enough parts
                    table_name_key = parts[-1]  # Get the last part (e.g., "Capder")
                else:
                    table_name_key = filename.replace(prefix, "").replace(".csv", "")
            else:
                # For other datasets, use the original logic
                table_name_key = filename.replace(prefix, "").replace(".csv", "")
                
            bq_table_name = bq_table_map.get(table_name_key, "")
            
            if not bq_table_name:
                logging.warning(f"Could not find BigQuery table for {filename} with key {table_name_key}")
                return  # Skip this file if table name is not found
                
            # Get BigQuery schema
            try:
                bq_schema = get_bq_schema(self.project_id, self.dataset, bq_table_name)
            except Exception as e:
                logging.warning(f"Could not get schema for {self.dataset}.{bq_table_name}: {str(e)}")
                bq_schema = None
            
            # Process RAW zone
            try:
                # Get metadata count
                source_count = metadata_count(raw_bucket, file_path)
                record["SOURCE_COUNT"] = source_count
                
                # Get record count and column count
                raw_count, raw_columns, _ = get_record_count_and_sums(raw_bucket, file_path, "RAW", False, bq_schema)
                record["RAW_RECORDS"] = raw_count
                record["RAW_COLUMN"] = raw_columns
                record["RAW_FAILED_RECORDS"] = source_count - raw_count if source_count > raw_count else 0
            except Exception as e:
                logging.error(f"Error processing RAW zone for {filename}: {str(e)}")
            
            # Process CERT zone
            try:
                cert_count, cert_columns, _ = get_record_count_and_sums(cert_bucket, file_path, "CERT", False, bq_schema)
                record["CERT_RECORDS"] = cert_count
                record["CERT_COLUMN"] = cert_columns
                record["CERT_FAILED_RECORDS"] = record["RAW_RECORDS"] - cert_count if record["RAW_RECORDS"] > cert_count else 0
            except Exception as e:
                logging.error(f"Error processing CERT zone for {filename}: {str(e)}")
            
            # Process ANALYTIC zone
            try:
                anal_count, anal_columns, column_sums = get_record_count_and_sums(anal_bucket, file_path, "ANALYTIC", True, bq_schema)
                record["ANALYTIC_RECORDS"] = anal_count
                record["ANALYTIC_COLUMN"] = anal_columns
                record["ANALYTIC_FAILED_RECORDS"] = record["CERT_RECORDS"] - anal_count if record["CERT_RECORDS"] > anal_count else 0
                record["ANALYTIC_col_sums"] = column_sums
                
                # Check BQ status if we have a table name
                if bq_table_name:
                    bq_status, bq_failed_count, reason = check_bq_status(anal_bucket, self.dataset, file_path, self.project_id, bq_table_name)
                    record["BQ_STATUS"] = bq_status
                    record["BQ_FAILED"] = bq_failed_count
                    record["REASON"] = reason
            except Exception as e:
                logging.error(f"Error processing ANALYTIC zone for {filename}: {str(e)}")
            
            yield record
        except Exception as e:
            logging.error(f"Error processing file {file_info}: {str(e)}")

# Run Apache Beam pipeline
def run_pipeline(project_id, dataset, folder_path, raw_bucket, cert_bucket, anal_bucket, bq_dataset_id, table_id):
    try:
        job_name = f"reconciliation_{dataset}".lower().replace('-', '_')[:40]
        
        options = PipelineOptions(
            project=project_id,
            runner="DataflowRunner",
            region="europe-west2",
            job_name=job_name,
            staging_location=f"gs://{raw_bucket}/staging",
            temp_location=f"gs://{raw_bucket}/temp",
            num_workers=4,
            max_num_workers=16,
            use_public_ips=False,
            autoscaling_algorithm="THROUGHPUT_BASED",
            save_main_session=True,
            disk_size_gb=100,
            machine_type="n1-standard-4",
            experiments=["use_runner_v2"]  # Use the latest runner for better performance
        )

        buckets_info = {"RAW": raw_bucket, "CERT": cert_bucket, "ANALYTIC": anal_bucket}

        # Test listing files before starting the pipeline
        all_files = list_files_in_folder(raw_bucket, folder_path, dataset)
        if not all_files:
            logging.error(f"No files found in {raw_bucket}/{folder_path} for dataset {dataset}")
            return
            
        logging.info(f"Starting pipeline with {len(all_files)} files")

        with beam.Pipeline(options=options) as p:
            # Create PCollection of files
            files_pcoll = (p 
                | "Create file list" >> beam.Create(all_files)
                | "Process files" >> beam.ParDo(ProcessFileFn(dataset, buckets_info, project_id, bq_dataset_id))
            )
            
            # Write results to BigQuery
            files_pcoll | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=f"{project_id}:{bq_dataset_id}.{table_id}",
                schema=recon_consilidated_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )

        logging.info("Pipeline execution completed successfully")
    except Exception as e:
        logging.error(f"Error running pipeline: {str(e)}")

if __name__ == "__main__":
    try:
        # Load environment configuration
        env_config = load_env_config()
        
        # Determine the dataset from command-line argument, defaulting to "REDBOOK"
        dataset = sys.argv[1] if len(sys.argv) > 1 else "REDBOOK"
        # Use "DAILY" folder for specific datasets and "MONTHLY" for others
        folder_base = "DAILY" if dataset in ["CAP_NVD_CAR", "CAP_NVD_LCV", "PRE_EMBARGO_J", "PRE_EMBARGO_LR"] else "MONTHLY"

        # Get configuration from environment variables
        project_id = env_config.get('PROJECT', "your_project_id")
        raw_bucket = env_config.get('RAW_ZONE', "raw_001uejeu_uej")
        cert_bucket = env_config.get('CERT_ZONE', "cert_001uejeu_uej")
        anal_bucket = env_config.get('ANALYTIC_ZONE', "anal_001uejeu_uej")
        bq_dataset_id = env_config.get('BQ_DATASET', "your_dataset_id")
        cap_hpi_path = env_config.get("CAP_PATH", "EXTERNAL/MFVS/PRICING/CAP-HPI")
        table_id = env_config.get("RECON_TABLE", "RECON_TABLE")

        # Get the latest month folder from the received folder
        month_folder = check_received_folder(project_id, raw_bucket, cap_hpi_path, dataset, folder_base)
        if not month_folder:
            month_folder = datetime.now().strftime("%Y%m%d")
            logging.warning(f"Could not find month folder, using current date: {month_folder}")
            
        folder_path = f"{cap_hpi_path}/{dataset}/{folder_base}/RECEIVED/{month_folder}/"

        print(f"Processing files for dataset: {dataset} in folder: {folder_path}")
        print("************************ Recon Started ********************")
        
        run_pipeline(
            project_id=project_id,
            dataset=dataset,
            folder_path=folder_path,
            raw_bucket=raw_bucket,
            cert_bucket=cert_bucket,
            anal_bucket=anal_bucket,
            bq_dataset_id=bq_dataset_id,
            table_id=table_id
        )
        
        print("************************ Recon Finished ********************")
    except Exception as e:
        logging.error(f"Error during execution: {str(e)}")
        sys.exit(1)
