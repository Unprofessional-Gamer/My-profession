from google.cloud import storage, bigquery
from io import StringIO
import pandas as pd
import sys,csv
from io import StringIO
from datetime import datetime, timedelta
import apache_beam as beam
from apache_beam.io.gcp.gcsio import GcsIO
from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Consolidated schema for BigQuery
consolidated_schema = {
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
        {"name": "ANAL_RECORDS", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ANAL_FAILED_RECORDS", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "RAW_COLUMN", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "CERT_COLUMN", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ANAL_COLUMN", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ANAL_col_sums", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "column_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sum_value", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "BQ_STATUS", "type": "STRING", "mode": "NULLABLE"},
        {"name": "BQ_FAILED", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "REASON", "type": "STRING", "mode": "NULLABLE"}
    ]
}

dataset_prefix = {
    "BLACKBOOK": {"files":["CPRVAL"]},
    "REDBOOK":  {"files":["LPRVAL"]},
    "GOLDBOOK":  {"files":["CPRRVN"]},
    "CAP_NVD_CAR": {"files":["TechnicalStyle.csv","Schema.csv","Sales.csv"],
    "TechnicalStyle.csv":  {"bq_table":"CAR_TECHNICALSTYLE","index":{0:'code',1:'filename',2:'detail',3:'info'}},
    "Schema.csv":  {"bq_table":"CAR_SCHEMA","index":{0:'code',1:'filename',2:'detail',3:'info'}},
    "Sales.csv":  {"bq_table":"CAR_Sales","index":{0:'code',1:'filename',2:'detail',3:'info'}} 
            },
    "CAP_NVD_LCV": {"files":["TechnicalStyle.csv","Schema.csv","Sales.csv"],
    "TechnicalStyle.csv":  {"bq_table":"LCV_TECHNICALSTYLE","index":{0:'code',1:'filename',2:'detail',3:'info'}},
    "Schema.csv":  {"bq_table":"LCV_SCHEMA","index":{0:'code',1:'filename',2:'detail',3:'info'}},
    "Sales.csv":  {"bq_table":"LCV_SALES","index":{0:'code',1:'filename',2:'detail',3:'info'}} 
            }
}

# Function to list CSV files in the GCS folder
def list_files_in_folder(bucket_name, folder_path, dataset):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=folder_path)
        
        # Access the files list properly from dataset_prefix
        target_files = dataset_prefix.get(dataset, {}).get("files", [])
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

# Beam DoFns for distributed processing
class FileMetadataExtractorFn(beam.DoFn):
    def __init__(self, dataset, folder_path, buckets_info, project_id):
        self.dataset = dataset
        self.folder_path = folder_path
        self.buckets_info = buckets_info
        self.project_id = project_id
        
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
                "RAW_RECORDS": 0, "CERT_RECORDS": 0, "ANAL_RECORDS": 0,
                "RAW_FAILED_RECORDS": 0, "CERT_FAILED_RECORDS": 0, "ANAL_FAILED_RECORDS": 0,
                "RAW_COLUMN": 0, "CERT_COLUMN": 0, "ANAL_COLUMN": 0,
                "ANAL_col_sums": [],
                "BQ_STATUS": "", "BQ_FAILED": 0, "REASON": ""
            }
            
            # Process RAW zone
            try:
                # Get metadata count
                source_count = self.metadata_count(raw_bucket, file_path, "RAW")
                record["SOURCE_COUNT"] = source_count
                
                # Get record count and column count
                raw_count, raw_columns = self.get_record_count(raw_bucket, file_path, "RAW", False)
                record["RAW_RECORDS"] = raw_count
                record["RAW_COLUMN"] = raw_columns
                record["RAW_FAILED_RECORDS"] = source_count - raw_count if source_count > raw_count else 0
            except Exception as e:
                logging.error(f"Error processing RAW zone for {filename}: {str(e)}")
            
            # Process CERT zone
            try:
                cert_count, cert_columns = self.get_record_count(cert_bucket, file_path, "CERT", False)
                record["CERT_RECORDS"] = cert_count
                record["CERT_COLUMN"] = cert_columns
                record["CERT_FAILED_RECORDS"] = record["RAW_RECORDS"] - cert_count if record["RAW_RECORDS"] > cert_count else 0
            except Exception as e:
                logging.error(f"Error processing CERT zone for {filename}: {str(e)}")
            
            # Process ANALYTIC zone
            try:
                anal_count, anal_columns = self.get_record_count(anal_bucket, file_path, "ANALYTIC", True)
                record["ANAL_RECORDS"] = anal_count
                record["ANAL_COLUMN"] = anal_columns
                record["ANAL_FAILED_RECORDS"] = record["CERT_RECORDS"] - anal_count if record["CERT_RECORDS"] > anal_count else 0
                
                # Get column sums for ANALYTIC zone
                column_sums = self.get_col_sum_dynamic(anal_bucket, file_path, True, "ANALYTIC", self.dataset)
                record["ANAL_col_sums"] = column_sums
                
                # Check BQ status
                self.check_bq_status(anal_bucket, self.dataset, file_path, record)
            except Exception as e:
                logging.error(f"Error processing ANALYTIC zone for {filename}: {str(e)}")
            
            yield record
        except Exception as e:
            logging.error(f"Error processing file {file_info}: {str(e)}")
    
    def metadata_count(self, bucket_name, file_path, zone):
        try:
            bucket = self.storage_client.bucket(bucket_name)
            metadatafile = file_path[:-4] + ".metadata.csv"
            blob = bucket.blob(metadatafile)
            
            if not blob.exists():
                logging.warning(f"Metadata file {metadatafile} does not exist")
                return 0
                
            content = blob.download_as_text(encoding='iso-8859-1')
            
            df = pd.read_csv(StringIO(content))
            
            if 'Key' in df.columns and 'Value' in df.columns and 'Total Records' in df['Key'].values:
                total_records = df[df['Key'] == 'Total Records']['Value'].values[0]
                logging.info(f"Metadata file {file_path} count: {total_records}")
                return int(total_records)
            logging.warning(f"Could not find 'Total Records' in metadata file {metadatafile}")
            return 0
        except Exception as e:
            logging.error(f"Error getting metadata count for {file_path}: {str(e)}")
            return 0

    def get_record_count(self, bucket_name, file_path, zone, skip_header):
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            
            if not blob.exists():
                logging.warning(f"File {file_path} does not exist in {zone} zone")
                return 0, 0
                
            content = blob.download_as_text(encoding='iso-8859-1')
            
            quotechar = '"' if zone == "RAW" else "'"

            df = pd.read_csv(StringIO(content), header=0 if skip_header else None, 
                            low_memory=False, delimiter=',', quotechar=quotechar, 
                            quoting=csv.QUOTE_ALL, encoding='iso-8859-1')
            
            column_count = len(df.columns)
            record_count = len(df)
            
            logging.info(f"File {file_path} in {zone} zone has {record_count} records and {column_count} columns")
            return record_count, column_count
        except Exception as e:
            logging.error(f"Error getting record count for {file_path} in {zone} zone: {str(e)}")
            return 0, 0
    
    def get_col_sum_dynamic(self, bucket_name, file_path, skip_header, zone, dataset):
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            
            if not blob.exists():
                logging.warning(f"File {file_path} does not exist in {zone} zone for column sum")
                return []
                
            content = blob.download_as_text(encoding='iso-8859-1')
            
            quotechar = '"' if zone == "RAW" else "'"

            df = pd.read_csv(StringIO(content), header=0 if skip_header else None, 
                            low_memory=False, delimiter=',', quotechar=quotechar, 
                            quoting=csv.QUOTE_ALL, encoding='iso-8859-1')

            numeric_columns = df.select_dtypes(include=["int64", "float64"]).columns
            if len(numeric_columns) == 0:
                logging.warning(f"No numeric columns found in {file_path}")
                return []

            filename = file_path.split("/")[-1]
            
            try:
                # Try to get the index values for this file type
                if dataset in dataset_prefix and filename in dataset_prefix[dataset]:
                    index_values = dataset_prefix[dataset][filename]["index"]
                    
                    column_sums = []
                    for idx, col in enumerate(numeric_columns, start=0):
                        column_name = index_values.get(idx, str(col))
                        column_sums.append({
                            "column_name": str(column_name),
                            "sum_value": str(df[col].sum())
                        })
                    return column_sums
            except Exception as e:
                logging.error(f"Error getting column sums for {filename}: {str(e)}")
            
            # Default if we can't find index values
            return [{
                "column_name": str(col),
                "sum_value": str(df[col].sum())
            } for col in numeric_columns]
        except Exception as e:
            logging.error(f"Error calculating column sums for {file_path}: {str(e)}")
            return []
    
    def check_bq_status(self, ana_bucket_name, dataset, file_path, record):
        try:
            filename = file_path.split('/')[-1]
            
            client = storage.Client(self.project_id)
            fs = GcsIO(client)
            
            gcs_file_path = f"gs://{ana_bucket_name}/{file_path}"
            logging.info(f"Checking BigQuery status for {gcs_file_path}")
            
            if not fs.exists(gcs_file_path):
                logging.warning(f"File {gcs_file_path} does not exist")
                record["BQ_STATUS"] = "File Not Found"
                record["REASON"] = f"File {gcs_file_path} does not exist"
                return
                
            with fs.open(gcs_file_path, mode='r') as pf:
                ana_lines = pf.readlines()
                ana_count = len(ana_lines) - 1 if ana_lines else 0
                
                if dataset in ["CAP_NVD_CAR", "CAP_NVD_LCV", "PRE_EMBARGO_J", "PRE_EMBARGO_LR"]:
                    file_key = filename
                    
                    # Check if this file type exists in dataset_prefix
                    if dataset in dataset_prefix and file_key in dataset_prefix[dataset]:
                        bq_table = dataset_prefix[dataset][file_key]["bq_table"]
                        
                        QUERY = f"""
                            SELECT COUNT(*) as count FROM `{self.project_id}.{dataset}.{bq_table}`
                        """
                        
                        bq_client = bigquery.Client(self.project_id)
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
                            
                            record["BQ_STATUS"] = bq_status
                            record["BQ_FAILED"] = bq_failed_count
                            record["REASON"] = reason
                            
                        logging.info(f"BQ check for {filename}: {record['BQ_STATUS']} (Ana: {ana_count}, BQ: {bq_count})")
        except Exception as e:
            logging.error(f"Error checking BQ status for {file_path}: {str(e)}")
            record["BQ_STATUS"] = "Error"
            record["REASON"] = str(e)

# Beam pipeline to process and write to BigQuery
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
                | "Process files" >> beam.ParDo(FileMetadataExtractorFn(dataset, folder_path, buckets_info, project_id))
            )
            
            # Write results to BigQuery
            files_pcoll | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=f"{project_id}:{bq_dataset_id}.{table_id}",
                schema=consolidated_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )

        logging.info("Pipeline execution completed successfully")
    except Exception as e:
        logging.error(f"Error running pipeline: {str(e)}")

if __name__ == "__main__":
    try:
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Determine the dataset from command-line argument, defaulting to "REDBOOK"
        dataset = sys.argv[1] if len(sys.argv) > 1 else "REDBOOK"
        # Use "DAILY" folder for specific datasets and "MONTHLY" for others
        folder_base = "DAILY" if dataset in ["CAP_NVD_CAR", "CAP_NVD_LCV", "PRE_EMBARGO_J", "PRE_EMBARGO_LR"] else "MONTHLY"

        # Replace these with your actual project details
        project_id = "your_project_id"  # REPLACE WITH ACTUAL PROJECT ID
        bq_dataset_id = "your_dataset_id"  # REPLACE WITH ACTUAL DATASET ID
        
        # Bucket IDs - replace with your actual bucket names
        raw_bucket = "raw_001uejeu_uej"
        cert_bucket = "cert_001uejeu_uej"
        anal_bucket = "anal_001uejeu_uej"

        table_id = "RECON_TABLE"

        month_folder = "20250721"
        folder_path = f"EXTERNAL/MFVS/PRICING/CAP-HPI/{dataset}/{folder_base}/RECEIVED/{month_folder}/"

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
