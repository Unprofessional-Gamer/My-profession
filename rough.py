# Add these imports at the top
from apache_beam import DoFn, PTransform, ParDo, Pipeline
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import json

# DoFn to list files
class ListFilesDoFn(DoFn):
    def __init__(self, folder_path):
        self.folder_path = folder_path

    def process(self, element, buckets_info):
        from google.cloud import storage
        storage_client = storage.Client()

        for zone, bucket_name in buckets_info.items():
            bucket = storage_client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=self.folder_path)
            for blob in blobs:
                if blob.name.endswith(".csv") and "schema" not in blob.name and "metadata" not in blob.name:
                    yield {
                        "bucket": bucket_name,
                        "file_path": blob.name,
                        "zone": zone
                    }

# DoFn to process each file
class ProcessFilesDoFn(DoFn):
    def __init__(self, dataset, project_id, bq_dataset_id):
        self.dataset = dataset
        self.project_id = project_id
        self.bq_dataset_id = bq_dataset_id

    def process(self, element):
        from datetime import datetime

        bucket_name = element["bucket"]
        file_path = element["file_path"]
        zone = element["zone"]

        dataset_info = dataset_mapping.get(self.dataset, {})
        prefix = dataset_info.get("prefix", "")
        bq_table_map = dataset_info.get("tables", {})

        filename = file_path.split("/")[-1]
        if not filename.startswith(prefix):
            return

        # File to table mapping logic
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

        try:
            record_count, column_count, column_sums = get_record_count_and_sums(bucket_name, file_path, zone, skip_header, bq_schema)
        except Exception as e:
            logging.error(f"Error reading file: {file_path}, error: {str(e)}")
            return

        source_count = metadata_count(bucket_name, file_path) if zone == "RAW" else 0
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

        yield result


# Final run_pipeline function
def run_pipeline(project_id, dataset, folder_path, bq_dataset_id, table_id, buckets_info):
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Start" >> beam.Create([None])
            | "List Files" >> beam.ParDo(ListFilesDoFn(folder_path), buckets_info=buckets_info)
            | "Process Files" >> beam.ParDo(ProcessFilesDoFn(dataset, project_id, bq_dataset_id))
            | "Write to BQ" >> WriteToBigQuery(
                table=f"{project_id}:{bq_dataset_id}.{table_id}",
                schema=recon_consilidated_schema,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )
        )

# Main Entry
if __name__ == "__main__":
    env_config = load_env_config()
    dataset = sys.argv[1] if len(sys.argv) > 1 else "PRE_EMBARGO"
    folder_base = "DAILY" if dataset in ["PRE_EMBARGO", "CAP_NVD_CAR", "CAP_NVD_LCV"] else "MONTHLY"

    project_id = env_config.get("PROJECT")
    raw_bucket = env_config.get("RAW_ZONE")
    cert_bucket = env_config.get("CERT_ZONE")
    analytic_bucket = env_config.get("ANALYTIC_ZONE")
    bq_dataset_id = env_config.get("BQ_DATASET")
    cap_hpi_path = env_config.get("CAP_PATH")
    table_id = env_config.get("RECON_TABLE")

    folder_path = f"{cap_hpi_path}/{folder_base}/"

    buckets_info = {"RAW": raw_bucket, "CERT": cert_bucket, "ANALYTIC": analytic_bucket}

    run_pipeline(project_id, dataset, folder_path, bq_dataset_id, table_id, buckets_info)
