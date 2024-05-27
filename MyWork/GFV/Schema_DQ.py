import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
import pandas as pd
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration for schemas and filenames
SCHEMA_MAP = {
    'file1.csv': ['col1', 'col2', 'col3'],
    'file2.csv': ['colA', 'colB', 'colC'],
    # Add more mappings as needed
}
MIN_VOLUME = 100
SPECIAL_CHARACTERS = re.compile(r'[@_!#$%^&*()<>?/|}{~:]')

class CheckSchema(beam.DoFn):
    def process(self, element, schema_map):
        filename, content = element
        expected_schema = schema_map.get(filename)
        if not expected_schema:
            raise ValueError(f"No schema defined for file {filename}")
        df = pd.read_csv(pd.compat.StringIO(content))
        if list(df.columns) == expected_schema:
            yield element
        else:
            raise ValueError(f"Schema mismatch in file {filename}")

class CheckNullValues(beam.DoFn):
    def process(self, element):
        filename, content = element
        df = pd.read_csv(pd.compat.StringIO(content))
        if df.isnull().values.any():
            raise ValueError(f"Null values found in file {filename}")
        else:
            yield element

class CheckVolume(beam.DoFn):
    def process(self, element):
        filename, content = element
        df = pd.read_csv(pd.compat.StringIO(content))
        if len(df) < MIN_VOLUME:
            raise ValueError(f"File {filename} has less than {MIN_VOLUME} records")
        else:
            yield element

class CheckSpecialCharacters(beam.DoFn):
    def process(self, element):
        filename, content = element
        df = pd.read_csv(pd.compat.StringIO(content))
        if df.applymap(lambda x: bool(SPECIAL_CHARACTERS.search(str(x)))).any().any():
            raise ValueError(f"Special characters found in file {filename}")
        else:
            yield element

def process_file(element, cerify_bucket_name, processed_folder, error_folder):
    from google.cloud import storage

    filename, content = element
    storage_client = storage.Client()
    consumer_bucket = storage_client.bucket(cerify_bucket_name)

    try:
        df = pd.read_csv(pd.compat.StringIO(content))
        processed_blob = consumer_bucket.blob(f"{processed_folder}/{filename}")
        processed_blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
        logging.info(f"Processed and uploaded {filename} to {processed_folder}")

    except Exception as e:
        error_blob = consumer_bucket.blob(f"{error_folder}/{filename}")
        error_blob.upload_from_string(content, content_type='text/csv')
        logging.error(f"Error processing file {filename}: {e}")

def run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, cerify_bucket_name, certify_folder_path):
    options = PipelineOptions(
        project=project_id,
        runner="DataflowRunner",
        temp_location=f'gs://{raw_zone_bucket_name}/temp',
        region='europe-west2',
        staging_location=f'gs://{raw_zone_bucket_name}/staging',
        service_account_email='svc-dfl-user@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',
        dataflow_kms_key='projects/tnt01-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptoKeys/keyhsm-kms-tnt01-euwe2-cdp',
        subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88h/regions/europe-west2/subnetworks/odycda-csn-euwe2-kc1-01-bld-01',
        num_workers=1,
        max_num_workers=4,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )
    
    processed_folder = f"{certify_folder_path}/Processed"
    error_folder = f"{certify_folder_path}/Error"

    p = beam.Pipeline(options=options)

    raw_zone_bucket = storage.Client().bucket(raw_zone_bucket_name)
    blobs = list(raw_zone_bucket.list_blobs(prefix=raw_zone_folder_path))

    files = [(blob.name.split('/')[-1], blob.download_as_string().decode('utf-8')) for blob in blobs if blob.name.endswith('.csv')]

    (p
     | "CreateFileList" >> beam.Create(files)
     | "CheckSchema" >> beam.ParDo(CheckSchema(), schema_map=SCHEMA_MAP)
     | "CheckNullValues" >> beam.ParDo(CheckNullValues())
     | "CheckVolume" >> beam.ParDo(CheckVolume())
     | "CheckSpecialCharacters" >> beam.ParDo(CheckSpecialCharacters())
     | "ProcessAndUpload" >> beam.Map(process_file, cerify_bucket_name=cerify_bucket_name, processed_folder=processed_folder, error_folder=error_folder)
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    project_id = 'tnt01-odycda-bld-01-1b81'
    raw_zone_bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    cerify_bucket_name = "tnt1092gisnnd872391a"
    
    raw_zone_folder_path = "thParty/GFV/Monthly/SFGDrop"
    certify_folder_path = 'thParty/GFV/Monthly/'

    logging.info("Starting the Dataflow pipeline")
    run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, cerify_bucket_name, certify_folder_path)
    logging.info("Dataflow pipeline completed")
