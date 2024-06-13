import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from google.cloud import storage
import csv
from datetime import datetime
import io

class VolumeCheckAndClassify(beam.DoFn):
    def __init__(self, raw_zone_bucket_name, error_folder_path, processed_folder_path):
        self.raw_zone_bucket_name = raw_zone_bucket_name
        self.error_folder = error_folder_path
        self.processed_folder = processed_folder_path

    def setup(self):
        self.storage_client = storage.Client(project_id)

    def process(self, file_path):
        print(f"Processing file: {file_path}")
        
        bucket = self.storage_client.bucket(self.raw_zone_bucket_name)
        blob = bucket.blob(file_path)
        content = blob.download_as_string().decode("utf-8")
        reader = csv.reader(content.splitlines())

        # Count the number of records, ignoring the header row
        record_count = sum(1 for row in reader) - 1
        filename = file_path.split("/")[-1]

        if record_count < 100:
            print(f"File {filename} has less than 100 records, moving to error folder")
            destination_blob_name = f"{self.error_folder}/{filename}"
        else:
            print(f"File {filename} has 100 or more records, moving to processed folder")
            destination_blob_name = f"{self.processed_folder}/{filename}"
        
        # Move the file to the appropriate folder
        destination_blob = bucket.blob(destination_blob_name)
        destination_blob.upload_from_string(content)
        blob.delete()

        yield file_path

def copy_processed_files_to_certify_zone(classified_bucket_name, processed_folder_path, certify_zone_bucket_name, certify_folder_path):
    print("Copying processed files to certify zone")
    storage_client = storage.Client(project_id)
    
    classified_bucket = storage_client.bucket(classified_bucket_name)
    certify_bucket = storage_client.bucket(certify_zone_bucket_name)
    
    processed_blobs = list(classified_bucket.list_blobs(prefix=processed_folder_path))
    
    for blob in processed_blobs:
        filename = blob.name.split("/")[-1]
        destination_blob_name = f"{certify_folder_path}/{filename}"
        
        # Copy file to the certify zone bucket
        certify_blob = certify_bucket.blob(destination_blob_name)
        certify_blob.rewrite(blob)
        
        # Delete the file from the classified folder
        blob.delete()
        
        print(f"Copied and deleted {filename} from classified to certify zone")

def run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, classified_folder_path, certify_zone_bucket_name, certify_folder_path):
    # Configure pipeline options for DataflowRunner
    options = PipelineOptions(
        project=project_id,
        runner="DirectRunner",
        region='europe-west2',
        staging_location=f'gs://{raw_zone_bucket_name}/staging',
        service_account_email='svc-dfl-user@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',
        dataflow_kms_key='projects/tnt01-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptoKeys/keyhsm-kms-tnt01-euwe2-cdp',
        subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88h/regions/europe-west2/subnetworks/odycda-csn-euwe2-kcl-01-bld-01',
        num_workers=1,
        max_num_workers=4,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )

    print("Initializing Google Cloud Storage client")
    storage_client = storage.Client(project_id)
    bucket = storage_client.get_bucket(raw_zone_bucket_name)
    blobs = [blob.name for blob in bucket.list_blobs(prefix=raw_zone_folder_path) if blob.name.endswith('.csv') and '/' not in blob.name[len(raw_zone_folder_path):].strip('/')]

    print(f"Found {len(blobs)} CSV files in raw zone folder: {raw_zone_folder_path}")
    
    # Define the Beam pipeline
    p = beam.Pipeline(options=options)
    
    files = p | 'Create File List' >> beam.Create(blobs)
    
    # Apply the volume check and classification transformation
    files | 'Volume Check and Classify' >> beam.ParDo(
        VolumeCheckAndClassify(
            raw_zone_bucket_name=raw_zone_bucket_name,
            error_folder_path=f"{classified_folder_path}/error",
            processed_folder_path=f"{classified_folder_path}/processed"
        )
    )

    print("Running the Beam pipeline")
    p.run().wait_until_finish()
    print("Pipeline execution completed")

    # Copy the processed files to the certify zone bucket
    copy_processed_files_to_certify_zone(
        classified_bucket_name=raw_zone_bucket_name,
        processed_folder_path=f"{classified_folder_path}/processed",
        certify_zone_bucket_name=certify_zone_bucket_name,
        certify_folder_path=certify_folder_path
    )

if __name__ == "__main__":
    # Define the main function and set the appropriate project and bucket details
    project_id = 'your-gcp-project-id'
    raw_zone_bucket_name = 'your-raw-zone-bucket'
    raw_zone_folder_path = 'your-raw-folder-path'
    classified_folder_path = 'your-classified-folder-path'  # Placeholder for classified files (processed, error)
    certify_zone_bucket_name = 'your-certify-zone-bucket'  # New placeholder for processed files directly
    certify_folder_path = 'your-certify-folder-path'  # New placeholder for processed files directly

    # Run the pipeline
    run_pipeline(
        project_id, raw_zone_bucket_name,raw_zone_folder_path,classified_folder_path,certify_zone_bucket_name, certify_folder_path)
