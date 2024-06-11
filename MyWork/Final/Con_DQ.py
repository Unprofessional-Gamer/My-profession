import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from google.cloud import storage
import csv
from datetime import datetime
import io

# Constants
DATE_FORMAT = "%d-%m-%y"
REPORT_PROCESSED_FILENAME = "consolidated_report_processed.csv"
REPORT_ERROR_FILENAME = "consolidated_report_error.csv"

class VolumeCheckAndClassify(beam.DoFn):
    def __init__(self, raw_zone_bucket_name, processed_folder, error_folder):
        self.raw_zone_bucket_name = raw_zone_bucket_name
        self.processed_folder = processed_folder
        self.error_folder = error_folder

    def setup(self):
        # Setup the storage client for Google Cloud Storage
        self.storage_client = storage.Client()

    def process(self, file_path):
        print(f"Processing file: {file_path}")
        
        bucket = self.storage_client.bucket(self.raw_zone_bucket_name)
        blob = bucket.blob(file_path)
        content = blob.download_as_string().decode("utf-8")
        reader = csv.reader(content.splitlines())

        # Count the number of records, ignoring the header row
        record_count = sum(1 for row in reader) - 1
        filename = file_path.split("/")[-1]
        current_date = datetime.now().strftime(DATE_FORMAT)

        if record_count < 100:
            print(f"File {filename} has less than 100 records, moving to error folder")
            destination_blob_name = f"{self.error_folder}/{filename}"
            report_data = [current_date, filename, record_count]
            yield beam.pvalue.TaggedOutput('error', report_data)
        else:
            print(f"File {filename} has 100 or more records, moving to processed folder")
            destination_blob_name = f"{self.processed_folder}/{filename}"
            report_data = [current_date, filename, record_count]
            yield beam.pvalue.TaggedOutput('processed', report_data)
        
        # Move the file to the appropriate folder
        destination_blob = bucket.blob(destination_blob_name)
        destination_blob.upload_from_string(content)
        blob.delete()

        yield file_path

class CreateOrAppendReport(beam.DoFn):
    def __init__(self, raw_zone_bucket_name, report_filename):
        self.raw_zone_bucket_name = raw_zone_bucket_name
        self.report_filename = report_filename

    def setup(self):
        # Setup the storage client for Google Cloud Storage
        self.storage_client = storage.Client()

    def process(self, report_data_list):
        print(f"Creating or appending report: {self.report_filename}")
        
        bucket = self.storage_client.bucket(self.raw_zone_bucket_name)
        report_blob = bucket.blob(f"reports/{self.report_filename}")

        # Download existing report content if available
        existing_content = ""
        if report_blob.exists():
            existing_content = report_blob.download_as_string().decode("utf-8")
        
        # Create in-memory file object to store updated report
        updated_content = io.StringIO()
        writer = csv.writer(updated_content)

        if existing_content:
            writer.writerows(csv.reader(existing_content.splitlines()))
        
        writer.writerows(report_data_list)
        
        # Upload the updated report content
        report_blob.upload_from_string(updated_content.getvalue())
        print(f"Report {self.report_filename} updated successfully")

class MoveProcessedFiles(beam.DoFn):
    def __init__(self, raw_zone_bucket_name, processed_folder, certify_zone_bucket_name, received_folder):
        self.raw_zone_bucket_name = raw_zone_bucket_name
        self.processed_folder = processed_folder
        self.certify_zone_bucket_name = certify_zone_bucket_name
        self.received_folder = received_folder

    def setup(self):
        # Setup the storage client for Google Cloud Storage
        self.storage_client = storage.Client()

    def process(self, file_path):
        raw_bucket = self.storage_client.bucket(self.raw_zone_bucket_name)
        certify_bucket = self.storage_client.bucket(self.certify_zone_bucket_name)
        blob = raw_bucket.blob(file_path)

        if file_path.startswith(self.processed_folder):
            destination_blob_name = f"{self.received_folder}/{file_path.split('/')[-1]}"
            certify_blob = certify_bucket.blob(destination_blob_name)
            certify_blob.rewrite(blob)
            blob.delete()
            print(f"Moved processed file {file_path} to certify zone bucket")
            yield file_path

def volume_count_check(project_id, raw_zone_bucket_name, raw_zone_folder_path, certify_zone_bucket_name, certify_received_folder):
    # Configure pipeline options for DataflowRunner
    options = PipelineOptions(
        project=project_id,
        runner="DataflowRunner",
        region='europe-west2',
        staging_location=f'gs://{raw_zone_bucket_name}/staging',
        service_account_email='svc-dfl-user@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',
        dataflow_kms_key='projects/tnt01-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptoKeys/keyhsm-kms-tnt01-euwe2-cdp',
        subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88h/regions/europe-west2/subnetworks/odycda-csn-euwe2-kc-01-bld-01',
        num_workers=1,
        max_num_workers=4,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )

    print("Initializing Google Cloud Storage client")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(raw_zone_bucket_name)
    blobs = [blob.name for blob in bucket.list_blobs(prefix=raw_zone_folder_path) if blob.name.endswith('.csv')]

    print(f"Found {len(blobs)} CSV files in raw zone folder: {raw_zone_folder_path}")
    
    # Define the Beam pipeline
    p = beam.Pipeline(options=options)
    
    files = p | 'Create File List' >> beam.Create(blobs)
    
    # Apply the volume check and classification transformation
    classified = (
        files 
        | 'Volume Check and Classify' >> beam.ParDo(
            VolumeCheckAndClassify(
                raw_zone_bucket_name=raw_zone_bucket_name,
                processed_folder='processed',
                error_folder='error'
            )
        ).with_outputs('processed', 'error', main='main')
    )

    # Collect reports for processed and error files
    processed_reports = classified.processed | 'Collect Processed Reports' >> beam.combiners.ToList()
    error_reports = classified.error | 'Collect Error Reports' >> beam.combiners.ToList()

    # Create or append to the consolidated report for processed files
    processed_reports | 'Create Processed Report' >> beam.ParDo(
        CreateOrAppendReport(
            raw_zone_bucket_name,
            report_filename=REPORT_PROCESSED_FILENAME
        )
    )

    # Create or append to the consolidated report for error files
    error_reports | 'Create Error Report' >> beam.ParDo(
        CreateOrAppendReport(
            raw_zone_bucket_name,
            report_filename=REPORT_ERROR_FILENAME
        )
    )

    # Move the processed files to the certify zone bucket
    files | 'Move Processed Files' >> beam.ParDo(
        MoveProcessedFiles(
            raw_zone_bucket_name,
            processed_folder='processed',certify_zone_bucket_name=certify_zone_bucket_name,received_folder=certify_received_folder
        )
    )

    print("Running the Beam pipeline")
    p.run().wait_until_finish()
    print("Pipeline execution completed")

if __name__ == "__main__":
    # Define the main function and set the appropriate project and bucket details
    project_id = 'your-gcp-project-id'
    raw_zone_bucket_name = 'your-raw-zone-bucket'
    raw_zone_folder_path = 'your-raw-folder-path'
    certify_zone_bucket_name = 'your-consumer-bucket'
    certify_received_folder = 'your-received-folder-path'

    # Run the pipeline
    print("*****************************Data Quality Check initiated******************************")
    volume_count_check(project_id,raw_zone_bucket_name,raw_zone_folder_path,certify_zone_bucket_name,certify_received_folder)
    print("*****************************Data Quality Check Finished******************************")