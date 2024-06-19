import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems
import zipfile
import io
import os

def extract_and_upload_zip_file(file_path, destination_folder, bucket_name):
    """Extract and upload CSV files from a zip file into a folder named after the zip file."""
    print(f'Starting processing of file: {file_path}')
    zip_data = FileSystems.open(file_path).read()
    with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
        # Extract folder name from file_path (e.g., 'gs://bucket/path/to/file.zip' -> 'file')
        folder_name = os.path.splitext(os.path.basename(file_path))[0]
        for file_info in z.infolist():
            if file_info.filename.endswith('.csv'):
                print(f'Extracting file: {file_info.filename}')
                with z.open(file_info) as extracted_file:
                    # Destination path: gs://bucket/destination_folder/file/filename.csv
                    destination_path = f'gs://{bucket_name}/{destination_folder}/{folder_name}/{file_info.filename}'
                    with FileSystems.create(destination_path) as dest:
                        dest.write(extracted_file.read())
                print(f'Uploaded file: {file_info.filename} to {destination_path}')
    # Delete the processed zip file from the bucket
    FileSystems.delete([file_path])
    print(f'Deleted file: {file_path} from source bucket')

def run_beam_pipeline(project_id, bucket_name, source_folder_1, source_folder_2, destination_folder):
    pipeline_options = PipelineOptions(
        project=project_id,
        runner="DataflowRunner",
        job_name="extract-zip-files",
        temp_location=f'gs://{bucket_name}/temp',
        region='europe-west2',
        staging_location=f'gs://{bucket_name}/staging',
        service_account_email='your-service-account-email',
        dataflow_kms_key='your-kms-key',
        subnetwork='your-subnetwork',
        num_workers=1,
        max_num_workers=4,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # List all zip files in both source folders
        files_1 = beam.io.match.MatchFiles(f'gs://{bucket_name}/{source_folder_1}/*.zip').expand()
        files_2 = beam.io.match.MatchFiles(f'gs://{bucket_name}/{source_folder_2}/*.zip').expand()

        files = (
            pipeline
            | "Create file list" >> beam.Create([file.path for file in files_1 + files_2])
        )

        extracted_files = files | "Extract and upload files" >> beam.Map(
            extract_and_upload_zip_file, destination_folder, bucket_name
        )

    print("Beam pipeline execution completed.")

if __name__ == '__main__':
    PROJECT_ID = 'my-project-id'
    BUCKET_NAME = 'my-bucket-name'
    SOURCE_FOLDER_1 = 'source-folder-1'
    SOURCE_FOLDER_2 = 'source-folder-2'
    DESTINATION_FOLDER = 'destination-folder'

    run_beam_pipeline(PROJECT_ID, BUCKET_NAME, SOURCE_FOLDER_1, SOURCE_FOLDER_2, DESTINATION_FOLDER)
