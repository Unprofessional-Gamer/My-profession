import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems
import zipfile
import io

def extract_and_upload_zip_file(file_path, destination_folder, bucket_name):
    """Extract and upload CSV files from a zip file using the zip filename as a folder name."""
    print(f'Starting processing of file: {file_path}')
    
    # Extract the base filename without extension
    zip_filename = file_path.split('/')[-1]
    folder_name = zip_filename.rsplit('.', 1)[0]
    
    zip_data = FileSystems.open(file_path).read()
    with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
        for file_info in z.infolist():
            if file_info.filename.endswith('.csv'):
                print(f'Extracting file: {file_info.filename}')
                with z.open(file_info) as extracted_file:
                    # Use the filename without the .zip extension as the folder name
                    destination_path = f'gs://{bucket_name}/{destination_folder}/{folder_name}/{file_info.filename}'
                    with FileSystems.create(destination_path) as dest:
                        dest.write(extracted_file.read())
                print(f'Uploaded file: {file_info.filename} to {destination_path}')
    
    # Delete the processed zip file from the bucket
    FileSystems.delete([file_path])
    print(f'Deleted file: {file_path} from source bucket')

def run_beam_pipeline(project_id, bucket_name, source_folder, destination_folder):
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
        # List all zip files in the source folder
        files = beam.io.match.MatchFiles(f'gs://{bucket_name}/{source_folder}/*.zip').expand()

        # Create a PCollection of file paths
        file_paths = (
            pipeline
            | "Create file list" >> beam.Create([file.path for file in files])
        )

        # Process each file
        extracted_files = file_paths | "Extract and upload files" >> beam.Map(
            extract_and_upload_zip_file, destination_folder, bucket_name
        )

    print("Beam pipeline execution completed.")

if __name__ == '__main__':
    PROJECT_ID = 'my-project-id'
    BUCKET_NAME = 'my-bucket-name'
    SOURCE_FOLDER = 'source-folder-1'
    DESTINATION_FOLDER = 'destination-folder'

    run_beam_pipeline(PROJECT_ID, BUCKET_NAME, SOURCE_FOLDER, DESTINATION_FOLDER)
