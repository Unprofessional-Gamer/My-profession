
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems
import zipfile
import io

def extract_and_upload_zip_file(element, destination_folder, bucket_name):
    file_name = element
    zip_data = FileSystems.open(file_name).read()
    with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
        for file_info in z.infolist():
            if file_info.filename.endswith('.csv'):
                with z.open(file_info) as extracted_file:
                    destination_path = f'gs://{bucket_name}/{destination_folder}/{file_info.filename}'
                    with FileSystems.create(destination_path) as dest:
                        dest.write(extracted_file.read())
    # Optionally delete the zip file from GCS
    FileSystems.delete([file_name])

def run(project_id, bucket_name, source_folder_1, source_folder_2, destination_folder):
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
        source_files_1 = FileSystems.match([f'gs://{bucket_name}/{source_folder_1}/*.zip'])[0].metadata_list
        source_files_2 = FileSystems.match([f'gs://{bucket_name}/{source_folder_2}/*.zip'])[0].metadata_list

        # Combine the file lists
        source_files = [file.path for file in source_files_1 + source_files_2]

        # Create a PCollection of file paths
        file_paths = pipeline | beam.Create(source_files)

        # Process each file
        file_paths | beam.Map(extract_and_upload_zip_file, destination_folder, bucket_name)

if __name__ == '__main__':
    PROJECT_ID = 'your-project-id'
    BUCKET_NAME = 'your-bucket-name'
    SOURCE_FOLDER_1 = 'path/to/source/folder1'
    SOURCE_FOLDER_2 = 'path/to/source/folder2'
    DESTINATION_FOLDER = 'path/to/destination/folder'

    run(PROJECT_ID, BUCKET_NAME, SOURCE_FOLDER_1, SOURCE_FOLDER_2, DESTINATION_FOLDER)
########################################################################################################################################
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems
import zipfile
import io

def extract_and_upload_zip_file(file_path, destination_folder, bucket_name):
    """Extract and upload CSV files from a zip file."""
    print(f'Starting processing of file: {file_path}')
    zip_data = FileSystems.open(file_path).read()
    with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
        for file_info in z.infolist():
            if file_info.filename.endswith('.csv'):
                print(f'Extracting file: {file_info.filename}')
                with z.open(file_info) as extracted_file:
                    destination_path = f'gs://{bucket_name}/{destination_folder}/{file_info.filename}'
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
