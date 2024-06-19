import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp import gcsfilesystem as gcsio
import zipfile
import io

def extract_and_upload_zip_file(file_path, destination_folder, bucket_name):
    """Extract and upload CSV files from a zip file using the zip filename as a folder name."""
    print(f'Starting processing of file: {file_path}')
    
    # Extract the base filename without extension
    zip_filename = file_path.split('/')[-1]
    folder_name = zip_filename.rsplit('.', 1)[0]
    
    zip_data = gcsio.GcsIO().open(file_path).read()
    with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
        for file_info in z.infolist():
            if file_info.filename.endswith('.csv'):
                print(f'Extracting file: {file_info.filename}')
                with z.open(file_info) as extracted_file:
                    # Use the filename without the .zip extension as the folder name
                    destination_path = f'gs://{bucket_name}/{destination_folder}/{folder_name}/{file_info.filename}'
                    gcsio.GcsIO().open(destination_path, 'w').write(extracted_file.read())
                print(f'Uploaded file: {file_info.filename} to {destination_path}')
    
    # Delete the processed zip file from the bucket
    gcsio.GcsIO().delete([file_path])
    print(f'Deleted file: {file_path} from source bucket')

def run_beam_pipeline(project_id, bucket_name, source_folder, destination_folder):
    pipeline_options = PipelineOptions(
        project=project_id,
        runner="DirectRunner",  # Use DirectRunner for local testing
        temp_location=f'gs://{bucket_name}/temp',
        region='us-central1',  # Change as per your preference
        staging_location=f'gs://{bucket_name}/staging',
        service_account_email='your-service-account-email',
        save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # List all zip files in the source folder
        files = (
            pipeline
            | "List files in source folder" >> beam.Create([f'gs://{bucket_name}/{source_folder}/'])
            | "Match files" >> beam.FlatMap(lambda path: gcsio.GcsIO().match([f'{path}*.zip']))
            | "Get metadata" >> beam.Map(lambda match_result: match_result.metadata_list)
            | "Flatten files" >> beam.FlatMap(lambda metadata_list: [file.path for file in metadata_list])
        )

        # Process each file
        extracted_files = files | "Extract and upload files" >> beam.Map(
            extract_and_upload_zip_file, destination_folder, bucket_name
        )

    print("Beam pipeline execution completed.")

if __name__ == '__main__':
    PROJECT_ID = 'my-project-id'
    BUCKET_NAME = 'my-bucket-name'
    SOURCE_FOLDER = 'source-folder-1'
    DESTINATION_FOLDER = 'destination-folder'

    run_beam_pipeline(PROJECT_ID, BUCKET_NAME, SOURCE_FOLDER, DESTINATION_FOLDER)
