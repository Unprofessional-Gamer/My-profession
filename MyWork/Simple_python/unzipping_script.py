from google.cloud import storage
import zipfile
import io

def list_files(bucket_name, folder):
    """List all files in a specific GCS folder."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder)
    return [blob.name for blob in blobs if blob.name.endswith('.zip')]

def extract_and_upload_zip_file(bucket_name, zip_file_path, destination_folder):
    """Extract CSV files from a zip file and upload them to GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(zip_file_path)
    
    zip_data = blob.download_as_bytes()
    with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
        for file_info in z.infolist():
            if file_info.filename.endswith('.csv'):
                print(f'Extracting and uploading file: {file_info.filename}')
                with z.open(file_info) as extracted_file:
                    destination_path = f'{destination_folder}/{file_info.filename}'
                    destination_blob = bucket.blob(destination_path)
                    destination_blob.upload_from_file(extracted_file)
                print(f'Uploaded file to: {destination_path}')
    
    # Delete the processed zip file
    blob.delete()
    print(f'Deleted zip file: {zip_file_path}')

def run_pipeline(project_id, bucket_name, source_folder_1, source_folder_2, destination_folder):
    source_files_1 = list_files(bucket_name, source_folder_1)
    source_files_2 = list_files(bucket_name, source_folder_2)

    all_source_files = source_files_1 + source_files_2

    for zip_file in all_source_files:
        extract_and_upload_zip_file(bucket_name, zip_file, destination_folder)

if __name__ == '__main__':
    PROJECT_ID = 'your-project-id'
    BUCKET_NAME = 'your-bucket-name'
    SOURCE_FOLDER_1 = 'path/to/source/folder1'
    SOURCE_FOLDER_2 = 'path/to/source/folder2'
    DESTINATION_FOLDER = 'path/to/destination/folder'

    run_pipeline(PROJECT_ID, BUCKET_NAME, SOURCE_FOLDER_1, SOURCE_FOLDER_2, DESTINATION_FOLDER)
