import os
import io
import zipfile
from google.cloud import storage

def process_zip_files(bucket_name, source_folder, destination_folder):
    """Process zip files from GCS, upload extracted CSV files to the destination folder in GCS, and delete the zip files."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blobs = bucket.list_blobs(prefix=source_folder)
    
    for blob in blobs:
        if blob.name.endswith('.zip'):
            print(f'Processing {blob.name}')
            # Read the zip file content into memory
            zip_data = blob.download_as_bytes()
            
            # Open the zip file from bytes
            with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                    if file_info.filename.endswith('.csv'):
                        print(f'Extracting {file_info.filename}')
                        with zip_ref.open(file_info) as csv_file:
                            # Upload extracted CSV to the destination folder in GCS
                            destination_blob_path = os.path.join(destination_folder, file_info.filename)
                            destination_blob = bucket.blob(destination_blob_path)
                            destination_blob.upload_from_file(csv_file)
                            print(f'Uploaded {file_info.filename} to gs://{bucket_name}/{destination_blob_path}')
            
            # Delete the processed zip file from the bucket
            blob.delete()
            print(f'Deleted {blob.name} from gs://{bucket_name}/{source_folder}')

def main(bucket_name, source_folder, destination_folder):
    process_zip_files(bucket_name, source_folder, destination_folder)
    
if __name__ == '__main__':
    BUCKET_NAME = 'your-bucket-name'
    SOURCE_FOLDER = 'path/to/raw/zone/folder'
    DESTINATION_FOLDER = 'path/to/destination/folder'
    
    main(BUCKET_NAME, SOURCE_FOLDER, DESTINATION_FOLDER)
