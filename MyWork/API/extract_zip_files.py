import zipfile
import os
import io
from google.cloud import storage

def list_files_in_bucket(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs]

def download_blob_to_memory(bucket_name, source_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    data = blob.download_as_bytes()
    return data

def upload_blob_from_memory(bucket_name, destination_blob_name, data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data)
    print(f"Uploaded to {destination_blob_name}")

def process_zip_files(raw_zone_bucket, raw_zone_folder, destination_bucket, destination_folder):
    # List all zip files in the raw zone folder
    zip_files = list_files_in_bucket(raw_zone_bucket, raw_zone_folder)

    for zip_file in zip_files:
        if zip_file.endswith('.zip'):
            # Create folder name without extension
            folder_name = os.path.splitext(zip_file.split('/')[-1])[0]
            
            # Download the zip file to memory
            zip_data = download_blob_to_memory(raw_zone_bucket, zip_file)
            
            # Unzip the file in memory
            with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as zip_ref:
                for file_name in zip_ref.namelist():
                    file_data = zip_ref.read(file_name)
                    destination_blob_name = f"{destination_folder}/{folder_name}/{file_name}"
                    upload_blob_from_memory(destination_bucket, destination_blob_name, file_data)

# Define your variables
raw_zone_bucket = 'bwiu8-raw_zone_277'
raw_zone_folder = 'thParty/MFVS/GFV/ZIP'
destination_bucket = 'destination_bucket'
destination_folder = 'thParty/MFVS/EXtract'

# Process the zip files
process_zip_files(raw_zone_bucket, raw_zone_folder, destination_bucket, destination_folder)
