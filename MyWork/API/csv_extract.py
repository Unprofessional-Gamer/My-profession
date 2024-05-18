import io
import zipfile
import requests
import base64
import xml.etree.ElementTree as ET
from google.cloud import storage
import logging

def download_and_extract_zip_from_gcs():
    try:
        # Initialize Google Cloud Storage client
        client = storage.Client()
        
        # Define GCS bucket and path of the ZIP file
        bucket_name = "your-bucket-name"
        zip_file_path = "thp_arty/MFVS/20240501-BlackBookPlatinumPlus-CSV.zip"  # Specify the path to your ZIP file
        
        # Get the ZIP file from GCS
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(zip_file_path)
        
        # Read the ZIP file contents into memory
        zip_data = io.BytesIO()
        blob.download_to_file(zip_data)
        zip_data.seek(0)  # Reset file pointer to the beginning
        
        # Extract the contents of the ZIP file
        with zipfile.ZipFile(zip_data, 'r') as zip_ref:
            extracted_files = zip_ref.namelist()  # Get list of all files in the ZIP archive
            
            for file_name in extracted_files:
                file_data = zip_ref.read(file_name)  # Read file data from the ZIP archive
                
                # Define destination path in GCS (specify folder structure)
                destination_blob_name = f"thp_arty/MFVS/{file_name}"  # Adjust destination folder structure as needed
                
                # Determine content type based on file extension
                content_type = "text/csv" if file_name.lower().endswith(".csv") else "text/plain"
                
                # Upload extracted file to GCS with specified content type
                upload_blob_to_gcs(client, bucket_name, destination_blob_name, file_data, content_type)
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def upload_blob_to_gcs(client, bucket_name, destination_blob_name, file_data, content_type):
    # Upload file data to GCS with specified content type
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    blob.upload_from_string(file_data, content_type=content_type)
    
    logging.info(f"Uploaded '{destination_blob_name}' to GCS bucket '{bucket_name}' with content type '{content_type}'")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    download_and_extract_zip_from_gcs()
