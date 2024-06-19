import requests
import base64
import xml.etree.ElementTree as ET
from google.cloud import storage
import logging
import io
import zipfile

def upload_blob_to_gcs(client, bucket_name, destination_blob_name, file_data, content_type):
    # Upload file data to GCS with specified content type
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(file_data, content_type=content_type)
    logging.info(f"Uploaded '{destination_blob_name}' to GCS bucket '{bucket_name}' with content type '{content_type}'")

def download_and_upload_to_gcs():
    try:
        # Fetch the SOAP response
        response = requests.get("https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=406&Password=lloyd406&ProductID=1324")
        
        logging.info("Fetched the response.... merging all the chunks")
        
        # Parse the XML response
        namespace = {'ns':'https://soap.cap.co.uk/datadownload/datadownload/'}
        root = ET.fromstring(response.content)
    
        success_element = ET.Element("Success")
        success_element.text = '1'
        root.append(success_element)

        file_name = root.find('.//ns:name', namespace).text
        
        logging.info(f"For filename: {file_name}, chunks are being merged")
        
        # Extract and decode chunks
        chunks = [chunk.text for chunk in root.findall('.//ns:Chunk', namespace)]
        file_data = b"".join(base64.b64decode(chunk) for chunk in chunks)
        
        logging.info("Chunks merged. Uploading to GCS bucket")
        
        # Initialize Google Cloud Storage client
        client = storage.Client(project=project_id)
        
        # Define your GCS bucket and destination file path
        bucket_name = "your-bucket-name"
        destination_blob_name = folder_path + file_name
        
        # Upload the file data to GCS with specific content type for ZIP
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(file_data, content_type='application/zip')
        
        logging.info(f"File '{file_name}' uploaded to GCS bucket '{bucket_name}' at path '{destination_blob_name}' with content type 'application/zip'")
        
        return file_name  # Return the file name for the next step
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

def download_and_extract_zip_from_gcs(file_name):
    try:
        # Initialize Google Cloud Storage client
        client = storage.Client()
        
        # Define GCS bucket and path of the ZIP file
        bucket_name = "your-bucket-name"
        zip_file_path = folder_path + file_name  # Use the file name from the previous step
        
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
            
            for extracted_file_name in extracted_files:
                file_data = zip_ref.read(extracted_file_name)  # Read file data from the ZIP archive
                
                # Define destination path in GCS (specify folder structure)
                destination_folder_name = file_name.replace('.zip', '')  # Create a folder name from the zip file name
                destination_blob_name = f"{folder_path}{destination_folder_name}/{extracted_file_name}"  # Adjust destination folder structure
                
                # Determine content type based on file extension
                content_type = "text/csv" if extracted_file_name.lower().endswith(".csv") else "text/plain"
                
                # Upload extracted file to GCS with specified content type
                upload_blob_to_gcs(client, bucket_name, destination_blob_name, file_data, content_type)
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    project_id = "tnt-01-bld"
    folder_path = 'thParty/MFVS/GFV/'

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Download and upload the ZIP file to GCS
        file_name = download_and_upload_to_gcs()
        
        # Extract the ZIP file contents and upload them to GCS
        download_and_extract_zip_from_gcs(file_name)
        
    except Exception as e:
        logging.error(f"Process failed: {e}")
