import requests
import base64
import xml.etree.ElementTree as ET
from google.cloud import storage
import logging

def download_and_upload_to_gcs():
    try:
        # Fetch the SOAP response
        response = requests.get("https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=406&Password=lloyd406&ProductID=1324")
        
        logging.info("Fetched the response.... merging all the chunks")
        
        # Parse the XML response
        root = ET.fromstring(response.content)
        
        # Check for the Success element
        success_element = root.find('.//{https://soap.cap.co.uk/datadownload/}Success')
        if success_element is None or success_element.text != '1':
            raise Exception("Download was not successful")
        
        file_name = root.find('.//{https://soap.cap.co.uk/datadownload/}Name').text
        
        logging.info(f"For filename: {file_name}, chunks are being merged")
        
        # Extract and decode chunks
        chunks = [chunk.text for chunk in root.findall('.//{https://soap.cap.co.uk/datadownload/}Chunk')]
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
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    
    project_id = "tnt-01-bld"
    folder_path = 'thParty/MFVS/GFV/'

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    download_and_upload_to_gcs()
