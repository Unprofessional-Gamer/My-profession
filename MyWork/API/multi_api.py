import requests
import base64
import xml.etree.ElementTree as ET
from google.cloud import storage
import logging

def download_and_upload_to_gcs(url):
    try:
        # Fetch the SOAP response
        response = requests.get(url)
        response.raise_for_status()  # Ensure we handle HTTP errors
        
        logging.info("Fetched the response.... merging all the chunks")
        
        # Parse the XML response
        namespace = {'ns':'https://soap.cap.co.uk/datadownload/datadownload/'}
        root = ET.fromstring(response.content)
    
        # Find file name element
        file_name_element = root.find('.//ns:name', namespace)
        if file_name_element is None:
            logging.error("File name element not found in the response")
            return
        file_name = file_name_element.text
        
        logging.info(f"For filename: {file_name}, chunks are being merged")
        
        # Extract and decode chunks
        chunks = root.findall('.//ns:Chunk', namespace)
        if not chunks:
            logging.error("No chunks found in the response")
            return
        
        file_data = b"".join(base64.b64decode(chunk.text) for chunk in chunks if chunk.text)
        
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
    
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP request failed: {e}")
    except ET.ParseError as e:
        logging.error(f"Failed to parse XML response: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    project_id = "tnt-01-bld"
    folder_path = 'thParty/MFVS/GFV/'
    
    urls = [
        "https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=406&Password=lloyd406&ProductID=1324",
        # Add more URLs here if needed
    ]

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    for url in urls:
        download_and_upload_to_gcs(url)
