import requests
import base64
import xml.etree.ElementTree as ET
from google.cloud import storage
import logging

# List of product URLs
product_urls = [
    "https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=406&Password=lloyd406&ProductID=1200",
    "https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=406&Password=lloyd406&ProductID=1204",
    "https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=406&Password=lloyd406&ProductID=1206",
    "https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=406&Password=lloyd406&ProductID=1314",
    "https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=406&Password=lloyd406&ProductID=1326"
]

def download_and_upload_to_gcs(url):
    try:
        # Fetch the SOAP response using the provided URL
        response = requests.get(url)
        
        logging.info("Fetched the response.... merging all the chunks")
        
        # Parse the XML response
        namespace = {'ns': 'https://soap.cap.co.uk/datadownload/datadownload/'}
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
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    
    project_id = "tnt-01-bld"
    folder_path = 'thParty/MFVS/GFV/'

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Loop through each product URL and process
    for product_url in product_urls:
        logging.info(f"Processing URL: {product_url}")
        download_and_upload_to_gcs(product_url)
