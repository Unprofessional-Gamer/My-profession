import requests
import base64
import xml.etree.ElementTree as ET
from google.cloud import storage
import logging

def download_and_upload_to_gcs(product_ids):
    try:
        # Initialize Google Cloud Storage client
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        
        for product_id in product_ids:
            try:
                # Construct the URL for the current product ID
                url = f"https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=406&Password=lloyd406&ProductID={product_id}"
                
                # Fetch the SOAP response
                response = requests.get(url)
                response.raise_for_status()  # Raise an exception for HTTP errors
                
                logging.info(f"Fetched the response for Product ID: {product_id}.... merging all the chunks")
                
                # Parse the XML response
                namespace = {'ns':'https://soap.cap.co.uk/datadownload/datadownload/'}
                root = ET.fromstring(response.content)
            
                success_element = ET.Element("Success")
                success_element.text = '1'
                root.append(success_element)
                
                file_name_element = root.find('.//ns:name', namespace)
                if file_name_element is None or file_name_element.text is None:
                    logging.error(f"Missing <name> element in the response for Product ID: {product_id}")
                    logging.error(f"Response content for Product ID {product_id}: {response.content.decode('utf-8')}")
                    continue
                
                file_name = file_name_element.text
                
                logging.info(f"For filename: {file_name}, chunks are being merged")
                
                # Extract and decode chunks
                chunks = root.findall('.//ns:Chunk', namespace)
                if not chunks:
                    logging.error(f"Missing <Chunk> elements in the response for Product ID: {product_id}")
                    logging.error(f"Response content for Product ID {product_id}: {response.content.decode('utf-8')}")
                    continue
                
                file_data = b"".join(base64.b64decode(chunk.text) for chunk in chunks if chunk.text)
                
                logging.info("Chunks merged. Uploading to GCS bucket")
                
                # Define your GCS destination file path
                destination_blob_name = folder_path + file_name
                
                # Upload the file data to GCS with specific content type for ZIP
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_string(file_data, content_type='application/zip')
                
                logging.info(f"File '{file_name}' uploaded to GCS bucket '{bucket_name}' at path '{destination_blob_name}' with content type 'application/zip'")
            except Exception as e:
                logging.error(f"An error occurred for Product ID {product_id}: {e}")
                logging.error(f"Response content for Product ID {product_id}: {response.content.decode('utf-8')}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    project_id = "tnt-01-bld"
    folder_path = 'thParty/MFVS/GFV/'
    bucket_name = "your-bucket-name"
    
    # List of product IDs
    product_ids = [1324, 1328, 1330]

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    download_and_upload_to_gcs(product_ids)
