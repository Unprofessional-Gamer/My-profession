import requests
import base64
import xml.etree.ElementTree as ET
from google.cloud import storage
import logging
from datetime import datetime

def download_and_upload_to_gcs(api_url):
    try:
        # Fetch the SOAP response
        response = requests.get(api_url)
        
        logging.info("Fetched the response.... merging all the chunks")
        
        # Parse the XML response
        root = ET.fromstring(response.content)
        file_name = root.find('.//{https://soap.cap.co.uk/datadownload/}Name').text
        
        logging.info(f"For filename: {file_name}, chunks are being merged")
        
        # Extract and decode chunks
        chunks = [chunk.text for chunk in root.findall('.//{https://soap.cap.co.uk/datadownload/}Chunk')]
        file_data = b"".join(base64.b64decode(chunk) for chunk in chunks)
        
        logging.info("Chunks merged. Determining folder structure and uploading to GCS bucket")
        
        # Determine folder structure based on filename
        folder_path = determine_folder_path(file_name)
        
        # Initialize Google Cloud Storage client
        client = storage.Client()
        
        # Define GCS bucket and destination file path
        bucket_name = "your-bucket-name"
        destination_blob_name = f"{folder_path}/{file_name}"  # Combine folder path and filename
        
        # Upload the file data to GCS with specific content type for ZIP
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(file_data, content_type='application/zip')
        
        logging.info(f"File '{file_name}' uploaded to GCS bucket '{bucket_name}' at path '{destination_blob_name}' with content type 'application/zip'")
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def determine_folder_path(file_name):
    # Extract date information from the filename
    date_str = file_name.split('-')[0]  # Extract the date part from the filename
    date_obj = datetime.strptime(date_str, '%Y%m%d')  # Parse date string into datetime object
    
    # Determine folder structure based on filename and date
    if "BlackBookPlatinumPlus-CSV.zip" in file_name:
        folder_name = "Blackbook"
    elif "RedBookCodesAndDescriptions-csv.zip" in file_name:
        folder_name = "2020-Redbook"
    elif "GoldBookCodesAndDescriptions-csv.zip" in file_name:
        folder_name = "2018-Goldbook"
    elif "PlatinumBookCodesAndDescriptions-csv.zip" in file_name:
        folder_name = "Platinumbook"
    else:
        folder_name = "Other"  # Default folder for unknown files
    
    year_month = date_obj.strftime('%Y-%m')  # Format year and month as YYYY-MM
    
    # Construct folder path based on rules
    if folder_name == "Blackbook":
        folder_path = f"{folder_name}/{year_month}"
    elif folder_name == "2020-Redbook":
        folder_path = f"{folder_name}/{year_month}"
    elif folder_name == "2018-Goldbook":
        folder_path = f"{folder_name}/{year_month}"
    elif folder_name == "Platinumbook":
        folder_path = f"{folder_name}/{year_month}"
    else:
        folder_path = f"Other/{year_month}"  # Default folder path
    
    return folder_path

if __name__ == "__main__":
    # List of API URLs to fetch data from
    api_urls = [
        "https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=406&Password=lloyd406&ProductID=1324",
        "https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=407&Password=lloyd407&ProductID=1325",
        "https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=408&Password=lloyd408&ProductID=1326"
    ]
    
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Download and upload each API response to GCS
    for api_url in api_urls:
        download_and_upload_to_gcs(api_url)
