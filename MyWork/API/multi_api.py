def download_and_upload_to_gcs(url):
    try:
        # Fetch the SOAP response
        response = requests.get(url)
        
        logging.info("Fetched the response.... merging all the chunks")
        
        # Parse the XML response
        namespace = {'ns':'https://soap.cap.co.uk/datadownload/datadownload/'}
        root = ET.fromstring(response.content)
<<<<<<< HEAD
    
        success_element = ET.Element("Success")
        success_element.text = '1'
        root.append(success_element)

        file_name = root.find('.//ns:name', namespace).text
=======
        namespace = {'ns': 'https://soap.cap.co.uk/datadownload/'}
        file_name_element = root.find('.//ns:Name', namespace)
        if file_name_element is None or file_name_element.text is None:
            logging.error(f"Missing <Name> element in the response for URL: {api_url}")
            return
        
        file_name = file_name_element.text
>>>>>>> 003c62ca0a28de8ed87c02c5df868fdbe00f51aa
        
        logging.info(f"For filename: {file_name}, chunks are being merged")
        
        # Extract and decode chunks
<<<<<<< HEAD
        chunks = [chunk.text for chunk in root.findall('.//ns:Chunk', namespace)]
        file_data = b"".join(base64.b64decode(chunk) for chunk in chunks)
        
        logging.info("Chunks merged. Uploading to GCS bucket")
=======
        chunks = root.findall('.//ns:Chunk', namespace)
        if not chunks:
            logging.error(f"Missing <Chunk> elements in the response for URL: {api_url}")
            return
        
        file_data = b"".join(base64.b64decode(chunk.text) for chunk in chunks if chunk.text)
        
        logging.info("Chunks merged. Determining folder structure and uploading to GCS bucket")
        
        # Determine folder structure based on filename
        folder_path = determine_folder_path(file_name)
>>>>>>> 003c62ca0a28de8ed87c02c5df868fdbe00f51aa
        
        # Initialize Google Cloud Storage client
        client = storage.Client(project=project_id)
        
<<<<<<< HEAD
        # Define your GCS bucket and destination file path
        bucket_name = "your-bucket-name"
        destination_blob_name = folder_path + file_name
=======
        # Define GCS bucket and destination file path
        bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-52fd781"
        destination_blob_name = f"{folder_path}/{file_name}"  # Combine folder path and filename
>>>>>>> 003c62ca0a28de8ed87c02c5df868fdbe00f51aa
        
        # Upload the file data to GCS with specific content type for ZIP
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(file_data, content_type='application/zip')
        
        logging.info(f"File '{file_name}' uploaded to GCS bucket '{bucket_name}' at path '{destination_blob_name}' with content type 'application/zip'")
    
    except Exception as e:
<<<<<<< HEAD
        logging.error(f"An error occurred: {e}")
=======
        logging.error(f"An error occurred for URL {api_url}: {e}")

def determine_folder_path(file_name):
    try:
        # Extract date information from the filename
        date_str = file_name.split('-')[0]  # Extract the date part from the filename
        date_obj = datetime.strptime(date_str, '%Y%m%d')  # Parse date string into datetime object
        
        year_month = date_obj.strftime('%Y-%m')  # Format year and month as YYYY-MM
        
        # Construct folder path based on date
        folder_path = f"{year_month}"
        return folder_path
    except Exception as e:
        logging.error(f"Error determining folder path for filename {file_name}: {e}")
        return "unknown_date"
>>>>>>> 003c62ca0a28de8ed87c02c5df868fdbe00f51aa

if __name__ == "__main__":
    project_id = "tnt-01-bld"
    folder_path = 'thParty/MFVS/GFV/'
    urls = [
        "https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=406&Password=lloyd406&ProductID=1324",
        # Add more URLs here
    ]

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    for url in urls:
        download_and_upload_to_gcs(url)
