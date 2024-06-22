from google.cloud import storage
import os

# Initialize the GCS client
client = storage.Client()

def list_blobs_with_prefix(bucket_name, prefix):
    """Lists all the blobs in the bucket that begin with the prefix."""
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return blobs

def append_to_file(bucket_name, file_path, data):
    """Appends data to a file in the bucket."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    if blob.exists():
        current_data = blob.download_as_text()
    else:
        current_data = ''

    new_data = current_data + data
    blob.upload_from_string(new_data)

def process_schema_files(raw_zone_bucket, raw_zone_folder_path, raw_zone_file_path):
    blobs = list_blobs_with_prefix(raw_zone_bucket, raw_zone_folder_path)
    
    for blob in blobs:
        if blob.name.endswith('.schema.csv'):
            # Extract the folder name and prefix
            folder_name = os.path.dirname(blob.name).split('/')[-1].split('-')[1]
            prefix = os.path.basename(blob.name).split('.')[0]
            
            # Download the content of the schema file
            schema_data = blob.download_as_text()

            # Split the schema data into lines
            lines = schema_data.strip().split('\n')
            
            # Create data to append in the desired format
            data_to_append = ""
            for line in lines:
                data_to_append += f"{folder_name},{prefix}\n{line}\n"
            
            # Append the data to the target file in raw zone file path
            append_to_file(raw_zone_bucket, raw_zone_file_path, data_to_append)

# Configuration
raw_zone_bucket = 'your-raw-zone-bucket'
raw_zone_folder_path = 'your/raw/zone/folder/path'
raw_zone_file_path = 'your/raw/zone/file/path/target.csv'

# Execute the processing function
process_schema_files(raw_zone_bucket, raw_zone_folder_path, raw_zone_file_path)
