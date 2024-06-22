from google.cloud import storage
import os
import time
from google.api_core.exceptions import TooManyRequests

# Initialize the GCS client
client = storage.Client()

def list_blobs_with_prefix(bucket_name, prefix):
    """Lists all the blobs in the bucket that begin with the prefix."""
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return blobs

def append_to_file(bucket_name, file_path, data, max_retries=5):
    """Appends data to a file in the bucket with retry logic."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    retries = 0
    while retries < max_retries:
        try:
            if blob.exists():
                current_data = blob.download_as_text()
            else:
                current_data = ''

            new_data = current_data + data
            blob.upload_from_string(new_data)
            break
        except TooManyRequests as e:
            retries += 1
            wait_time = 2 ** retries
            print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
        except Exception as e:
            print(f"An error occurred: {e}")
            break

def process_schema_files(raw_zone_bucket, raw_zone_folder_path, raw_zone_file_path_base):
    blobs = list_blobs_with_prefix(raw_zone_bucket, raw_zone_folder_path)
    buffer = ""
    file_counter = 0
    schema_file_counter = 0

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
            buffer += f"{folder_name},{prefix}\n"
            for line in lines:
                buffer += f"{line}\n"
            buffer += "\n"  # Adding a new line for separation

            schema_file_counter += 1

            # If we've processed 12 schema files, write the buffer to a new CSV file
            if schema_file_counter == 12:
                file_counter += 1
                raw_zone_file_path = f"{raw_zone_file_path_base}_part{file_counter}.csv"
                append_to_file(raw_zone_bucket, raw_zone_file_path, buffer)
                buffer = ""
                schema_file_counter = 0

    # Append any remaining data in the buffer
    if buffer:
        file_counter += 1
        raw_zone_file_path = f"{raw_zone_file_path_base}_part{file_counter}.csv"
        append_to_file(raw_zone_bucket, raw_zone_file_path, buffer)

# Configuration
raw_zone_bucket = 'your-raw-zone-bucket'
raw_zone_folder_path = 'your/raw/zone/folder/path'
raw_zone_file_path_base = 'your/raw/zone/file/path/target'

# Execute the processing function
process_schema_files(raw_zone_bucket, raw_zone_folder_path, raw_zone_file_path_base)
