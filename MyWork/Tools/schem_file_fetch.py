from google.cloud import storage
import csv
import os

# Initialize GCS client
client = storage.Client()

# Configuration
raw_bucket_name = 'your-raw-bucket'
raw_folder_path = 'your-raw-folder-path'
output_dir_path = 'output_files'

# Ensure the output directory exists
os.makedirs(output_dir_path, exist_ok=True)

def process_schema_files(bucket_name, folder_path, output_dir_path):
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    
    schema_files = [blob for blob in blobs if blob.name.endswith('.schema.csv')]

    output_file_count = 1
    file_count = 0
    output_file_path = os.path.join(output_dir_path, f'output_{output_file_count}.csv')
    
    # Open the initial output CSV file for writing
    output_file = open(output_file_path, mode='w', newline='')
    csv_writer = csv.writer(output_file)

    for blob in schema_files:
        if file_count == 5:
            output_file.close()
            print(f"Output written to {output_file_path}")
            output_file_count += 1
            file_count = 0
            output_file_path = os.path.join(output_dir_path, f'output_{output_file_count}.csv')
            output_file = open(output_file_path, mode='w', newline='')
            csv_writer = csv.writer(output_file)
        
        # Extract folder name from the blob name
        try:
            folder_name = blob.name.split('/')[-2].split('-')[1]
            file_name_prefix = os.path.basename(blob.name).replace('.schema.csv', '')
        except IndexError as e:
            print(f"Error extracting folder name or file name prefix for blob: {blob.name}. Error: {e}")
            continue

        try:
            # Read the schema file content
            content = blob.download_as_text()
            lines = content.strip().split('\n')
        except Exception as e:
            print(f"Error downloading or processing content from blob: {blob.name}. Error: {e}")
            continue

        for line in lines:
            try:
                # Write folder name and file name prefix
                csv_writer.writerow([folder_name, file_name_prefix])
                # Write schema data
                csv_writer.writerow([line])
            except Exception as e:
                print(f"Error writing to CSV file: {output_file_path}. Error: {e}")
                continue

        file_count += 1

    output_file.close()
    print(f"Output written to {output_file_path}")

# Run the script
try:
    process_schema_files(raw_bucket_name, raw_folder_path, output_dir_path)
except Exception as e:
    print(f"An error occurred: {e}")
