from google.cloud import storage
import csv
import os

# Initialize GCS client
client = storage.Client()

# Configuration
raw_bucket_name = 'your-raw-bucket'
raw_folder_path = 'your-raw-folder-path'

def process_schema_files(bucket_name, folder_path):
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    
    schema_files = []
    
    for blob in blobs:
        if blob.name.endswith('.schema.csv'):
            schema_files.append(blob)

    output_file_count = 1
    file_count = 0
    output_file_path = f'output_{output_file_count}.csv'
    
    # Open the initial output CSV file for writing
    output_file = open(output_file_path, mode='w', newline='')
    csv_writer = csv.writer(output_file)

    for blob in schema_files:
        if file_count == 3:
            output_file.close()
            output_file_count += 1
            file_count = 0
            output_file_path = f'output_{output_file_count}.csv'
            output_file = open(output_file_path, mode='w', newline='')
            csv_writer = csv.writer(output_file)
        
        # Extract folder name from the blob name
        folder_name = blob.name.split('/')[-2].split('-')[1]
        file_name_prefix = os.path.basename(blob.name).replace('.schema.csv', '')

        # Read the schema file content
        content = blob.download_as_text()
        lines = content.strip().split('\n')

        for line in lines:
            # Write folder name and file name prefix
            csv_writer.writerow([folder_name, file_name_prefix])
            # Write schema data
            csv_writer.writerow([line])

        file_count += 1

    output_file.close()
    print(f"Output written to {output_file_path}")

# Run the script
try:
    process_schema_files(raw_bucket_name, raw_folder_path)
except Exception as e:
    print(f"An error occurred: {e}")
