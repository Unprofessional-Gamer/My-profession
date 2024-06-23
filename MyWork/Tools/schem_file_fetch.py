from google.cloud import storage
import csv
import os

# Initialize GCS client
client = storage.Client()

# Configuration
raw_bucket_name = 'your-raw-bucket'
raw_folder_path = 'your-raw-folder-path'
output_file_path = 'output.csv'

def process_schema_files(bucket_name, folder_path, output_file_path):
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    
    # Open the output CSV file for writing
    with open(output_file_path, mode='w', newline='') as output_file:
        csv_writer = csv.writer(output_file)
        # Process each blob in the specified folder
        for blob in blobs:
            if blob.name.endswith('.schema.csv'):
                # Extract folder name from the blob name
                folder_name = blob.name.split('/')[0].split('-')[1]
                file_name_prefix = os.path.basename(blob.name).replace('.schema.csv', '')

                # Read the schema file content
                content = blob.download_as_text()
                lines = content.strip().split('\n')

                for line in lines:
                    # Write folder name and file name prefix
                    csv_writer.writerow([folder_name, file_name_prefix])
                    # Write schema data
                    csv_writer.writerow([line])

# Run the script
process_schema_files(raw_bucket_name, raw_folder_path, output_file_path)
