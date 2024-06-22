import re
import pandas as pd
from google.cloud import storage
from io import BytesIO

# Replace with your credentials and bucket information
bucket_name = 'your-bucket-name'
raw_folder_path = 'path/to/your/raw/folder'
excel_output_path = 'path/to/your/output/excel.xlsx'

# Connect to Google Cloud Storage
client = storage.Client()
bucket = client.bucket(bucket_name)

# Regular expression to match folder names like YYYYMMDD-foldername-csv/
folder_regex = re.compile(r'^\d{8}-([^/]+)-csv/$')

# Function to list folders matching the pattern
def list_folders(bucket, prefix):
    folders = []
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        if blob.name.endswith('/'):
            match = folder_regex.match(blob.name)
            if match:
                folders.append(match.group(1))
    return folders

# Function to process schema files and write to Excel
def process_schema_files(bucket, folders, output_path):
    excel_writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
    
    for folder in folders:
        folder_prefix = f"{folder}-csv/"
        folder_blobs = bucket.list_blobs(prefix=folder_prefix)
        
        for blob in folder_blobs:
            if blob.name.endswith('.schema.csv'):
                # Read schema file
                content = blob.download_as_string()
                df = pd.read_csv(BytesIO(content))
                
                # Prepare data for Excel
                df['foldername'] = folder
                df['filename prefix'] = blob.name.split('/')[-1].split('.')[0]
                
                # Write to Excel sheet
                sheet_name = f"{folder}_{blob.name.split('/')[-1].split('.')[0]}"
                df.to_excel(excel_writer, sheet_name=sheet_name, index=False)
    
    excel_writer.save()

# Main script
if __name__ == "__main__":
    # List folders matching the pattern
    folders = list_folders(bucket, raw_folder_path)
    
    # Process schema files and write to Excel
    process_schema_files(bucket, folders, excel_output_path)
