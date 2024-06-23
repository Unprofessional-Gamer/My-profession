import re
import pandas as pd
from google.cloud import storage
from io import BytesIO

# Replace with your credentials and bucket information
bucket_name = 'your-bucket-name'
raw_folder_path = 'data/raw/'
output_csv_path = 'output/consolidated_schemas.csv'

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

# Function to process schema files and consolidate data into a single CSV
def process_schema_files(bucket, folders, output_path):
    consolidated_data = []
    
    for folder in folders:
        folder_prefix = f"{raw_folder_path}{folder}-csv/"
        folder_blobs = bucket.list_blobs(prefix=folder_prefix)
        
        for blob in folder_blobs:
            if blob.name.endswith('.schema.csv'):
                # Read schema file
                content = blob.download_as_string()
                df = pd.read_csv(BytesIO(content))
                
                # Prepare data with additional metadata columns
                filename_prefix = blob.name.split('/')[-1].split('.')[0]
                df.insert(0, 'filename', filename_prefix)
                df.insert(0, 'foldername', folder)
                
                # Append to the consolidated data list
                consolidated_data.append(df)
    
    # Concatenate all dataframes in the list
    if consolidated_data:
        final_df = pd.concat(consolidated_data, ignore_index=True)
        # Write the final dataframe to a CSV file
        final_df.to_csv(output_path, index=False)

# Main script
if __name__ == "__main__":
    # List folders matching the pattern
    folders = list_folders(bucket, raw_folder_path)
    
    # Process schema files and write to the consolidated CSV
    process_schema_files(bucket, folders, output_csv_path)
