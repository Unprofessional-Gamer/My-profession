from io import StringIO
from google.cloud import storage
import pandas as pd
import os

# Set up GCS client
def list_files_in_folder(bucket_name, folder_path):
    print(f"Listing all files in folder: {folder_path}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
    print(f"Found {len(files)} CSV files.")
    return files

def read_csv_file_from_gcs(bucket_name, file_path):
    print(f"Reading file from GCS: {file_path}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()
    return pd.read_csv(StringIO(content))

def consolidate_error_files(bucket_name, error_folder_path, raw_zone_folder_path, output_file_name):
    print("Starting the error consolidation process...")
    files = list_files_in_folder(bucket_name, error_folder_path)
    
    if not files:
        print("No CSV files found in the specified folder.")
        return

    error_data = {}
    
    for file in files:
        try:
            df = read_csv_file_from_gcs(bucket_name, file)
            # Check if the DataFrame is empty
            if df.empty:
                print(f"Warning: {file} is empty. Skipping this file.")
                continue
            # Get the last column name (assuming it's the error column)
            error_column = df.columns[-1]
            print(f"Processing errors in column: {error_column} from file: {file}")
            
            # Count occurrences of each error type
            errors = df[error_column].value_counts().to_dict()
            error_data[os.path.basename(file)] = errors
        except pd.errors.EmptyDataError:
            print(f"Error: {file} is empty or malformed. Skipping this file.")
            continue
    
    if not error_data:
        print("No valid error data found in the specified files.")
        return
    
    # Create a consolidated DataFrame
    consolidated_df = pd.DataFrame(error_data).fillna(0)
    consolidated_df.index.name = 'Error'
    
    # Convert the consolidated DataFrame to a CSV string
    output_csv_content = consolidated_df.to_csv()
    print("Consolidated DataFrame created. Saving to GCS...")

    # Upload the consolidated CSV file back to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    raw_zone_output_path = os.path.join(raw_zone_folder_path, output_file_name)
    blob = bucket.blob(raw_zone_output_path)
    blob.upload_from_string(output_csv_content, content_type='text/csv')
    print(f"Consolidated error report saved to {raw_zone_output_path}")

if __name__ == "__main__":
    # Define your bucket name and folder paths
    bucket_name = 'your_bucket_name'
    error_folder_path = 'certify_zone/error/'  # Adjust this to your actual error folder path
    raw_zone_folder_path = 'raw_zone/'  # Adjust this to your actual raw zone folder path
    output_file_name = 'consolidated_error_report.csv'

    # Run the consolidation function
    consolidate_error_files(bucket_name, error_folder_path, raw_zone_folder_path, output_file_name)
