import time
import datetime
import subprocess
from google.cloud import storage

def monitor_bucket(bucket_name, folder_path):
    checking_number = 1  # Initialize checking number
    print(f"Monitoring folder '{folder_path}' in bucket '{bucket_name}' for new files...")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # Keep track of files seen so far
    seen_files = set()

    while True:
        print(f"Checking #{checking_number} - {datetime.datetime.now()}")  # Print checking number and current date/time
        blobs = bucket.list_blobs(prefix=folder_path)
        for blob in blobs:
            if blob.name not in seen_files:
                print(f"New file detected: {blob.name}")
                # Run data quality checks
                subprocess.run(["python", "data_quality_checks.py", blob.name])
                # Load data into BigQuery
                subprocess.run(["python", "bq_loader.py", blob.name])
                # Handle the new file here, e.g., process it, move it, etc.
                seen_files.add(blob.name)

        # Increment checking number
        checking_number += 1
        
        # Sleep for a certain interval before checking again
        time.sleep(60)  # Poll every 60 seconds

def main():
    # Replace 'raw-zone-bucket' and 'folder/path' with your bucket name and folder path
    monitor_bucket('raw-zone-bucket', 'folder/path')

if __name__ == "__main__":
    main()
