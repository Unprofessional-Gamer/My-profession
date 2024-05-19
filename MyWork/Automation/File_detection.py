import time
from datetime import datetime
from google.cloud import storage

def monitor_bucket(bucket_name, folder_path):
    check_number = 1
    print(f"Monitoring folder '{folder_path}' in bucket '{bucket_name}' for new files...")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # Keep track of files seen so far
    seen_files = set()
    
    check_number = 0

    while True:
        
        print(f"\nChecking number: {check_number}")
        print(f"Date and time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        blobs = bucket.list_blobs(prefix=folder_path)
        for blob in blobs:
            if blob.name not in seen_files:
                print(f"New file detected: {blob.name}")
                # Handle the new file here, e.g., process it, move it, etc.
                seen_files.add(blob.name)
        check_number += 1
        # Sleep for a certain interval before checking again
        time.sleep(60)  # Poll every 60 seconds

def main():
    # Replace 'raw-zone-bucket' and 'folder/path' with your bucket name and folder path
    monitor_bucket('raw-zone-bucket', 'folder/path')

if __name__ == "__main__":
    main()
