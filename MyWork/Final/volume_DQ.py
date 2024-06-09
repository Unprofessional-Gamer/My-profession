from google.cloud import storage
import csv

def move_file(source_bucket_name, source_blob_name, destination_bucket_name, destination_blob_name):
    """Moves a blob from one bucket to another."""
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)

def get_approximate_record_count(blob_metadata):
    """Estimates the record count based on blob size."""
    # Assuming 1000 bytes per record
    approx_record_count = int(blob_metadata['size']) // 1000
    return approx_record_count

def perform_dq_checks(project_id, source_bucket, source_folder_path, destination_bucket, destination_folder_path):
    storage_client = storage.Client(project=project_id)

    # List all files in the source folder
    source_bucket = storage_client.get_bucket(source_bucket)
    blobs = source_bucket.list_blobs(prefix=source_folder_path)

    for blob in blobs:
        print(f"Processing file: {blob.name}")

        # Get the blob metadata to get the size
        blob_metadata = blob.metadata

        # Calculate approximate record count based on blob size
        approx_record_count = get_approximate_record_count(blob_metadata)

        print(f"Approximate record count: {approx_record_count}")

        # Determine destination folder path
        if approx_record_count < 100:
            destination_path = destination_folder_path + '/Error/' + blob.name.split('/')[-1]
        else:
            destination_path = destination_folder_path + '/Processed/' + blob.name.split('/')[-1]

        # Move the file to the appropriate folder
        move_file(source_bucket.name, blob.name, destination_bucket, destination_path)
        print(f"File moved to: {destination_path}")

if __name__ == "__main__":
    project_id = "project_id"
    source_bucket = "raw zone bucket"
    source_folder_path = "thParty/MFVS/GFV/testing"
    destination_bucket = "certify zone bucket"
    destination_folder_path = "thParty/MFVS/GFV/Data_Quality"

    perform_dq_checks(project_id, source_bucket, source_folder_path, destination_bucket, destination_folder_path)
