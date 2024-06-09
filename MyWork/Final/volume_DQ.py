from google.cloud import storage
import csv

def move_file(source_bucket_name, source_blob_name, destination_bucket_name, destination_blob_name):
    """Moves a blob from one bucket to another."""
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
    source_blob.delete()

def perform_dq_checks(project_id, source_bucket, source_folder_path, destination_bucket, destination_folder_path):
    storage_client = storage.Client(project=project_id)

    # List all files in the source folder
    source_bucket = storage_client.get_bucket(source_bucket)
    blobs = source_bucket.list_blobs(prefix=source_folder_path)

    for blob in blobs:
        print(f"Processing file: {blob.name}")

        # Download the CSV file
        blob_data = blob.download_as_string()
        csv_data = blob_data.decode('utf-8').splitlines()

        # Count the records
        csv_reader = csv.reader(csv_data)
        record_count = sum(1 for row in csv_reader)

        # Determine destination folder path
        if record_count < 100:
            destination_path = destination_folder_path + '/Error/' + blob.name.split('/')[-1]
        else:
            destination_path = destination_folder_path + '/Processed/' + blob.name.split('/')[-1]

        # Move the file to the appropriate folder
        move_file(source_bucket.name, blob.name, destination_bucket, destination_path)
        print(f"File moved to: {destination_path}")

if __name__ == "__main__":
    project_id = "project_id"
    source_bucket = "raw zone bucket"
    source_folder_path = "3rdParty/MFVS/GFV/testing"
    destination_bucket = "certify zone bucket"
    destination_folder_path = "3rdParty/MFVS/GFV/Data_Quality"

    perform_dq_checks(project_id, source_bucket, source_folder_path, destination_bucket, destination_folder_path)
