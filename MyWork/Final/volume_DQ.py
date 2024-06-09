from google.cloud import storage
import io
import csv

def count_records(csv_blob):
    """Count the total number of records in a CSV file."""
    blob_content = csv_blob.download_as_string()
    csv_data = io.StringIO(blob_content.decode('utf-8'))
    csv_reader = csv.reader(csv_data)
    record_count = sum(1 for row in csv_reader)
    print(f"File {csv_blob.name} has {record_count} records.")
    return record_count

def move_file(source_blob, destination_bucket, destination_folder):
    """Move a file to the specified destination bucket and folder."""
    destination_blob_name = destination_folder + "/" + source_blob.name.split("/")[-1]
    destination_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
    source_blob.delete()
    print(f"File {source_blob.name} moved to {destination_folder}.")

def main(project_id, source_bucket_name, source_folder_path, destination_bucket_name, destination_folder_path):
    """Main function to perform data quality checks and move files."""
    storage_client = storage.Client(project=project_id)
    source_bucket = storage_client.get_bucket(source_bucket_name)
    destination_bucket = storage_client.get_bucket(destination_bucket_name)

    for blob in source_bucket.list_blobs(prefix=source_folder_path):
        if blob.name.endswith('.csv'):
            record_count = count_records(blob)
            if record_count < 100:
                move_file(blob, destination_bucket, destination_folder_path + "/Error")
            else:
                move_file(blob, destination_bucket, destination_folder_path + "/Processed")

if __name__ == "__main__":
    project_id = "project_id"
    source_bucket_name = "raw zone bucket"
    source_folder_path = "3rdParty/MFVS/GFV/testing"
    destination_bucket_name = "certify zone bucket"
    destination_folder_path = "3rdParty/MFVS/GFV/Data_Quality"

    main(project_id, source_bucket_name, source_folder_path, destination_bucket_name, destination_folder_path)
