from google.cloud import storage
import csv

class VolumeCheckAndClassify:
    def __init__(self, raw_zone_bucket_name, error_folder_path, processed_folder_path, certify_zone_bucket_name, certify_folder_path):
        self.raw_zone_bucket_name = raw_zone_bucket_name
        self.error_folder = error_folder_path
        self.processed_folder = processed_folder_path
        self.certify_zone_bucket_name = certify_zone_bucket_name
        self.certify_folder = certify_folder_path
        self.storage_client = storage.Client()

    def process_file(self, file_path):
        print(f"Processing file: {file_path}")

        bucket = self.storage_client.bucket(self.raw_zone_bucket_name)
        blob = bucket.blob(file_path)
        content = blob.download_as_string().decode("utf-8")
        reader = csv.reader(content.splitlines())

        # Count the number of records, ignoring the header row
        record_count = sum(1 for row in reader) # - 1
        filename = file_path.split("/")[-1]

        if record_count < 100:
            print(f"File {filename} has less than 100 records, moving to error folder")
            destination_blob_name = f"{self.error_folder}/{filename}"
            destination_blob = bucket.blob(destination_blob_name)
            destination_blob.upload_from_string(content)
        else:
            print(f"File {filename} has 100 or more records, moving to processed and certify folder")
            processed_blob_name = f"{self.processed_folder}/{filename}"
            certify_blob_name = f"{self.certify_folder}/{filename}"

            # Write to processed folder
            processed_blob = bucket.blob(processed_blob_name)
            processed_blob.upload_from_string(content)

            # Write to certify folder in the certify zone bucket
            certify_bucket = self.storage_client.bucket(self.certify_zone_bucket_name)
            certify_blob = certify_bucket.blob(certify_blob_name)
            certify_blob.upload_from_string(content)

        # Delete the original file
        blob.delete()
        print(f"File {filename} processed and moved accordingly.")

def run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, classified_folder_path, certify_zone_bucket_name, certify_folder_path):
    print("Initializing Google Cloud Storage client")
    storage_client = storage.Client(project_id)
    bucket = storage_client.get_bucket(raw_zone_bucket_name)
    blobs = [blob.name for blob in bucket.list_blobs(prefix=raw_zone_folder_path) if blob.name.endswith('.csv') and '/' not in blob.name[len(raw_zone_folder_path):].strip('/')]

    print(f"Found {len(blobs)} CSV files in raw zone folder: {raw_zone_folder_path}")

    volume_checker = VolumeCheckAndClassify(
        raw_zone_bucket_name=raw_zone_bucket_name,
        error_folder_path=f"{classified_folder_path}/error",
        processed_folder_path=f"{classified_folder_path}/processed",
        certify_zone_bucket_name=certify_zone_bucket_name,
        certify_folder_path=certify_folder_path
    )

    for file_path in blobs:
        volume_checker.process_file(file_path)

    print("Pipeline execution completed")

if __name__ == "__main__":
    # Define the main function and set the appropriate project and bucket details
    project_id = 'your-gcp-project-id'
    raw_zone_bucket_name = 'your-raw-zone-bucket'
    raw_zone_folder_path = 'your-raw-folder-path'
    classified_folder_path = 'your-classified-folder-path'  # Placeholder for classified files (processed, error)
    certify_zone_bucket_name = 'your-certify-zone-bucket'  # Placeholder for processed files in certify zone
    certify_folder_path = 'your-certify-folder-path'  # Placeholder for processed files in certify zone

    # Run the pipeline
    run_pipeline(project_id,raw_zone_bucket_name,raw_zone_folder_path,classified_folder_path,certify_zone_bucket_name,certify_folder_path)
