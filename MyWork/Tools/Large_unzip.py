from google.cloud import storage
import zipfile
import io

def unzip_large_file_in_gcs(raw_zone_bucket_name, raw_zone_folder_path, destination_bucket_name, destination_folder_path, zip_file_name):
    # Initialize the Google Cloud Storage client
    storage_client = storage.Client()

    # Reference the raw zone bucket and the zip file within it
    raw_zone_bucket = storage_client.bucket(raw_zone_bucket_name)
    zip_blob = raw_zone_bucket.blob(f"{raw_zone_folder_path}/{zip_file_name}")

    # Create an in-memory buffer to hold the zip file content
    zip_buffer = io.BytesIO()
    zip_blob.download_to_file(zip_buffer)
    zip_buffer.seek(0)

    # Open the zip file
    with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
        # List all files in the zip file
        zip_file_list = zip_ref.namelist()

        # Reference the destination bucket
        destination_bucket = storage_client.bucket(destination_bucket_name)

        for file_name in zip_file_list:
            # Extract each file to an in-memory buffer
            extracted_file_data = zip_ref.read(file_name)
            extracted_file_buffer = io.BytesIO(extracted_file_data)

            # Create a new blob in the destination bucket
            destination_blob = destination_bucket.blob(f"{destination_folder_path}/{file_name}")

            # Upload the extracted file to the destination bucket
            destination_blob.upload_from_file(extracted_file_buffer, rewind=True)
            print(f"Uploaded {file_name} to {destination_folder_path}/{file_name}")

if __name__ == "__main__":
    raw_zone_bucket_name = 'your-raw-zone-bucket-name'
    raw_zone_folder_path = 'your-raw-zone-folder-path'
    destination_bucket_name = 'your-destination-bucket-name'
    destination_folder_path = 'your-destination-folder-path'
    zip_file_name = 'your-zip-file-name.zip'

    unzip_large_file_in_gcs(raw_zone_bucket_name, raw_zone_folder_path, destination_bucket_name, destination_folder_path, zip_file_name)
