from google.cloud import storage
import py7zr
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

    # Open the zip file using py7zr
    with py7zr.SevenZipFile(zip_buffer, 'r') as archive:
        # Extract all files in the zip file to a dictionary
        extracted_files = archive.readall()

        # Reference the destination bucket
        destination_bucket = storage_client.bucket(destination_bucket_name)

        for file_name, file_data in extracted_files.items():
            # Create a new blob in the destination bucket
            destination_blob = destination_bucket.blob(f"{destination_folder_path}/{file_name}")

            # Upload the extracted file to the destination bucket
            destination_blob.upload_from_file(io.BytesIO(file_data.read()), rewind=True)
            print(f"Uploaded {file_name} to {destination_folder_path}/{file_name}")

if __name__ == "__main__":
    raw_zone_bucket_name = 'your-raw-zone-bucket-name'
    raw_zone_folder_path = 'your-raw-zone-folder-path'
    destination_bucket_name = 'your-destination-bucket-name'
    destination_folder_path = 'your-destination-folder-path'
    zip_file_name = 'your-zip-file-name.zip'

    unzip_large_file_in_gcs(raw_zone_bucket_name, raw_zone_folder_path, destination_bucket_name, destination_folder_path, zip_file_name)
