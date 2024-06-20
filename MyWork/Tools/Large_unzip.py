from google.cloud import storage
import patoolib
import io
import os

def unzip_large_file_in_gcs(raw_zone_bucket_name, raw_zone_folder_path, destination_bucket_name, destination_folder_path, zip_file_name):
    # Initialize the Google Cloud Storage client
    storage_client = storage.Client()

    # Reference the raw zone bucket and the zip file within it
    raw_zone_bucket = storage_client.bucket(raw_zone_bucket_name)
    zip_blob = raw_zone_bucket.blob(f"{raw_zone_folder_path}/{zip_file_name}")

    # Create a temporary directory to store the zip file and extract it
    temp_dir = "/tmp/extracted_files"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # Download the zip file content into a local file
    zip_file_path = os.path.join(temp_dir, zip_file_name)
    zip_blob.download_to_filename(zip_file_path)

    # Extract the zip file using patoolib
    patoolib.extract_archive(zip_file_path, outdir=temp_dir)

    # Reference the destination bucket
    destination_bucket = storage_client.bucket(destination_bucket_name)

    # Upload extracted files to the destination bucket
    for root, dirs, files in os.walk(temp_dir):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            relative_path = os.path.relpath(file_path, temp_dir)
            destination_blob = destination_bucket.blob(f"{destination_folder_path}/{relative_path}")

            with open(file_path, 'rb') as file_data:
                destination_blob.upload_from_file(file_data, rewind=True)
            print(f"Uploaded {relative_path} to {destination_folder_path}/{relative_path}")

    # Clean up temporary files
    os.remove(zip_file_path)
    for root, dirs, files in os.walk(temp_dir):
        for file_name in files:
            os.remove(os.path.join(root, file_name))
        for dir_name in dirs:
            os.rmdir(os.path.join(root, dir_name))
    os.rmdir(temp_dir)

if __name__ == "__main__":
    raw_zone_bucket_name = 'your-raw-zone-bucket-name'
    raw_zone_folder_path = 'your-raw-zone-folder-path'
    destination_bucket_name = 'your-destination-bucket-name'
    destination_folder_path = 'your-destination-folder-path'
    zip_file_name = 'your-zip-file-name.zip'

    unzip_large_file_in_gcs(raw_zone_bucket_name, raw_zone_folder_path, destination_bucket_name, destination_folder_path, zip_file_name)
