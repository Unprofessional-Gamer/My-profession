from google.cloud import storage
import zipfile
import io
from datetime import datetime
import os

gfv_files = {"CPRRVU", "CPRRVN", "LPRRVN", "LPRRVU"}

def extract_date_from_filename(filename):
    """Extract date from the filename."""
    date_str = filename.split('-')[-1].split('.')[0]
    date_str = date_str[:8]
    if len(date_str) == 8 and date_str.isdigit():
        return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    else:
        return datetime.now().strftime('%Y-%m-%d')

def zip_and_transfer_csv_files(project_id, raw_zone_bucket_name, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path, consumer_archieve_path):
    """Zip specified files into a ZIP folder and transfer to multiple locations."""
    
    storage_client = storage.Client(project=project_id)
    raw_zone_bucket = storage_client.bucket(raw_zone_bucket_name)
    consumer_bucket = storage_client.bucket(consumer_bucket_name)

    blob_list = list(raw_zone_bucket.list_blobs(prefix=raw_zone_folder_path))

    if not blob_list:
        print(f"No files found in {raw_zone_folder_path}.")
        return
    
    try:
        # Create memory zip
        zip_buffer = {}
        for blob in blob_list:
            if blob.name.endswith('.csv'):
                date = extract_date_from_filename(blob.name)
                naming_convention = check_naming_convention(blob.name, date)
                file_name = blob.name.split('/')[-1]
                if naming_convention is not None:
                    zip_name = os.path.join(consumer_folder_path, naming_convention).replace("\\", "/")
                    if zip_name not in zip_buffer:
                        zip_buffer[zip_name] = io.BytesIO()
                    csv_data = blob.download_as_bytes()
                    with zipfile.ZipFile(zip_buffer[zip_name], 'a', zipfile.ZIP_DEFLATED) as zipf:
                        zipf.writestr(os.path.basename(blob.name), csv_data)

        for zip_name, buffer in zip_buffer.items():
            buffer.seek(0)
            
            # Upload to consumer_folder_path
            zip_blob = consumer_bucket.blob(zip_name + ".zip")
            zip_blob.upload_from_file(buffer, content_type='application/zip')
            print(f"Uploaded to {zip_name + '.zip'}")
            
            buffer.seek(0)  # Reset the buffer to the beginning
            
            # Upload to consumer_archieve_path
            archive_zip_name = zip_name.replace(consumer_folder_path, consumer_archieve_path)
            archive_blob = consumer_bucket.blob(archive_zip_name + ".zip")
            archive_blob.upload_from_file(buffer, content_type='application/zip')
            print(f"Uploaded to {archive_zip_name + '.zip'}")
        
        print(f"Uploaded files to {consumer_bucket_name} successfully.")

    except Exception as e:
        print(f"Error: {e}")

def check_naming_convention(filename, date):
    date_format = date.replace('-', '')
    if 'CPRNEW' in filename or 'CDENEW' in filename:
        return f"{date_format}-CarsNvpo-csv"
    elif 'CPRVAL' in filename or 'CDEVAL' in filename:
        return f"{date_format}-BlackBookCodesAndDescriptions-csv"
    elif 'LDENEW' in filename or 'LPRNEW' in filename:
        return f"{date_format}-LightsNvpo-csv"
    elif 'LDEVAL' in filename or 'LPRVAL' in filename:
        return f"{date_format}-RedLCVBookCodesAndDescriptions-csv"
    else:
        return None

def copy_and_transfer_csv(project_id, raw_zone_bucket_name, certify_files_path, consumer_bucket_name, consumer_folder_path, consumer_archieve_path, gfv_files):
    storage_client = storage.Client(project=project_id)
    raw_zone_bucket = storage_client.bucket(raw_zone_bucket_name)
    consumer_bucket = storage_client.bucket(consumer_bucket_name)

    blob_list = list(raw_zone_bucket.list_blobs(prefix=certify_files_path))
    for blob in blob_list:
        file_name = blob.name.split("/")[-1]
        if any(file_name.startswith(prefix) for prefix in gfv_files):
            print(f"Processing file: {file_name}")
            
            # Copy to consumer_folder_path
            consumer_bucket.blob(f"{consumer_folder_path}/{file_name}").upload_from_string(blob.download_as_bytes(), 'text/csv')
            
            # Copy to consumer_archieve_path
            consumer_bucket.blob(f"{consumer_archieve_path}/{file_name}").upload_from_string(blob.download_as_bytes(), 'text/csv')
            
            print(f"Moved {file_name} to {consumer_folder_path}/{file_name} and {consumer_archieve_path}/{file_name}")
        else:
            print(f"Ignored file: {file_name}")

def delete_files(project_id, raw_zone_bucket_name, certify_files_path):
    """Delete files from certify_files_path."""
    storage_client = storage.Client(project=project_id)
    raw_zone_bucket = storage_client.bucket(raw_zone_bucket_name)

    blob_list = list(raw_zone_bucket.list_blobs(prefix=certify_files_path))

    for blob in blob_list:
        print(f"Deleting file: {blob.name}")
        blob.delete()

    print(f"Deleted {len(blob_list)} files from {certify_files_path}")

if __name__ == "__main__":
    
    project_id = 'tnt01-odycda-bld-01-1b81'
    raw_zone_bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    consumer_bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"

    certify_files_path = "thParty/GFV/Monthly/SFGDrop"
    consumer_folder_path = 'thParty/GFV/Monthly'
    current_folder = datetime.now().strftime('%m-%Y')
    consumer_archieve_path = f'thParty/GFV/Monthly/{current_folder}/ARCHIEVE'

    print("**********CSV file copying started**********")
    copy_and_transfer_csv(project_id, raw_zone_bucket_name, certify_files_path, consumer_bucket_name, consumer_folder_path, consumer_archieve_path, gfv_files)
    print("**********CSV file copying completed**********")

    print("**********Files zipping started**********")
    zip_and_transfer_csv_files(project_id, raw_zone_bucket_name, certify_files_path, consumer_bucket_name, consumer_folder_path, consumer_archieve_path)
    print("**********Files zipping completed**********")

    print("**********Deleting files started**********")
    delete_files(project_id, raw_zone_bucket_name, certify_files_path)
    print("**********Deleting files completed**********")
