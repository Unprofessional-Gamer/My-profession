from google.cloud import storage
import os
import zipfile
import io
import pandas as pd
from datetime import datetime

def extract_date_from_filename(filename):
    """Extract date from the filename."""
    date_str = filename.split('-')[-1]
    date_str = date_str[:8]
    if len(date_str) == 8 and date_str.isdigit():
        return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    else:
        return datetime.now().strftime('%Y-%m-%d')

def zip_and_transfer_csv_files(storage_client,raw_zone_bucket,raw_zone_folder_path,consumer_bucket_name, consumer_folder_path):
    blob_list = list(raw_zone_bucket.list_blob(prefix=raw_zone_folder_path))

    if not blob_list:
        print("No CSV files found in the raw zone bucket.")
        return


    try:
        zip_buffer = {}
        for blob in blob_list:
            if blob.name.endswith('.csv'):
                date = extract_date_from_filename(blob.name)
                folder_name = date[5:8]+date[:4]
                naming_convention = check_naming_conventions(blob.name,date)
                if naming_convention is not None:
                    folder_name = os.path.join(consumer_folder_path, folder_name).replace('\\','/')
                    folder_name = os.path.join(folder_name, naming_convention).replace('\\','/')
                else:
                    folder_name = os.path.join(folder_name, 'GFV')
                if folder_name not in zip_buffer:
                    zip_buffer[folder_name] = io.BytesIO()

                # Download CSV file as bytes
                csv_data = blob.download_as_bytes()

                # Write CSV bytes to the in-memory zip buffer
                with zipfile.ZipFile(zip_buffer[folder_name], 'a', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.writestr(os.path.basename(blob.name), csv_data)

        for folder_name, zip_buffer in zip_buffer.items():
            zip_buffer.seek(0)
            consumer_bucket = storage_client.bucket(consumer_bucket_name)
            zip_blob = consumer_bucket.blob(os.path.join(folder_name + ".zip"))
            zip_blob.upload_from_file(zip_buffer, content_type='application/zip')

        print(f"Files organized and transferred successfully.")

    except Exception as k:
        print(f"An error occurred: {k}")

def check_naming_conventions(filename):
    """Check if the filename matches the naming conventions."""
    if 'CPRNEW' in filename or 'CDENEW' in filename:
        return 'CarsNvpo'
    elif 'CPRVAL' in filename or 'CDEVAL' in filename:
        return 'BlackBookCodesAndDescriptions'
    else:
        return None
    
def copy_and_transfer(raw_zone_bucket,raw_zone_csv_path,consumer_bucket,consumer_folder_path):
    blob_list = list(raw_zone_bucket.list_blobs(prefix=raw_zone_csv_path))
    for blob in blob_list:
        file_name = blob.name.spilt('/')[-1]
        date = extract_date_from_filename(file_name)
        folder_date = date[5:8]+date[:4]
        df1= pd.read_csv(f"gs://rawzone_bucket_address/{raw_zone_csv_path}/{file_name}",skiprows=1)
        consumer_bucket.blob(f"{consumer_folder_path}/{folder_date}/{file_name}").upload_from_string(df1.to_csv(index=False), 'text/csv')


if __name__ == "__main__":
    project_id = 'your-project-id'
    raw_zone_bucket_name = 'raw-zone-bucket-name'
    consumer_bucket_name = 'consumer-bucket-name'

    source_base_path = 'thParty/MFVS/GFV/monthly'
    raw_zone_folder_path = 'thParty/MFVS/GFV/monthly'
    raw_zone_zip_path = f"{source_base_path}/Raw_zips"
    raw_zone_csv_path = f"{source_base_path}/csv_files"
    consumer_folder_path = 'thParty/MFVS/GFV/monthly/Ziptest'


    # Initialize GCS clients
    storage_client = storage.Client(project=project_id)

    # List CSV files in the raw zone bucket
    raw_zone_bucket = storage_client.bucket(raw_zone_bucket_name)
    consumer_bucket = storage_client.bucket(consumer_bucket_name)
    print("Start Zipping")

    zip_and_transfer_csv_files(consumer_bucket_name,raw_zone_bucket, raw_zone_zip_path, storage_client,consumer_folder_path)
    print("Zipping completed ---------------------- Starting to move csv files")
    copy_and_transfer(consumer_bucket_name,raw_zone_bucket, raw_zone_csv_path,consumer_folder_path)
    print("files Loaded")
