from google.cloud import storage
import zipfile
import io
from datetime import datetime
import pandas as pd
import os

def extract_date_from_filename(self, filename):
    """Extract date from the filename."""
    date_str = filename.split('-')[-1].split('.')[0]
    date_str = date_str[:8]
    if len(date_str) == 8 and date_str.isdigit():
        return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    else:
        return datetime.now().strftime('%Y-%m-%d')

def zip_and_transfer_csv_files(storage_clinet, raw_zone_bucket, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path):
    """Zip specified files into a ZIP folder."""
    
    blob_list = list(raw_zone_bucket.list_blobs(prefix=raw_zone_folder_path))

    if not blob_list:
        print(f"No files found in {raw_zone_folder_path}.")
        return
    
    try:
        # creaet memory zip
        zip_buffer = {}
        i=0
        for blob in blob_list:
            if blob.name.endswith('.csv'):
                i = i+1
            date = extract_date_from_filename(blob.name)
            folder_name = date[5:8]+date[:4]
            naming_convention = check_naming_convention(blob.name,date)
            file_name = blob.name.split('/')[-1]
            if naming_convention is not None:
                folder_name = os.path.join(consumer_folder_path,folder_name).replace("\\","/")
                folder_name = os.path.join(folder_name, naming_convention).replace("\\","/")
                if folder_name not in zip_buffer:
                    zip_buffer[folder_name] = io.BytesIO()
                csv_data = blob.download_as_bytes()
                with zipfile.ZipFile(zip_buffer[folder_name], 'a', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.writestr(os.path.basename(blob.name), csv_data)

            else:
                folder_name = os.path.join(consumer_folder_path, folder_name).replace("\\","/")
                df1=pd.read_csv(f"gs://tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a/{raw_zone_folder_path}/{file_name}",skiprows=1)
                consumer_bucket = storage_clinet.bucket(consumer_bucket_name)
                consumer_bucket.blob(f"{folder_name}/{file_name}").upload_from_string(df1.to_csv(index=False), 'text/csv')
                print(f"moved {file_name} as a csv file {folder_name}/{file_name}")

        for folder_name, zip_buffer in zip_buffer.items():
            zip_buffer.seek(0)
            consumer_bucket = storage_clinet.bucket(consumer_bucket_name)
            zip_blob = consumer_bucket.blob(os.path.join(folder_name + ".zip"))
            print(f"zipped blob is : {zip_blob}")
            zip_blob.upload_from_file(zip_buffer, content_type='application/zip')
        print(f"uploaded {zip_blob.name} to {consumer_bucket_name}")
    except Exception as k:
            print(f"Error in {file_name} : {k}")

def check_naming_convention(filename,date):
    date_format = date[:4]+date[5:7]+date[8:]
    if 'CPRNEW' in filename or 'CDENEW' in filename:
        return f"{date_format}-CarsNvpo-csv"
    elif 'CPRVAL' in filename or 'CDEVAL' in filename:
        return f"{date_format}-BlackBookCodesAndDescription-csv"
    else:
        return None
    
def copy_and_transfer_csv(raw_zone_bucket, raw_zone_csv_path, consumer_bucket, consumer_folder_path):
    blob_list = list(raw_zone_bucket.list_blobs(prefix=raw_zone_csv_path))
    for blob in blob_list:
        if blob.name.endswith(".csv"):
            file_name = blob.name.split("/")[-1]
            date=extract_date_from_filename(file_name)
            folder_date = date[5:8]+date[:4]
            print(folder_date)
            df1=pd.read_csv(f"gs://tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a/{raw_zone_csv_path}/{file_name}",skiprows=1)
            consumer_bucket.blob(f"{consumer_folder_path}/{folder_date}/{file_name}").upload_from_string(df1.to_csv(index=False), 'text/csv')


if __name__ == "__main__":

    # Define Google Cloud Storage bucket names
    project_id = 'tnt01-odycda-bld-01-c08e'
    raw_zone_bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    consumer_bucket_name = "tnt1092gisnnd872391a"

    sfg_base_path = "thParty/GFV/Monthly/SFGDrop" # ----------> SFG file drop
    staging_folder_path = "thparty/thParty/GFV/Monthly/Dummy_data" # Raw zone file staging

    storage_client = storage.Client(project=project_id)
    raw_zone_bucket = storage_client.bucket(raw_zone_bucket_name)

    consumer_folder_path = 'thParty/GFV/Monthly/'
    raw_zone_zip_path = f"{sfg_base_path}" # files to be zipped from raw zone

    consumer_bucket = storage_client.bucket(consumer_bucket_name)
    print("******************************Zipping Started*********************")
    zip_and_transfer_csv_files(storage_client, raw_zone_bucket, raw_zone_zip_path, consumer_bucket_name, consumer_folder_path)
    print("**********************ZIpping Completed********************************")
    print("*********** CSV Files Loaded Successfully ****************")

