from google.cloud import storage
import zipfile
import io
from datetime import datetime
import pandas as pd
import os
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.fileio import ReadMatches

def extract_date_from_filename(filename):
    date_str = filename.split('-')[-1].split('.')[0]
    date_str = date_str[:8]
    if len(date_str) == 8 and date_str.isdigit():
        return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    else:
        return datetime.now().strftime('%Y-%m-%d')

def move_files(storage_client, raw_zone_bucket, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path):
    blob_list = list(raw_zone_bucket.list_blobs(prefix=raw_zone_folder_path))
    if not blob_list:
        print(f"No files found in {raw_zone_folder_path}.")
        return
    
    try:
        for blob in blob_list:
            file_name = blob.name.split('/')[-1]
            if file_name.startswith(('CPRRVU', 'CPRRVN')):
                consumer_bucket = storage_client.bucket(consumer_bucket_name)
                consumer_bucket.blob(f"{consumer_folder_path}/{file_name}").upload_from_string(blob.download_as_bytes())
                print(f"Moved {file_name} as is to {consumer_folder_path}/{file_name}")
            else:
                handle_zip_and_transfer(blob, consumer_bucket_name, consumer_folder_path)
        print(f"Uploaded files to {consumer_bucket_name} successfully.")
    except Exception as e:
        print(f"Error: {e}")

def handle_zip_and_transfer(blob, consumer_bucket_name, consumer_folder_path):
    date = extract_date_from_filename(blob.name)
    naming_convention = check_naming_convention(blob.name, date)
    if naming_convention is not None:
        zip_name = os.path.join(consumer_folder_path, naming_convention).replace("\\", "/")
        zip_buffer = io.BytesIO()
        csv_data = blob.download_as_bytes()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr(os.path.basename(blob.name), csv_data)
        zip_buffer.seek(0)
        consumer_bucket = storage.Client().bucket(consumer_bucket_name)
        zip_blob = consumer_bucket.blob(zip_name + ".zip")
        zip_blob.upload_from_file(zip_buffer, content_type='application/zip')
    else:
        df = pd.read_csv(f"gs://{blob.bucket.name}/{blob.name}", skiprows=1)
        consumer_bucket = storage.Client().bucket(consumer_bucket_name)
        consumer_bucket.blob(f"{consumer_folder_path}/{blob.name.split('/')[-1]}").upload_from_string(df.to_csv(index=False), 'text/csv')
        print(f"Moved {blob.name} as a CSV file to {consumer_folder_path}/{blob.name.split('/')[-1]}")

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

def copy_and_transfer_csv(raw_zone_bucket, raw_zone_csv_path, consumer_bucket, consumer_folder_path):
    blob_list = list(raw_zone_bucket.list_blobs(prefix=raw_zone_csv_path))
    for blob in blob_list:
        file_name = blob.name.split("/")[-1]
        if "GFV" in file_name:
            date = extract_date_from_filename(file_name)
            df = pd.read_csv(f"gs://{raw_zone_bucket.name}/{raw_zone_csv_path}/{file_name}", skiprows=1)
            consumer_bucket.blob(f"{consumer_folder_path}/{file_name}").upload_from_string(df.to_csv(index=False), 'text/csv')
            print(f"Moved {file_name} as a CSV file to {consumer_folder_path}/{file_name}")

def run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path):
    options = PipelineOptions(
        project=project_id,
        runner="DataflowRunner",
        region='europe-west2',
        staging_location=f'gs://{raw_zone_bucket_name}/staging',
        service_account_email='svc-dfl-user@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',
        dataflow_kms_key='projects/tnt01-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptoKeys/keyhsm-kms-tnt01-euwe2-cdp',
        subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88h/regions/europe-west2/subnetworks/odycda-csn-euwe2-kc1-01-bld-01',
        num_workers=1,
        max_num_workers=4,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )

    def process_blob(blob_name):
        storage_client = storage.Client(project=project_id)
        raw_zone_bucket = storage_client.bucket(raw_zone_bucket_name)
        consumer_bucket = storage_client.bucket(consumer_bucket_name)
        
        blob = raw_zone_bucket.blob(blob_name)
        file_name = blob_name.split('/')[-1]
        
        if file_name.startswith(('CPRRVU', 'CPRRVN')):
            consumer_bucket.blob(f"{consumer_folder_path}/{file_name}").upload_from_string(blob.download_as_bytes())
            return f"Moved {blob_name} as is"
        else:
            date = extract_date_from_filename(blob_name)
            naming_convention = check_naming_convention(blob_name, date)
            if naming_convention is not None:
                zip_name = os.path.join(consumer_folder_path, naming_convention).replace("\\", "/")
                zip_buffer = io.BytesIO()
                csv_data = blob.download_as_bytes()
                with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.writestr(file_name, csv_data)
                zip_buffer.seek(0)
                zip_blob = consumer_bucket.blob(zip_name + ".zip")
                zip_blob.upload_from_file(zip_buffer, content_type='application/zip')
            else:
                df = pd.read_csv(f"gs://{raw_zone_bucket_name}/{blob_name}", skiprows=1)
                consumer_bucket.blob(f"{consumer_folder_path}/{file_name}").upload_from_string(df.to_csv(index=False), 'text/csv')
            return f"Processed {blob_name}"

    with beam.Pipeline(options=options) as pipeline:
        files = (
            pipeline
            | 'List files' >> fileio.MatchFiles(f'gs://{raw_zone_bucket_name}/{raw_zone_folder_path}/**/*.csv')
            | 'Read matches' >> ReadMatches()
            | 'Extract file paths' >> beam.Map(lambda x: x.metadata.path)
            | 'Process files' >> beam.Map(process_blob)
        )

if __name__ == "__main__":
    
    project_id = 'tnt01-odycda-bld-01-1b81'
    raw_zone_bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    consumer_bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"

    sfg_base_path = "thParty/GFV/Monthly/SFGDrop" 
    staging_folder_path = "thparty/thParty/GFV/Monthly/Dummy_data" 

    consumer_folder_path = 'thParty/GFV/Monthly'
    raw_zone_zip_path = f"{sfg_base_path}" 
    raw_zone_csv_path = f"{staging_folder_path}/GFV_files"

    storage_client = storage.Client(project=project_id)
    raw_zone_bucket = storage_client.bucket(raw_zone_bucket_name)
    consumer_bucket = storage_client.bucket(consumer_bucket_name)

    print("**********Files moving started**********")
    move_files(storage_client, raw_zone_bucket, raw_zone_zip_path, consumer_bucket_name, consumer_folder_path)
    print("**********Files moving completed**********")

    run_pipeline(project_id, raw_zone_bucket_name, raw_zone_zip_path, consumer_bucket_name, consumer_folder_path)
