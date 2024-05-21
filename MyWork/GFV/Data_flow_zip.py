from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import zipfile
import io
from datetime import datetime
import pandas as pd
import os

MONTH_MAPPING = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec"
}

def extract_date_from_filename(filename):
    """Extract date from the filename."""
    date_str = filename.split('-')[-1].split('.')[0]
    date_str = date_str[:8]
    if len(date_str) == 8 and date_str.isdigit():
        return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    else:
        return datetime.now().strftime('%Y-%m-%d')

def check_naming_convention(filename, date):
    date_format = date[:4]+date[5:7]+date[8:]
    if 'CPRNEW' in filename or 'CDENEW' in filename:
        return f"{date_format}-CarsNvpo-csv"
    elif 'CPRVAL' in filename or 'CDEVAL' in filename:
        return f"{date_format}-BlackBookCodesAndDescription-csv"
    else:
        return None

def process_and_upload(element, consumer_bucket_name, consumer_folder_path):
    from google.cloud import storage

    storage_client = storage.Client()
    raw_zone_bucket_name, file_name = element.split(',', 1)
    raw_zone_bucket = storage_client.bucket(raw_zone_bucket_name)
    blob = raw_zone_bucket.blob(file_name)
    
    date = extract_date_from_filename(file_name)
    month_folder = MONTH_MAPPING[date[5:7]]
    folder_name = f"{date[:4]}/{month_folder}"
    naming_convention = check_naming_convention(file_name, date)

    if naming_convention:
        folder_name = os.path.join(consumer_folder_path, folder_name).replace("\\", "/")
        folder_name = os.path.join(folder_name, naming_convention).replace("\\", "/")
        zip_buffer = io.BytesIO()
        csv_data = blob.download_as_bytes()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr(os.path.basename(file_name), csv_data)
        zip_buffer.seek(0)
        consumer_bucket = storage_client.bucket(consumer_bucket_name)
        zip_blob = consumer_bucket.blob(os.path.join(folder_name + ".zip"))
        zip_blob.upload_from_file(zip_buffer, content_type='application/zip')
        print(f"Uploaded {zip_blob.name} to {consumer_bucket_name}")
    else:
        folder_name = os.path.join(consumer_folder_path, folder_name).replace("\\", "/")
        df = pd.read_csv(io.BytesIO(blob.download_as_bytes()), skiprows=1)
        consumer_bucket = storage_client.bucket(consumer_bucket_name)
        consumer_bucket.blob(f"{folder_name}/{file_name}").upload_from_string(df.to_csv(index=False), 'text/csv')
        print(f"Moved {file_name} as a CSV file to {folder_name}/{file_name}")

def run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path):
    options = PipelineOptions(
        project=project_id,
        runner="DataflowRunner",
        temp_location=f'gs://{raw_zone_bucket_name}/temp',
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
    
    p = beam.Pipeline(options=options)

    raw_zone_bucket = storage.Client().bucket(raw_zone_bucket_name)
    blob_list = list(raw_zone_bucket.list_blobs(prefix=raw_zone_folder_path))

    file_list = [f"{raw_zone_bucket_name},{blob.name}" for blob in blob_list if blob.name.endswith('.csv')]

    (p
     | "CreateFileList" >> beam.Create(file_list)
     | "ProcessAndUpload" >> beam.Map(process_and_upload, consumer_bucket_name=consumer_bucket_name, consumer_folder_path=consumer_folder_path)
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    project_id = 'tnt01-odycda-bld-01-1b81'
    raw_zone_bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    consumer_bucket_name = "tnt1092gisnnd872391a"
    
    sfg_base_path = "thParty/GFV/Monthly/SFGDrop"
    consumer_folder_path = 'thParty/GFV/Monthly/'

    print("******************************Zipping Started*********************")
    run_pipeline(project_id, raw_zone_bucket_name, sfg_base_path, consumer_bucket_name, consumer_folder_path)
    print("**********************Zipping Completed********************************")
