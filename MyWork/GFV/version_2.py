from google.cloud import storage
import zipfile
import io
from datetime import datetime
import pandas as pd
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio
from apache_beam.io.fileio import ReadMatches

def extract_date_from_filename(filename):
    """Extract date from the filename."""
    date_str = filename.split('-')[-1].split('.')[0]
    date_str = date_str[:8]
    if len(date_str) == 8 and date_str.isdigit():
        return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    else:
        return datetime.now().strftime('%Y-%m-%d')

def check_naming_convention(filename, date):
    """Check the naming convention for specific patterns."""
    date_format = date.replace('-', '')
    if filename.startswith('CPRNEW') or filename.startswith('CDENEW'):
        return f"{date_format}-CarsNvpo-csv"
    elif filename.startswith('CPRVAL') or filename.startswith('CDEVAL'):
        return f"{date_format}-BlackBookCodesAndDescriptions-csv"
    elif filename.startswith('LDENEW') or filename.startswith('LPRNEW'):
        return f"{date_format}-LightsNvpo-csv"
    elif filename.startswith('LDEVAL') or filename.startswith('LPRVAL'):
        return f"{date_format}-RedLCVBookCodesAndDescriptions-csv"
    else:
        return None

def zip_and_transfer_csv_files(blob, storage_client, consumer_bucket_name, consumer_folder_path):
    """Zip specified files into a ZIP folder."""
    date = extract_date_from_filename(blob.name)
    naming_convention = check_naming_convention(blob.name, date)
    if naming_convention is not None:
        zip_name = os.path.join(consumer_folder_path, naming_convention).replace("\\", "/")
        zip_buffer = io.BytesIO()
        csv_data = blob.download_as_bytes()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr(os.path.basename(blob.name), csv_data)
        zip_buffer.seek(0)
        consumer_bucket = storage_client.bucket(consumer_bucket_name)
        zip_blob = consumer_bucket.blob(zip_name + ".zip")
        zip_blob.upload_from_file(zip_buffer, content_type='application/zip')
        print(f"Uploaded zip file {zip_name + '.zip'} to {consumer_bucket_name}")
    else:
        print(f"File {blob.name} does not match any naming convention, skipped zipping.")

def move_special_files(blob, storage_client, consumer_bucket_name, consumer_folder_path):
    """Move files starting with 'CPRRVU' or 'CPRRVN' to the consumer folder path."""
    file_name = blob.name.split("/")[-1]
    if file_name.startswith("CPRRVU") or file_name.startswith("CPRRVN"):
        df = pd.read_csv(io.BytesIO(blob.download_as_bytes()), skiprows=1)
        consumer_bucket = storage_client.bucket(consumer_bucket_name)
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
        save_main_session=True,
        temp_location=f'gs://{raw_zone_bucket_name}/temp'
    )

    def process_blob(element):
        storage_client = storage.Client(project=project_id)
        raw_zone_bucket = storage_client.bucket(raw_zone_bucket_name)
        consumer_bucket_name = element['consumer_bucket_name']
        consumer_folder_path = element['consumer_folder_path']
        blob_name = element['blob_name']

        blob = raw_zone_bucket.blob(blob_name)

        if blob_name.startswith("CPRRVU") or blob_name.startswith("CPRRVN"):
            move_special_files(blob, storage_client, consumer_bucket_name, consumer_folder_path)
        else:
            zip_and_transfer_csv_files(blob, storage_client, consumer_bucket_name, consumer_folder_path)

        return f"Processed {blob_name}"

    with beam.Pipeline(options=options) as pipeline:
        _ = (
            pipeline
            | 'List files' >> fileio.MatchFiles(f'gs://{raw_zone_bucket_name}/{raw_zone_folder_path}/**/*.csv')
            | 'Read matches' >> ReadMatches()
            | 'Extract file paths' >> beam.Map(lambda x: {
                'blob_name': x.metadata.path.split(f'gs://{raw_zone_bucket_name}/')[-1],
                'consumer_bucket_name': consumer_bucket_name,
                'consumer_folder_path': consumer_folder_path
            })
            | 'Process files' >> beam.Map(process_blob)
        )

if __name__ == "__main__":
    project_id = 'tnt01-odycda-bld-01-1b81'
    raw_zone_bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    consumer_bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    raw_zone_folder_path = "thParty/GFV/Monthly/SFGDrop"
    consumer_folder_path = 'thParty/GFV/Monthly'

    print("**********Pipeline started**********")
    run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path)
    print("**********Pipeline completed**********")
