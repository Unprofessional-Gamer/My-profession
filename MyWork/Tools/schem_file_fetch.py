import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def process_schema_files(element):
    bucket_name = element['bucket']
    blob_name = element['name']
    folder_name = blob_name.split('/')[0].split('-')[1]
    file_name_prefix = blob_name.split('/')[-1].replace('.schema.csv', '')

    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_text()
    lines = content.strip().split('\n')

    output_rows = []
    for line in lines:
        output_rows.append({'folder_name': folder_name, 'file_name_prefix': file_name_prefix, 'schema_data': line})

    return output_rows

def run_pipeline(project_id, raw_zone_bucket, raw_zone_folder_path, output_file_path):
    pipeline_options = PipelineOptions(
        project=project_id,
        runner="DataflowRunner",
        region='europe-west2',
        staging_location=f'gs://{raw_zone_bucket}/staging',
        temp_location=f'gs://{raw_zone_bucket}/temp',
        service_account_email='svc-dfl-user@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',
        dataflow_kms_key='projects/tnt01-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptoKeys/keyhsm-kms-tnt01-euwe2-cdp',
        subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88h/regions/europe-west2/subnetworks/odycda-csn-euwe2-kc1-01-bld-01',
        num_workers=1,
        max_num_workers=4,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Read Files' >> beam.io.ReadFromText(f'gs://{raw_zone_bucket}/{raw_zone_folder_path}/*.schema.csv')
            | 'Process Schema Files' >> beam.FlatMap(process_schema_files)
            | 'Format Output' >> beam.Map(lambda x: f"{x['folder_name']},{x['file_name_prefix']},{x['schema_data']}")
            | 'Write Output' >> beam.io.WriteToText(output_file_path)
        )

if __name__ == '__main__':
    # Replace with your actual configurations
    project_id = 'your-project-id'
    raw_zone_bucket = 'your-raw-bucket'
    raw_zone_folder_path = 'your-raw-folder-path/*'  # Use wildcard to read all schema files
    output_file_path = 'gs://your-output-bucket/output.txt'  # Output file should be in a GCS path

    run_pipeline(project_id, raw_zone_bucket, raw_zone_folder_path, output_file_path)
