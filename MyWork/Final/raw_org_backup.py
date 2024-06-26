import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime

class CopyFilesFn(beam.DoFn):
    def __init__(self, source_paths, destination_path):
        self.source_paths = source_paths
        self.destination_path = destination_path

    def process(self, element):
        # Get current month and year
        folder_date = datetime.now().strftime('%y-%Y')
        destination_base_path = f'{self.destination_path}/{folder_date}'
        archive_folder_path = f'{destination_base_path}/ARCHIVE'
        
        # Create destination base path if it doesn't exist
        FileSystems.mkdirs([destination_base_path])
        FileSystems.mkdirs([archive_folder_path])
        
        for source_path in self.source_paths:
            match_results = FileSystems.match([source_path + '/*'])
            for match_result in match_results:
                for metadata in match_result.metadata_list:
                    source_file_path = metadata.path
                    # Generate destination file path
                    relative_path = source_file_path.replace(source_path, '').lstrip('/')
                    destination_file_path = f'{archive_folder_path}/{relative_path}'
                    
                    # Copy file to destination
                    FileSystems.copy([source_file_path], [destination_file_path])
                    print(f'Copied {source_file_path} to {destination_file_path}')
                    
                    # Delete original file
                    FileSystems.delete([source_file_path])
                    print(f'Deleted {source_file_path}')
                    
                    yield f'Copied {source_file_path} to {destination_file_path}'

def run_pipeline(project_id, raw_zone_bucket_name, raw_files_paths, destination_folder_path, destination_bucket_name):
    options = PipelineOptions(
        project=project_id,
        runner="DirectRunner",
        region='europe-west2',
        staging_location=f'gs://{raw_zone_bucket_name}/staging',
        temp_location=f'gs://{raw_zone_bucket_name}/temp',
        service_account_email='svc-dfl-user@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',
        dataflow_kms_key='projects/tnt01-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptoKeys/keyhsm-kms-tnt01-euwe2-cdp',
        subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88h/regions/europe-west2/subnetworks/odycda-csn-euwe2-kc1-01-bld-01',
        num_workers=1,
        max_num_workers=4,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )

    raw_zone_paths = [f'gs://{raw_zone_bucket_name}/{path}' for path in raw_files_paths]
    consumer_path = f'gs://{destination_bucket_name}/{destination_folder_path}'

    with beam.Pipeline(options=options) as p:
        copy_results = (
            p
            | 'Create start' >> beam.Create([None])
            | 'Copy files' >> beam.ParDo(CopyFilesFn(raw_zone_paths, consumer_path))
        )

        copy_results | beam.io.WriteToText(f'gs://{raw_zone_bucket_name}/copy_results', file_name_suffix='.txt')

if __name__ == "__main__":
    # Define your parameters
    project_id = 'tnt01-odycda-bld-01'
    raw_zone_bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    raw_files_paths = ["INTERNAL/MFVS/GFV/CAP", "INTERNAL/MFVS/GFV/GFV"]
    destination_folder_path = "thparty/MFVS/GFV"
    destination_bucket_name = raw_zone_bucket_name  # Assuming the same bucket, if different, specify here

    run_pipeline(project_id, raw_zone_bucket_name, raw_files_paths, destination_folder_path, destination_bucket_name)
