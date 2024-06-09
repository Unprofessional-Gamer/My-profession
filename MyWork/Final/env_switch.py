import apache_beam as beam
from apache_beam.io import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
import logging

class FilterAndMoveFiles(beam.DoFn):
    def __init__(self, source_bucket_name, destination_bucket_name, source_folder, destination_folder, prefixes):
        self.source_bucket_name = source_bucket_name
        self.destination_bucket_name = destination_bucket_name
        self.source_folder = source_folder
        self.destination_folder = destination_folder
        self.prefixes = prefixes

    def setup(self):
        self.source_client = storage.Client()
        self.destination_client = storage.Client()

    def process(self, element):
        source_blob_name = element.path[len(f'gs://{self.source_bucket_name}/'):]
        file_name = source_blob_name.replace(self.source_folder + '/', '', 1)
        
        if any(file_name.startswith(prefix) for prefix in self.prefixes):
            destination_blob_name = f'{self.destination_folder}/{file_name}'

            source_bucket = self.source_client.bucket(self.source_bucket_name)
            source_blob = source_bucket.blob(source_blob_name)
            
            destination_bucket = self.destination_client.bucket(self.destination_bucket_name)
            source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)

            logging.info(f"Copied {source_blob_name} to {destination_blob_name}")
            yield f"Copied {source_blob_name} to {destination_blob_name}"
        else:
            logging.info(f"Skipped {source_blob_name} as it does not match the prefixes")
            yield f"Skipped {source_blob_name} as it does not match the prefixes"

def main():
    logging.getLogger().setLevel(logging.INFO)
    
    project_id = 'your-project-id'
    raw_zone_bucket_name = 'source-bucket-name'
    raw_zone_folder_path = 'source-folder'
    consumer_bucket_name = 'destination-bucket-name'
    consumer_folder_path = 'destination-folder'
    prefixes = ['CPRRVU', 'CPRRVN']  # Specific prefixes to filter files

    options = PipelineOptions(
        project=project_id,
        runner="DataflowRunner",
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

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'List Files' >> beam.Create([f'gs://{raw_zone_bucket_name}/{raw_zone_folder_path}/**'])
            | 'Match Files' >> beam.FlatMap(lambda pattern: FileSystems.match([pattern])[0].metadata_list)
            | 'Filter and Move Files' >> beam.ParDo(FilterAndMoveFiles(
                raw_zone_bucket_name, consumer_bucket_name, raw_zone_folder_path, consumer_folder_path, prefixes))
            | 'Print Results' >> beam.Map(print)
        )

if __name__ == '__main__':
    main()
