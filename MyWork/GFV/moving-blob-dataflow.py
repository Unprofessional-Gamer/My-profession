import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.filesystems import FileSystems

class FilterAndMoveFiles(beam.DoFn):
    def __init__(self, source_bucket, destination_bucket, source_prefix, destination_prefix, prefixes):
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.source_prefix = source_prefix
        self.destination_prefix = destination_prefix
        self.prefixes = prefixes

    def process(self, element):
        from google.cloud import storage
        client = storage.Client()

        source_bucket = client.bucket(self.source_bucket)
        destination_bucket = client.bucket(self.destination_bucket)
        blob_name = element.path
        blob = source_bucket.blob(blob_name)

        if any(blob.name.startswith(f"{self.source_prefix}/{prefix}") for prefix in self.prefixes):
            destination_blob_name = f"{self.destination_prefix}/{blob.name[len(self.source_prefix)+1:]}"
            print(f"Copying {blob.name} to {destination_blob_name}")
            source_bucket.copy_blob(blob, destination_bucket, destination_blob_name)
            print(f"Deleting {blob.name} from {self.source_bucket}")
            blob.delete()
            yield f"Moved: {blob.name} to {destination_blob_name}"
        else:
            print(f"Skipping {blob.name} as it does not match any of the prefixes")

def run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path):
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
            | 'Move Files' >> beam.ParDo(FilterAndMoveFiles(
                raw_zone_bucket_name, consumer_bucket_name, raw_zone_folder_path, consumer_folder_path, prefixes))
            | 'Print Results' >> beam.Map(print)
        )

if __name__ == '__main__':
    project_id = 'your-gcp-project-id'
    raw_zone_bucket_name = "vdlsb73te"
    raw_zone_folder_path = "thparty/MFVS/GFV/SFGDrop"
    consumer_bucket_name = "ghds732yeybdjbj"
    consumer_folder_path = "thparty/MFVS/GFV/Archieve"
    prefixes = [
        "LDENEW",
        "LPRVAL",
        "CDEVAL",
        "CPRVAL",
        "CDENEW",
        "CPRNEW",
        "LPENEW",
        "LPRVAN",
        "CPRRVU",
        "CPRRVN",
    ]

    print("Starting the pipeline...")
    run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path)
    print("Pipeline execution completed.")
