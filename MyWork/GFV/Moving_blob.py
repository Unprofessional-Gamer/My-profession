from google.cloud import storage

prefixes = [
        "CPRNEW",
        "CDENEW",
        "CPRVAL",
        "CDEVAL",
        "LDENEW",
        "LPRNEW",
        "LDEVAL",
        "LPRVAL",
        "CPRRVU",
        "CPRRVN",
        ]

def move_files_with_prefixes(source_bucket_name,source_folder_path,destination_bucket_name,destination_folder_path,prefixes):
    # Initialize the GCS client
    client = storage.Client(project=project_id)

    # Get the source and destination buckets
    source_bucket = client.bucket(source_bucket_name)
    destination_bucket = client.bucket(destination_bucket_name)

    # List blobs in the source bucket with the specified folder path
    blobs = client.list_blobs(source_bucket, prefix=source_folder_path)

    for blob in blobs:
        # Check if the blob's name starts with any of the specified prefixes
        if any(blob.name.startswith(f"{source_folder_path}/{prefix}") for prefix in prefixes):
            # Define the destination blob name
            destination_blob_name = f"{destination_folder_path}/{blob.name[len(source_folder_path)+1:]}"
            destination_blob = destination_bucket.blob(destination_blob_name)

            # Copy the blob to the destination bucket
            source_bucket.copy_blob(blob, destination_bucket, destination_blob_name)

            # Delete the blob from the source bucket
            blob.delete()

            print(f"Moved: {blob.name} to {destination_blob_name}")

if __name__ == "__main__":
    # Define your parameters
    raw_zone_bucket = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    raw_zone_folder_path = "thparty/MFVS/GFV/SFGDrop"
    DP_consumer_bucket = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    consumer_folder_path = "thparty/MFVS/GFV/Archieve"
    project_id = 'tnt01-odycda-bld-01-1b81'


    # Move the files
    print("**************** Files Moving Started  *****************")
    move_files_with_prefixes(raw_zone_bucket,raw_zone_folder_path,DP_consumer_bucket,consumer_folder_path,prefixes)
    print("**************** Files Moving Completed *****************")


################################################################################################################################3

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

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
        blob = source_bucket.blob(element.path)

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
            | 'List Files' >> beam.io.gcp.gcs.MatchFiles(f'gs://{raw_zone_bucket_name}/{raw_zone_folder_path}/*')
            | 'Match Results' >> beam.io.gcp.gcs.ReadMatches()
            | 'Move Files' >> beam.ParDo(FilterAndMoveFiles(
                raw_zone_bucket_name, consumer_bucket_name, raw_zone_folder_path, consumer_folder_path, prefixes))
            | 'Print Results' >> beam.Map(print)
        )

if __name__ == '__main__':

    # Define your parameters

    project_id = 'tnt01-odycda-bld-01-1b81'
    raw_zone_bucket = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    raw_zone_folder_path = "thparty/MFVS/GFV/SFGDrop"
    DP_consumer_bucket = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    consumer_folder_path = "thparty/MFVS/GFV/Archieve"

    prefixes = [
        "CPRNEW",
        "CDENEW",
        "CPRVAL",
        "CDEVAL",
        "LDENEW",
        "LPRNEW",
        "LDEVAL",
        "LPRVAL",
        "CPRRVU",
        "CPRRVN",
        ]

    print("***************************************** Starting the pipeline ******************************************")
    run_pipeline(project_id, raw_zone_bucket, raw_zone_folder_path, DP_consumer_bucket, consumer_folder_path)
    print("***************************************** Pipeline execution completed ***********************************")

