import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from datetime import datetime

class MoveGCSFiles(beam.DoFn):
    def __init__(self, bucket_name, folder_path, archive_folder_path):
        self.bucket_name = bucket_name
        self.folder_path = folder_path
        self.archive_folder_path = archive_folder_path
        self.current_month = datetime.now().strftime('%m-%Y')
    
    def start_bundle(self):
        self.client = storage.Client(project_id)
        self.bucket = self.client.bucket(self.bucket_name)
    
    def process(self, element):
        source_blob = self.bucket.blob(element)
        destination_blob_name = f"{self.archive_folder_path}/{self.current_month}/Archive/{element.split('/')[-1]}"
        destination_blob = self.bucket.blob(destination_blob_name)

        # Copy the file to the archive folder
        self.bucket.copy_blob(source_blob, self.bucket, destination_blob_name)
        
        # Delete the original file
        source_blob.delete()
        
        yield f"Moved to archive and deleted: {element}"

def list_files(bucket_name, folder_path):
    client = storage.Client(project_id)
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    return [blob.name for blob in blobs]

def dq_archiev_pipeline(project_id, raw_zone_bucket, raw_zone_folder_path, archive_folder_path):
    options = PipelineOptions(
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

    with beam.Pipeline(options=options) as p:
        files_to_move = list_files(raw_zone_bucket, raw_zone_folder_path)

        move_results = (
            p 
            | 'Create PCollection' >> beam.Create(files_to_move)
            | 'Move Files' >> beam.ParDo(MoveGCSFiles(raw_zone_bucket, raw_zone_folder_path, archive_folder_path))
        )

if __name__ == "__main__":
    project_id = 'tenu-wiue2-k'
    raw_zone_bucket = "raw_zone_bucket"
    raw_zone_folder_path = 'raw_zone_folder_path'
    archive_folder_path = 'archive_folder_path'

    dq_archiev_pipeline(project_id, raw_zone_bucket, raw_zone_folder_path, archive_folder_path)
