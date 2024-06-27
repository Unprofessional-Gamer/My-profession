import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime

class CopyFilesFn(beam.DoFn):
    def __init__(self, source_path, destination_path):
        self.source_path = source_path
        self.destination_path = destination_path

    def process(self, element):
        # Get current month and year
        folder_date = datetime.now().strftime('%m-%Y')
        destination_folder_path = f'{self.destination_path}/{folder_date}'
        
        match_results = FileSystems.match([self.source_path + '/*'])
        for match_result in match_results:
            for metadata in match_result.metadata_list:
                source_file_path = metadata.path
                destination_file_path = source_file_path.replace(self.source_path, destination_folder_path)
                FileSystems.copy([source_file_path], [destination_file_path])
                print(f'Copied {source_file_path} to {destination_file_path}')
                yield f'Copied {source_file_path} to {destination_file_path}'

def archieve_pipeline(project_id, raw_zone_bucket_name, cap_source_path, gfv_source_path, cap_destination_path, gfv_destination_path):
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
        # Process files from cap_source_path to cap_destination_path
        copy_results1 = (
            p
            | 'Create start1' >> beam.Create([None])
            | 'Copy files from cap_source_path' >> beam.ParDo(CopyFilesFn(cap_source_path, cap_destination_path))
        )
        
        # Process files from gfv_source_path to gfv_destination_path
        copy_results2 = (
            p
            | 'Create start2' >> beam.Create([None])
            | 'Copy files from gfv_source_path' >> beam.ParDo(CopyFilesFn(gfv_source_path, gfv_destination_path))
        )

        # Combine results and write to text file
        (copy_results1, copy_results2)
        | 'Flatten results' >> beam.Flatten()
        | 'Write results' >> beam.io.WriteToText(f'gs://{raw_zone_bucket_name}/copy_results', file_name_suffix='.csv' or '.zip')

if __name__ == "__main__":
    # Define your parameters
    project_id = 'tnt01-odycda-bld-01'
    raw_zone_bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    cap_source_path = "gs://tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a/thparty/MFVS/GFV/SFGDrop"
    gfv_source_path = "gs://tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a/thparty/MFVS/GFV/AnotherDrop"
    cap_destination_path = "gs://tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a/thparty/MFVS/GFV/Destination1"
    gfv_destination_path = "gs://tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a/thparty/MFVS/GFV/Destination2"

    archieve_pipeline(project_id, raw_zone_bucket_name, cap_source_path, gfv_source_path, cap_destination_path, gfv_destination_path)
