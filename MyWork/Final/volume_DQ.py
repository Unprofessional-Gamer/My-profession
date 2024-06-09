from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def count_records(element):
    yield len(element)

def move_file_based_on_record_count(bucket_name, base_path, file_name, record_count):
    if record_count < 100:
        destination_folder = "ERROR"
    else:
        destination_folder = "PROCESSED"

    client = storage.Client()
    blob = client.bucket(bucket_name).blob(f"{base_path}/Monthly/{destination_folder}/{file_name}")
    source_blob = client.bucket(bucket_name).blob(f"{base_path}/Monthly/RECEIVED/{file_name}")

    blob.copy(source_blob)
    source_blob.delete()

def start_checks():
    options = PipelineOptions(
        project='tnt01-odycda-bld-01-1681',
        runner='DataflowRunner',
        job_name='rawtocertcleaning_all_files',
        region='europe-west2',
        staging_location='gs://tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181/EXTERNAL/MFVS/PRICING/dataflow/staging',
        service_account_email='svc-dataflow-runner@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',
        dataflow_kms_key='projects/tnt01-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptokeys/keyhsm-kms-tatei-euwe2-cdp',
        subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88b/regions/europe-west2/subnetworks/odycda-csn-euwe2-kc1-01-bld-01',
        num_workers=1,
        max_num_workers=3,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        bucket_name = 'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181'
        base_path = 'EXTERNAL/MFVS/PRICING'
        folder_path = f'{base_path}/Monthly/RECEIVED/'
        client = storage.Client()
        blobs = client.list_blobs(bucket_name, prefix=folder_path)

        for blob in blobs:
            if blob.name.endswith('.csv'):
                file_name = blob.name.split('/')[-1]
                input = (p
                          | f"Reading the file data {file_name}" >> beam.io.ReadFromText(blob.name)
                          | "Split values" >> beam.Map(lambda x: x.split(','))
                          )

                record_count = (input
                              | f"Count records {file_name}" >> beam.ParDo(count_records)
                              | f"Sum record counts {file_name}" >> beam.combiners.Sum.Globally()
                            )

                result = p.run()
                result.wait_until_finish()

                record_count_value = result.metrics().query(beam.metrics.MetricsFilter().with_name('processed_records')).count
                
                print(f"Record count for {file_name}: {record_count_value}")

                move_file_based_on_record_count(bucket_name, base_path, file_name, record_count_value)

def start_data_lister():
    start_checks()
    print("All files in the base path processed")

if __name__ == "__main__":
    print("*************************Starting the data lister***************************************")
    start_data_lister()
    print("****************************** Data lister completed***********************************")
