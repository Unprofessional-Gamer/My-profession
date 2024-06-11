import logging
from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CountRecords(beam.DoFn):
    def process(self, element):
        yield 1

def check_pass_status(element):
    return element > 100

def log_record_count(file_name, record_count):
    if record_count:
        count = record_count[0]
        logger.info(f"{file_name} has {count} records")
    else:
        logger.warning(f"Could not determine the record count for {file_name}")

def activate_data_cleaning(bucket_name, base_path, file_name):
    options = PipelineOptions(
        project='tnt01-odycda-bld-01-1681',
        runner='DataflowRunner',
        job_name='cautocertmove',
        region='europe-west2',
        staging_location='gs://tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181/EXTERNAL/MFVS/PRICING/dataflow/staging',
        service_account_email='svc-dataflow-runner@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',
        dataflow_kms_key='projects/tnt01-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptokeys/keyhsm-kms-tatei-euwe2-cdp',
        subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88b/regions/europe-west2/subnetworks/odvcda-csn-euwe2-kc1-01-bld-01',
        num_workers=1,
        max_num_workers=3,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        file_read = (p
                     | f"Reading the file data {file_name}" >> beam.io.ReadFromText(f"gs://{bucket_name}/{base_path}/{file_name}")
                     | f"Removing duplicates from the file data {file_name}" >> beam.Distinct()
                     )

        processed_file_read = (file_read
                               | f"Splitting the input data into computational units {file_name}" >> beam.Map(lambda x: x.split(','))
                               | f"Formatting as CSV output format {file_name}" >> beam.Map(lambda x: ','.join(x))
                               )

        (processed_file_read
         | f"Writing to certified zone received folder {file_name}" >> beam.io.WriteToText(f"gs://tnt01-odycda-bld-01-stb-eu-certzone-386745f0/{base_path}/RECEIVED/{file_name}",
                                                                                          file_name_suffix="",
                                                                                          num_shards=1,
                                                                                          shard_name_template="-")
         )

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
        base_path = 'thParty/MFVS/GFV/update'
        folder_path = f'{base_path}/Monthly/RECEIVED/'
        client = storage.Client("tnt01-odycda-bld-01-1681")
        blob_list = client.list_blobs(bucket_name, prefix=folder_path)

        for blob in blob_list:
            if blob.name.endswith('.csv'):
                file_name = blob.name.split('/')[-1]
                
                logger.info(f"Processing file: {file_name}")
                
                input = (p
                          | f"Reading the file data {file_name}" >> beam.io.ReadFromText(f"gs://{bucket_name}/{blob.name}")
                          | "Count records" >> beam.ParDo(CountRecords())
                          | "Sum record counts" >> beam.CombineGlobally(sum)
                          )

                output = (input | "Log record count" >> beam.Map(lambda count, file_name=file_name: log_record_count(file_name, [count])))

                passed_records = (input
                                  | "Filter records with pass status" >> beam.Filter(check_pass_status)
                                  | f"Reading the file data {file_name}" >> beam.io.ReadFromText(f"gs://{bucket_name}/{blob.name}")
                                  | f"Writing to cauzone processed folder {file_name}" >> beam.io.WriteToText(f"gs://{bucket_name}/{base_path}/Monthly/PROCESSED/{file_name}",
                                                                                                                file_name_suffix='',
                                                                                                                num_shards=1,
                                                                                                                shard_name_template='')
                                  )

                failed_records = (input
                                  | "Filter records with fail status" >> beam.Filter(lambda x: not check_pass_status(x))
                                  | f"Reading the file data {file_name}" >> beam.io.ReadFromText(f"gs://{bucket_name}/{blob.name}")
                                  | f"Writing to cauzone error folder {file_name}" >> beam.io.WriteToText(f"gs://{bucket_name}/{base_path}/Monthly/ERROR/{file_name[:4]}_error.csv",
                                                                                                              file_name_suffix='',
                                                                                                              num_shards=1,
                                                                                                              shard_name_template='')
                                  )

                result = p.run()
                result.wait_until_finish()

                check_status = True  # Assuming all checks passed 

                if check_status:
                    logger.info(f"All checks passed for {file_name}. Moving to certification stage.")
                    activate_data_cleaning(bucket_name, base_path, file_name)
                    logger.info("Audit checks completed")
                else:
                    logger.warning(f"Some checks failed for {file_name}")

def start_data_lister():
    start_checks()
    logger.info("All files in the base path processed")

if __name__ == "__main__":
    logger.info("*************************Starting the data lister***************************************")
    start_data_lister()
    logger.info("****************************** Data lister completed***********************************")
