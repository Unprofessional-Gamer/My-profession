from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class CountRecords(beam.DoFn):
    def process(self, element):
        # Simply pass the element along with a count of 1
        yield 1

def check_pass_status(element):
    # Check if the total count is greater than 100
    return element > 100

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

        record_count = (file_read
                        | f"Counting records in {file_name}" >> beam.ParDo(CountRecords())
                        | f"Summing record counts in {file_name}" >> beam.CombineGlobally(sum)
                        )

        passed_records = (record_count
                          | "Filter records with pass status" >> beam.Filter(check_pass_status)
                          | "Pass status to string" >> beam.Map(lambda x: f"Passed with {x} records")
                          | f"Writing pass status to {file_name}" >> beam.io.WriteToText(f"gs://{bucket_name}/{base_path}/PROCESSED/{file_name}",
                                                                                        file_name_suffix="",
                                                                                        num_shards=1,
                                                                                        shard_name_template="-")
                          )

        failed_records = (record_count
                          | "Filter records with fail status" >> beam.Filter(lambda x: not check_pass_status(x))
                          | "Fail status to string" >> beam.Map(lambda x: f"Failed with {x} records")
                          | f"Writing fail status to {file_name}" >> beam.io.WriteToText(f"gs://{bucket_name}/{base_path}/ERROR/{file_name[:4]}_error.csv",
                                                                                        file_name_suffix="",
                                                                                        num_shards=1,
                                                                                        shard_name_template="-")
                          )

        result = p.run()
        result.wait_until_finish()

        check_status = True  # Assuming all checks passed 

        if check_status:
            print(f"All checks passed for {file_name}")
            activate_data_cleaning(bucket_name, base_path, file_name)
            print("Audit checks completed")
        else:
            print(f"Some checks failed for {file_name}")

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
                input = (p
                          | f"Reading the file data {file_name}" >> beam.io.ReadFromText(f"gs://{bucket_name}/{blob.name}")
                          | "Count records" >> beam.ParDo(CountRecords())
                          | "Sum record counts" >> beam.CombineGlobally(sum)
                          )

                passed_records = (input
                                  | "Filter records with pass status" >> beam.Filter(check_pass_status)
                                  | "Pass status to string" >> beam.Map(lambda x: f"Passed with {x} records")
                                  | f"Writing pass status to {file_name}" >> beam.io.WriteToText(f"gs://{bucket_name}/{base_path}/Monthly/PROCESSED/{file_name}",
                                                                                                file_name_suffix='',
                                                                                                num_shards=1,
                                                                                                shard_name_template='')
                                  )

                failed_records = (input
                                  | "Filter records with fail status" >> beam.Filter(lambda x: not check_pass_status(x))
                                  | "Fail status to string" >> beam.Map(lambda x: f"Failed with {x} records")
                                  | f"Writing fail status to {file_name}" >> beam.io.WriteToText(f"gs://{bucket_name}/{base_path}/Monthly/ERROR/{file_name[:4]}_error.csv",
                                                                                                file_name_suffix='',
                                                                                                num_shards=1,
                                                                                                shard_name_template='')
                                  )

                result = p.run()
                result.wait_until_finish()

                check_status = True  # Assuming all checks passed 

                if check_status:
                    print(f"All checks passed for {file_name}")
                    activate_data_cleaning(bucket_name, base_path, file_name)
                    print("Audit checks completed")
                else:
                    print(f"Some checks failed for {file_name}")

def start_data_lister():
    start_checks()
    print("All files in the base path processed")

if __name__ == "__main__":
    print("*************************Starting the data lister***************************************")
    start_data_lister()
    print("****************************** Data lister completed***********************************")
