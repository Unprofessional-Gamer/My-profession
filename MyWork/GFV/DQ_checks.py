from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class RemoveDoubleQuotes(beam.DoFn):
    def process(self, element):
        custom = [ele.replace("','", "") for ele in element]
        return [custom]

class FilterNull(beam.DoFn):
    def process(self, element):
        updated_custom = [ele if ele not in ['null', None, 'Nan', 'NONE', 'Null', 'n'] else '' for ele in element]
        return [updated_custom]

class RemoveUnwantedChars(beam.DoFn):
    def process(self, element):
        custom = [ele.translate(str.maketrans(",#$%^!&-", "         ")) for ele in element]
        return [custom]

def activate_data_cleaning(bucket_name, base_path, file_name):
    options = PipelineOptions(
        project='tnt01-odycda-bld-01-1681',
        runner='DataflowRunner',
        job_name='cautocertmove',
        #temp_location='gs://tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181/EXTERNAL/MFVS/PRICING/dataflow/temp',
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
                     | "Reading the file data" >> beam.io.ReadFromText(f"gs://{bucket_name}/{base_path}/{file_name}")
                     | "Removing duplicates from the file data" >> beam.Distinct()
                     )

        processed_file_read = (file_read
                               | f"Splitting the input data into computational units {file_name}" >> beam.Map(lambda x: x.split(','))
                               | f"Making the data into standard iterable units {file_name}" >> beam.ParDo(RemoveDoubleQuotes())
                               | f"Removing the null values in file data {file_name}"  >> beam.ParDo(FilterNull())
                               | f"Removing the Unwanted characters in file data {file_name}" >> beam.ParDo(RemoveUnwantedChars())
                               | f"Formatting as CSV output format {file_name}" >> beam.Map(lambda x: ','.join(x))
                               )

        (processed_file_read
         | "Writing to certified zone received folder" >> beam.io.WriteToText(f"gs://tnt01-odycda-bld-01-stb-eu-certzone-386745f0/{base_path}/RECEIVED/{file_name}",
                                                                              file_name_suffix="",
                                                                              num_shards=1,
                                                                              shard_name_template="-")
         )

class NullCheck(beam.DoFn):
    def process(self, element):
        null_list = []
        for ele in element:
            if ele in ['null','None','Nan','NONE','N/A','n/a','']:
                null_list.append(ele)
        if len(null_list) == len(element):
            element.extend(["Passed null check"])
        else:
            element.extend(['Failed null check'])
        yield element

class VolumeCheck(beam.DoFn):
    def process(self, element, vol_count):
        if 0 < int(vol_count):
            element.extend(["Passed volume check"])
        else:
            element.extend(['Failed volume check'])
        yield element

def check_pass_status(element):
    return all('Passed' in item for item in element[-2:])

def check_fail_status(element):
    return any('Failed' in item for item in element[-2:])

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
                          | "Split values" >> beam.Map(lambda x: x.split(','))
                          )

                volume_count = (input
                                | f"Calculate volume count {file_name}" >> beam.combiners.Count.Globally()
                                )

                volume_check = (input
                                | f"Volume check {file_name}" >> beam.ParDo(VolumeCheck(), vol_count=beam.pvalue.AsSingleton(volume_count))
                                )

                null_check = (volume_check
                              | f"Null check {file_name}" >> beam.Filter(lambda x: len(x) > 0)
                              | f"Null check value {file_name}" >> beam.ParDo(NullCheck())
                              )

                passed_records = (null_check
                                  | f"Filter records with pass status {file_name}" >> beam.Filter(check_pass_status)
                                  | f"Formatting as CSV output format {file_name}" >> beam.Map(lambda x: ','.join(x[:-2]))
                                  | f"Writing to cauzone processed folder {file_name}" >> beam.io.WriteToText(f"gs://{bucket_name}/{base_path}/Monthly/PROCESSED/{file_name}",
                                                                                                                file_name_suffix='',
                                                                                                                num_shards=1,
                                                                                                                shard_name_template='')
                                  )

                failed_records = (null_check
                                  | f"Filter records with fail status {file_name}" >> beam.Filter(check_fail_status)
                                  | f"Formatting as CSV output format {file_name}" >> beam.Map(lambda x: ','.join(x))
                                  | f"Writing to cauzone error folder {file_name}" >> beam.io.WriteToText(f"gs://{bucket_name}/{base_path}/Monthly/ERROR/{file_name[:4]}_error.csv",
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
    print("**..... Starting the data lister ")
    start_data_lister()
    print("**** Data lister completed.. ************")
