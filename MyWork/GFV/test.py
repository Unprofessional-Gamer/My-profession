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
        temp_location='gs://tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181/EXTERNAL/MFVS/PRICING/dataflow/temp',
        region='europe-west2',
        staging_location='gs://tnt01-odycda-bid-01-stb-eu-rawzone-52fd7181/EXTERNAL/MFVS/PRICING/dataflow/staging',
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
                               | f"Making the data into standard iterable units {file_name}" >> beam.ParDo(RemoveDoubleQuotes())
                               | f"Removing the null values in file data {file_name}" >> beam.ParDo(FilterNull())
                               | f"Removing the Unwanted characters in file data {file_name}" >> beam.ParDo(RemoveUnwantedChars())
                               | f"Formatting to get CSV output format {file_name}" >> beam.Map(lambda x: ','.join(x))
                               )

        (processed_file_read
         | f"Writing to certified zone received folder {file_name}" >> beam.io.WriteToText(
             f"gs://tnt01-odycda-bld-01-stb-eu-certzone-386745f0/{base_path}/Monthly/RECEIVED/{file_name}",
             file_name_suffix="",
             num_shards=1,
             shard_name_template="-")
         )

class NullCheck(beam.DoFn):
    def process(self, element):
        null_list = []
        for ele in element:
            if ele.strip() == '':
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
        job_name='rawtocertcleaning',
        region='europe-west2',
        staging_location='gs://tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181/EXTERNAL/MFVS/PRICING/dataflow/staging',
        service_account_email='svc-dataflow-runner@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',
        dataflow_kms_key='projects/tnt01-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptokeys/keyhsm-kms-tnt01-euwe2-cdp',
        subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88b/regions/europe-west2/subnetworks/odycda-csn-euwe2-kc1-01-bld-01',
        num_workers=1,
        max_num_workers=3,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )

    bucket_name = 'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181'
    base_path = 'thParty/MFVS/GFV/update'
    folder_path = f'{base_path}/Monthly/RECEIVED/'
    client = storage.Client("tnt01-odycda-bld-01-1681")
    blobs = client.list_blobs(bucket_name, prefix=folder_path)
    
    with beam.Pipeline(options=options) as p:
        for blob in blobs:
            if blob.name.endswith('.csv'):
                file_name = blob.name.split('/')[-1]
                inputt = (p
                          | f"Reading the file data {file_name}" >> beam.io.ReadFromText(f"gs://{bucket_name}/{base_path}/Monthly/RECEIVED/{file_name}")
                          | "Split values" >> beam.Map(lambda x: x.split(','))
                          )

                volume_count = (inputt
                                | f"Calculate volume count {file_name}" >> beam.combiners.Count.Globally()
                                )

                volume_check = (inputt
                                | f"Volume check {file_name}" >> beam.ParDo(VolumeCheck(), vol_count=beam.pvalue.AsSingleton(volume_count))
                                )

                null_check = (volume_check
                              | f"Null check {file_name}" >> beam.Filter(lambda x: len(x) > 0)
                              | f"Null check value {file_name}" >> beam.ParDo(NullCheck())
                              )

                passed_records = (null_check
                                  | f"Filter records with pass status {file_name}" >> beam.Filter(check_pass_status)
                                  | f"Formatting into CSV output format {file_name}" >> beam.Map(lambda x: ','.join(x[:-2]))
                                  | f"Writing to cauzone processed folder {file_name}" >> beam.io.WriteToText(
                                      f"gs://{bucket_name}/{base_path}/Monthly/PROCESSED/{file_name[:-4]}.csv",
                                      file_name_suffix='',
                                      num_shards=1,
                                      shard_name_template=''
                                  )
                                  )

                failed_records = (null_check
                                  | f"Filter records with fail status {file_name}" >> beam.Filter(check_fail_status)
                                  | f"Formatting as CSV output format {file_name}" >> beam.Map(lambda x: ','.join(x))
                                  | f"Writing to cauzone error folder {file_name}" >> beam.io.WriteToText(
                                      f"gs://{bucket_name}/{base_path}/Monthly/ERROR/{file_name[:-4]}_error.csv",
                                      file_name_suffix='',
                                      num_shards=1,
                                      shard_name_template=''
                                  )
                                  )

                result = p.run()
                result.wait_until_finish()

                check_status = True

                if check_status:
                    print("Cert Checks finished")
                    activate_data_cleaning(bucket_name, base_path, file_name)
                    print("Audit checks completed")
                else:
                    print("Cert Checks failed")

def set_mime_type(bucket_name, file_path, mime_type):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    blob.content_type = mime_type
    blob.patch()

def change_mime_type_for_all(bucket_name, base_path):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=base_path)
    
    for blob in blobs:
        if blob.name.endswith('.csv'):
            print(f"Changing MIME type for {blob.name} to text/csv")
            set_mime_type(bucket_name, blob.name, 'text/csv')

def start_data_lister():
    start_checks()
    print("Proceeding with next year data")
    print("All data finished \nProceeding with next dataset")
    print("All the datasets finished")
    print("All the datasets finished")
    change_mime_type_for_all('tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181', 'thParty/MFVS/GFV/update/Monthly/PROCESSED/')
    change_mime_type_for_all('tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181', 'thParty/MFVS/GFV/update/Monthly/ERROR/')

if __name__ == "__main__":
    print("**..... Starting the data lister ")
    start_data_lister()
    print("**** Data lister completed.. ************")
