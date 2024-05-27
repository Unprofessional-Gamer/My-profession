import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage

class RemoveDoubleQuotes(beam.DoFn):
    def process(self, element):
        custom = [ele.replace('"', '') for ele in element]
        return [custom]

class FilterNulls(beam.DoFn):
    def process(self, element):
        updated_custom = [ele if ele not in ['null', None, 'NaN', 'NONE', 'Null', 'n/a'] else '' for ele in element]
        return [updated_custom]

class RemoveSpecialChars(beam.DoFn):
    def process(self, element):
        custom = [ele for ele in element]
        remove_table = str.maketrans("", "", "1234567890-=+")
        updated_custom = [ele.translate(remove_table) for ele in custom]
        return [updated_custom]

class NullCheck(beam.DoFn):
    def process(self, element):
        null_list = []
        for ele in element:
            if ele in ['null', None, 'NaN', 'NONE', 'Null', 'n/a']:
                null_list.append(ele)
        if len(null_list) == 0:
            element.extend(["Passed null check"])
        else:
            element.extend(["Failed null check"])
        yield element

class VolumeCheck(beam.DoFn):
    def process(self, element, vol_count):
        if 0 < int(vol_count) < 1000:
            element.extend(["Passed volume check"])
        else:
            element.extend(["Failed volume check"])
        yield element

def check_pass_status(element):
    return all('Passed' in item for item in element[-2:])

def check_fail_status(element):
    return any('Failed' in item for item in element[-2:])

def run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path):
    options = PipelineOptions(
        project=project_id,
        runner="DataflowRunner",
        temp_location=f'gs://{raw_zone_bucket_name}/temp',
        region='europe-west2',
        staging_location=f'gs://{raw_zone_bucket_name}/staging',
        service_account_email='svc-dataflow-runner@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',
        dataflow_kms_key='projects/tnt01-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptoKeys/keyhsm-kms-tnt01-euwe2-cdp',
        subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88h/regions/europe-west2/subnetworks/odycda-csn-euwe2-kc1-01-bld-01',
        num_workers=1,
        max_num_workers=4,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        raw_files = (
            p
            | 'List Files' >> beam.io.fileio.MatchFiles(f'gs://{raw_zone_bucket_name}/{raw_zone_folder_path}/*.csv')
            | 'Read Matches' >> beam.io.fileio.ReadMatches()
            | 'Read Files' >> beam.FlatMap(lambda x: beam.io.ReadFromText(x.metadata.path))
        )

        processed_files = (
            raw_files
            | 'Split Rows' >> beam.Map(lambda x: x.split(','))
            | 'Remove Double Quotes' >> beam.ParDo(RemoveDoubleQuotes())
            | 'Filter Nulls' >> beam.ParDo(FilterNulls())
            | 'Remove Special Chars' >> beam.ParDo(RemoveSpecialChars())
        )

        volume_count = (
            processed_files
            | 'Count Rows' >> beam.combiners.Count.Globally()
        )

        checked_files = (
            processed_files
            | 'Null Check' >> beam.ParDo(NullCheck())
            | 'Volume Check' >> beam.ParDo(VolumeCheck(), vol_count=beam.pvalue.AsSingleton(volume_count))
        )

        passed_records = (
            checked_files
            | 'Filter Passed Records' >> beam.Filter(check_pass_status)
            | 'Format CSV' >> beam.Map(lambda x: ','.join(x[:-2]))
            | 'Write Passed Records' >> beam.io.WriteToText(f'gs://{consumer_bucket_name}/{consumer_folder_path}/PROCESSED/', file_name_suffix='.csv', shard_name_template='')
        )

        failed_records = (
            checked_files
            | 'Filter Failed Records' >> beam.Filter(check_fail_status)
            | 'Format CSV' >> beam.Map(lambda x: ','.join(x))
            | 'Write Failed Records' >> beam.io.WriteToText(f'gs://{consumer_bucket_name}/{consumer_folder_path}/ERROR/', file_name_suffix='_error.csv', shard_name_template='')
        )

if __name__ == "__main__":
    project_id = 'tnt01-odycda-bld-01-1681'
    raw_zone_bucket_name = 'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181'
    raw_zone_folder_path = 'thParty/GFV/update'
    consumer_bucket_name = 'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181'
    consumer_folder_path = 'thParty/GFV/check'

    run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path)
