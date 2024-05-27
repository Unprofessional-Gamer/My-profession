from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class RemoveDoubleQuotes(beam.DoFn):
    def process(self, element):
        custom = [ele.replace('"', '') for ele in element]
        return [custom]

class FilterNullValues(beam.DoFn):
    def process(self, element):
        updated_custom = [ele if ele not in ['null', None, 'NaN', 'NONE', 'Null', 'n/a'] else '' for ele in element]
        return [updated_custom]

class RemoveSpecialCharacters(beam.DoFn):
    def process(self, element):
        special_chars = '1234567890-=+'
        remove_table = str.maketrans("", "", special_chars)
        updated_custom = [ele.translate(remove_table) for ele in element]
        return [updated_custom]

class NullCheck(beam.DoFn):
    def process(self, element):
        null_list = [ele for ele in element if ele in ['null', None, 'NaN', 'NONE', 'Null', '']]
        if len(null_list) < len(element):
            element.append("Passed null check")
        else:
            element.append("Failed null check")
        yield element

class VolumeCheck(beam.DoFn):
    def process(self, element, vol_count):
        if 0 < vol_count < 1000:
            element.append("Passed volume check")
        else:
            element.append("Failed volume check")
        yield element

def check_pass_status(element):
    return all('Passed' in item for item in element[-2:])

def check_fail_status(element):
    return any('Failed' in item for item in element[-2:])

def run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path):
    options = PipelineOptions(
        project=project_id,
        runner='DataflowRunner',
        temp_location=f'gs://{raw_zone_bucket_name}/temp',
        region='europe-west2',
        staging_location=f'gs://{raw_zone_bucket_name}/staging',
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
        raw_files = (
            p
            | 'List Files' >> beam.io.fileio.MatchFiles(f'gs://{raw_zone_bucket_name}/{raw_zone_folder_path}/*.csv')
            | 'Read Matches' >> beam.io.fileio.ReadMatches()
            | 'Read Files' >> beam.io.ReadAllFromText()
        )

        # Data Quality Checks
        rows = (
            raw_files
            | 'Split Rows' >> beam.Map(lambda x: x.split(','))
            | 'Remove Double Quotes' >> beam.ParDo(RemoveDoubleQuotes())
            | 'Filter Null Values' >> beam.ParDo(FilterNullValues())
            | 'Remove Special Characters' >> beam.ParDo(RemoveSpecialCharacters())
        )

        volume_count = (
            rows
            | 'Count Rows' >> beam.combiners.Count.Globally()
        )

        checked_rows = (
            rows
            | 'Null Check' >> beam.ParDo(NullCheck())
            | 'Volume Check' >> beam.ParDo(VolumeCheck(), vol_count=beam.pvalue.AsSingleton(volume_count))
        )

        passed_rows = (
            checked_rows
            | 'Filter Passed Rows' >> beam.Filter(check_pass_status)
            | 'Format as CSV' >> beam.Map(lambda x: ','.join(x[:-2]))
        )

        failed_rows = (
            checked_rows
            | 'Filter Failed Rows' >> beam.Filter(check_fail_status)
            | 'Format as CSV' >> beam.Map(lambda x: ','.join(x[:-2]))
        )

        # Write the results to GCS
        passed_rows | 'Write Passed Rows' >> beam.io.WriteToText(
            f'gs://{consumer_bucket_name}/{consumer_folder_path}/Processed/processed_file',
            file_name_suffix='.csv',
            num_shards=1,
            shard_name_template=''
        )

        failed_rows | 'Write Failed Rows' >> beam.io.WriteToText(
            f'gs://{consumer_bucket_name}/{consumer_folder_path}/Error/error_file',
            file_name_suffix='.csv',
            num_shards=1,
            shard_name_template=''
        )

if __name__ == "__main__":
    project_id = 'tnt01-odycda-bld-01-1681'
    raw_zone_bucket_name = 'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181'
    raw_zone_folder_path = 'thParty/GFV/update'
    consumer_bucket_name = 'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181'
    consumer_folder_path = 'thParty/GFV/check'

    print("Starting Data Quality Check Pipeline...")
    print(f"Project ID: {project_id}")
    print(f"Raw Zone Bucket: {raw_zone_bucket_name}/{raw_zone_folder_path}")
    print(f"Consumer Bucket: {consumer_bucket_name}/{consumer_folder_path}")

    run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path)
