import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems

class DataQualityChecks(beam.DoFn):
    """Perform data quality checks on each row of the CSV."""
    
    def process(self, element):
        row, filename = element
        columns = row.split(',')
        errors = []

        # Null value check
        if any(col.strip() == '' for col in columns):
            errors.append('Null value found')

        # Special character check (allowed special characters are '1234567890-=+')
        special_char_pattern = re.compile(r'[^\w\s!@#$%^&*()-=+]')
        if any(special_char_pattern.search(col) for col in columns):
            errors.append('Special character found')

        if errors:
            yield beam.pvalue.TaggedOutput('error', (row, filename))
        else:
            yield (row, filename)

def run_pipeline(project_id, raw_zone_bucket_name, raw_zone_folder_path, consumer_bucket_name, consumer_folder_path):
    """Run the Beam pipeline to perform data quality checks."""
    
    options = PipelineOptions(
        project=project_id,
        runner="DataflowRunner",
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
            | 'List Files' >> beam.io.MatchFiles(f'gs://{raw_zone_bucket_name}/{raw_zone_folder_path}/*.csv')
            | 'Read Matches' >> beam.io.ReadMatches()
            | 'Extract File Path' >> beam.Map(lambda x: x.metadata.path)
        )

        rows = (
            raw_files
            | 'Read Files' >> beam.FlatMap(read_file_lines)
        )

        processed, errors = (
            rows
            | 'Data Quality Checks' >> beam.ParDo(DataQualityChecks()).with_outputs('error', main='main')
        )

        write_results(processed, consumer_bucket_name, consumer_folder_path, 'Processed')
        write_results(errors, consumer_bucket_name, consumer_folder_path, 'Error')

def read_file_lines(file_path):
    """Read lines from a file in GCS."""
    with FileSystems.open(file_path) as f:
        for line in f:
            yield line.decode('utf-8').strip(), file_path

def write_results(results, bucket_name, folder_path, subfolder):
    """Write the results to GCS."""
    def get_output_path(element):
        row, file_path = element
        filename = file_path.split('/')[-1]
        return f'gs://{bucket_name}/{folder_path}/{subfolder}/{filename}'

    results | f'Write Results to {subfolder}' >> beam.MapTuple(lambda row, file_path: (row, get_output_path((row, file_path)))) | beam.GroupByKey() | beam.MapTuple(write_to_file)

def write_to_file(rows, output_path):
    """Write rows to a file in GCS."""
    with FileSystems.create(output_path) as f:
        for row in rows:
            f.write(f"{row}\n".encode('utf-8'))

if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', required=True)
    parser.add_argument('--raw_zone_bucket_name', required=True)
    parser.add_argument('--raw_zone_folder_path', required=True)
    parser.add_argument('--consumer_bucket_name', required=True)
    parser.add_argument('--consumer_folder_path', required=True)
    args = parser.parse_args()

    run_pipeline(
        project_id=args.project_id,
        raw_zone_bucket_name=args.raw_zone_bucket_name,
        raw_zone_folder_path=args.raw_zone_folder_path,
        consumer_bucket_name=args.consumer_bucket_name,
        consumer_folder_path=args.consumer_folder_path
    )
