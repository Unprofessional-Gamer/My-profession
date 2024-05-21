import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from google.cloud import bigquery
from google.oauth2 import service_account

def run(project_id, bq_dataset, bucket_name, base_path, file_name):
    # Define your pipeline options
    options = PipelineOptions(
        project=project_id,
        runner="DataflowRunner",  # Use "DirectRunner" for local testing
        temp_location='gs://tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181/thParty/MFVS/GFV/Monthly/dataflow/temp',
        region='europe-west2',
        staging_location='gs://tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181/thParty/MFVS/GFV/Monthly/staging',
        service_account_email='svc-dfl-user@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',
        subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88h/regions/europe-west2/subnetworks/odycda-csn-euwe2-kc1-01-bld-01',
        num_workers=1,
        max_num_workers=4,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )

    p = beam.Pipeline(options=options)

    # Define the queries for extracting the specified columns
    queries = {
        'table1': f'SELECT colum2 AS merged_column FROM `{project_id}.{bq_dataset}.table1`',
        'table2': f'SELECT colum4 AS merged_column FROM `{project_id}.{bq_dataset}.table2`',
        'table3': f'SELECT colum6 AS merged_column FROM `{project_id}.{bq_dataset}.table3`',
        'table4': f'SELECT colum2 AS merged_column FROM `{project_id}.{bq_dataset}.table4`'
    }

    # Function to read from BigQuery
    def read_from_bq(query):
        return (p
                | f"Read {query}" >> beam.io.Read(beam.io.BigQuerySource(query=query))
                | beam.Map(lambda row: row['merged_column']))

    # Read and merge data from all tables
    merged_data = (
        [read_from_bq(query) for query in queries.values()]
        | "Merge PCollections" >> beam.Flatten()
    )

    # Write merged data to BigQuery
    merged_data | 'Write to BQ' >> beam.io.WriteToBigQuery(
        f'{project_id}:{bq_dataset}.table5',
        schema='merged_column:STRING',
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    project_id = 'your_project_id'
    bq_dataset = 'your_dataset'
    bucket_name = 'your_bucket_name'
    base_path = 'your_base_path'
    file_name = 'your_file_name'
    run(project_id, bq_dataset, bucket_name, base_path, file_name)

##############################################################################################################################################3
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery

def run(project_id, bq_dataset, bucket_name, base_path, file_name):
    # BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Define the BigQuery table references and columns
    table_references = {
        'table1': {'table_id': 'table1', 'column': 'column2'},
        'table2': {'table_id': 'table2', 'column': 'column4'},
        'table3': {'table_id': 'table3', 'column': 'column6'},
        'table4': {'table_id': 'table4', 'column': 'column2'}
    }
    
    # Pipeline options
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project=project_id,
        temp_location=f"gs://{bucket_name}/temp",
        region='europe-west2',
        staging_location=f"gs://{bucket_name}/staging",
        service_account_email='svc-dfl-user@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',
        dataflow_kms_key='projects/tnt01-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptoKeys/keyhsm-kms-tnt01-euwe2-cdp',
        subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88h/regions/europe-west2/subnetworks/odycda-csn-euwe2-kc1-01-bld-01',
        num_workers=1,
        max_num_workers=4,
        use_public_ips=False,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )
    
    # Pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        merged_data = (
            p
            | 'Read from BigQuery' >> beam.Create(table_references.items())
            | 'Query BigQuery' >> beam.Map(lambda x: query_bigquery(client, x))
            | 'Merge Columns' >> beam.Map(merge_columns)
        )

        # Write merged data to BigQuery
        merged_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table=f"{project_id}.{bq_dataset}.table5",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            schema='merged_column:STRING'
        )

def query_bigquery(client, table_info):
    table_id = table_info['table_id']
    column = table_info['column']
    
    query = f"SELECT {column} FROM `{client.project}.{bq_dataset}.{table_id}`"
    query_job = client.query(query)
    results = query_job.result()
    
    return [row[column] for row in results]

def merge_columns(data):
    return {'merged_column': ','.join(str(item) for item in data)}

if __name__ == '__main__':
    project_id = 'your-project-id'
    bq_dataset = 'your-dataset-name'
    bucket_name = 'your-bucket-name'
    base_path = 'your-base-path'
    file_name = 'your-file-name'
    
    run(project_id, bq_dataset, bucket_name, base_path, file_name)
