import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import json
import datetime


class ReadFromCloudSQLAndFormat(beam.DoFn):
    def process(self, element):
        # `element` is a tuple from JDBC: (id, timestamp, api_name, payload)
        id, timestamp, api_name, payload = element

        # Convert to dict for BigQuery insert
        yield {
            "id": id,
            "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime.datetime) else timestamp,
            "api_name": api_name,
            "payload": payload  # Can be string or JSON-encoded string
        }


def run(argv=None):
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Change to DirectRunner for local testing
        project='your-gcp-project-id',
        temp_location='gs://your-temp-bucket/temp/',
        region='your-region',
        job_name='cloudsql-to-bq-json-pipeline',
        save_main_session=True,
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from CloudSQL" >> beam.io.ReadFromJdbc(
                table_name='(SELECT id, timestamp, api_name, payload::text FROM audit_logs) AS tmp',
                driver_class_name='org.postgresql.Driver',
                jdbc_url='jdbc:postgresql:///<your-cloudsql-instance>?user=<username>&password=<password>',
                username='<username>',
                password='<password>',
                query=None,
                fetch_size=1000,
                row_mapper=lambda row: row  # returns tuple
            )
            | "Format for BQ" >> beam.ParDo(ReadFromCloudSQLAndFormat())
            | "Write to BQ" >> beam.io.WriteToBigQuery(
                table='your-project-id:your_dataset.audit_logs',
                schema='id:INTEGER,timestamp:TIMESTAMP,api_name:STRING,payload:JSON',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                custom_gcs_temp_location='gs://your-temp-bucket/temp/'
            )
        )


if __name__ == "__main__":
    run()