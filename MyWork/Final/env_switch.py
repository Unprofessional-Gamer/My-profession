import apache_beam as beam
from apache_beam.io import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage

class CopyFilesToBuildEnv(beam.DoFn):
    def __init__(self, source_bucket_name, destination_bucket_name, source_folder, destination_folder):
        self.source_bucket_name = source_bucket_name
        self.destination_bucket_name = destination_bucket_name
        self.source_folder = source_folder
        self.destination_folder = destination_folder

    def process(self, element):
        source_blob_name = element.path[len(f'gs://{self.source_bucket_name}/'):]
        destination_blob_name = f'{self.destination_folder}/{source_blob_name.replace(self.source_folder + '/', "", 1)}'

        source_bucket = storage.Client().bucket(self.source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)
        
        destination_bucket = storage.Client().bucket(self.destination_bucket_name)
        source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)

        result = f"Copied {source_blob_name} to {destination_blob_name}"
        print(result)
        yield result

def main():
    source_project_id = 'integration-env-project-id'
    source_bucket_name = 'integration-env-bucket-name'
    destination_project_id = 'build-env-project-id'
    destination_bucket_name = 'build-env-bucket-name'
    source_folder = 'source-folder'
    destination_folder = 'destination-folder'

    options = PipelineOptions(
        project=source_project_id,
        runner="DirectRunner"  # Change to DataflowRunner for production use
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'List Files' >> beam.Create([f'gs://{source_bucket_name}/{source_folder}/**'])
            | 'Match Files' >> beam.FlatMap(lambda pattern: FileSystems.match([pattern])[0].metadata_list)
            | 'Copy Files to Build Env' >> beam.ParDo(CopyFilesToBuildEnv(
                source_bucket_name, destination_bucket_name, source_folder, destination_folder))
        )

if __name__ == '__main__':
    print("*************************************Movement started********************************************")
    main()
    print("*************************************Movement Finished********************************************")
