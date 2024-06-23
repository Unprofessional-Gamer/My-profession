import re
import csv
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from io import BytesIO, StringIO
import pandas as pd

# Replace with your credentials and bucket information
bucket_name = 'your-bucket-name'
raw_folder_path = 'data/raw/'
output_csv_path = 'output/consolidated_schemas.csv'

# Regular expression to match folder names like YYYYMMDD-foldername-csv/
folder_regex = re.compile(r'^\d{8}-([^/]+)-csv/$')

# Function to list folders matching the pattern
def list_folders(bucket, prefix):
    folders = []
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        if blob.name.endswith('/'):
            match = folder_regex.match(blob.name)
            if match:
                folders.append(match.group(1))
    return folders

# Function to list schema files in a folder
def list_schema_files(bucket, folder):
    folder_prefix = f"{raw_folder_path}{folder}-csv/"
    schema_files = []
    blobs = bucket.list_blobs(prefix=folder_prefix)
    for blob in blobs:
        if blob.name.endswith('.schema.csv'):
            schema_files.append(blob.name)
    return schema_files

# Function to read schema file and return rows with foldername and filename prefix
def read_schema_file(file_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_string()
    df = pd.read_csv(BytesIO(content))
    
    foldername = file_path.split('/')[1].split('-')[1]
    filename_prefix = file_path.split('/')[-1].split('.')[0]
    
    df.insert(0, 'filename', filename_prefix)
    df.insert(0, 'foldername', foldername)
    
    output = StringIO()
    df.to_csv(output, index=False, header=False)
    return output.getvalue().strip().split('\n')

# Beam Pipeline
class GCSOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--bucket_name', type=str, help='GCS Bucket Name')
        parser.add_value_provider_argument('--raw_folder_path', type=str, help='GCS Raw Folder Path')
        parser.add_value_provider_argument('--output_csv_path', type=str, help='Output CSV Path')

def run():
    options = PipelineOptions()
    gcs_options = options.view_as(GCSOptions)
    
    with beam.Pipeline(options=options) as p:
        # Get the list of folders
        folders = p | 'List Folders' >> beam.Create(list_folders(storage.Client().bucket(bucket_name), raw_folder_path))
        
        # Get the list of schema files
        schema_files = (
            folders
            | 'List Schema Files' >> beam.FlatMap(lambda folder: list_schema_files(storage.Client().bucket(bucket_name), folder))
        )
        
        # Read schema files and consolidate data
        rows = (
            schema_files
            | 'Read Schema Files' >> beam.FlatMap(read_schema_file)
        )
        
        # Write to output CSV
        rows | 'Write to CSV' >> beam.io.WriteToText(output_csv_path, file_name_suffix='.csv', shard_name_template='')

if __name__ == '__main__':
    run()
