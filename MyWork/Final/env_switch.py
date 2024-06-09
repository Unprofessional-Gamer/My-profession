from google.cloud import storage

def list_blobs_with_prefix(bucket_name, prefix, source_credentials):
    """Lists all the blobs in the bucket that begin with the prefix."""
    storage_client = storage.Client.from_service_account_json(source_credentials)
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)

    return blobs

def copy_blob(source_bucket_name, source_blob_name, destination_bucket_name, destination_blob_name, source_credentials, destination_credentials):
    """Copies a blob from one bucket to another with a new name."""
    source_client = storage.Client.from_service_account_json(source_credentials)
    source_bucket = source_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    
    destination_client = storage.Client.from_service_account_json(destination_credentials)
    destination_bucket = destination_client.bucket(destination_bucket_name)
    
    destination_blob = destination_bucket.blob(destination_blob_name)
    
    # Copy the blob to the destination bucket
    destination_blob.rewrite(source_blob)

def main():
    source_bucket_name = 'source-bucket-name'
    destination_bucket_name = 'destination-bucket-name'
    source_folder = 'source-folder/'
    destination_folder = 'destination-folder/'
    source_credentials = 'path/to/source/credentials.json'
    destination_credentials = 'path/to/destination/credentials.json'

    # List all files in the source folder
    blobs = list_blobs_with_prefix(source_bucket_name, source_folder, source_credentials)
    
    for blob in blobs:
        source_blob_name = blob.name
        # Remove the source folder prefix from the source blob name to get the file name
        file_name = source_blob_name.replace(source_folder, '', 1)
        destination_blob_name = f"{destination_folder}{file_name}"
        
        copy_blob(source_bucket_name, source_blob_name, destination_bucket_name, destination_blob_name, source_credentials, destination_credentials)
        print(f"Copied {source_blob_name} to {destination_blob_name}")

if __name__ == '__main__':
    main()
