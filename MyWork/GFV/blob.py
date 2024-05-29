from google.cloud import storage
from datetime import datetime

prefixes = [
    "CPRNEW",
    "CDENEW",
    "CPRVAL",
    "CDEVAL",
    "LDENEW",
    "LPRNEW",
    "LDEVAL",
    "LPRVAL",
    "CPRRVU",
    "CPRRVN",
]

MONTH_MAPPING = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec"
}

def move_files_with_prefixes(source_bucket_name, source_folder_path, destination_bucket_name, destination_folder_path, prefixes):
    # Initialize the GCS client
    client = storage.Client(project=project_id)

    # Get the source and destination buckets
    source_bucket = client.bucket(source_bucket_name)
    destination_bucket = client.bucket(destination_bucket_name)

    # Get the current year and month
    current_year = datetime.now().year
    current_month = datetime.now().strftime("%m")

    # Get the month name from the mapping
    month_name = MONTH_MAPPING[current_month]

    # Construct the full destination path
    full_destination_folder_path = f"{destination_folder_path}/{current_year}/{month_name}/Archive"

    # List blobs in the source bucket with the specified folder path
    blobs = client.list_blobs(source_bucket, prefix=source_folder_path)

    for blob in blobs:
        # Check if the blob's name starts with any of the specified prefixes
        if any(blob.name.startswith(f"{source_folder_path}/{prefix}") for prefix in prefixes):
            # Define the destination blob name
            destination_blob_name = f"{full_destination_folder_path}/{blob.name[len(source_folder_path)+1:]}"
            destination_blob = destination_bucket.blob(destination_blob_name)

            # Copy the blob to the destination bucket
            source_bucket.copy_blob(blob, destination_bucket, destination_blob_name)

            # Delete the blob from the source bucket
            blob.delete()

            print(f"Moved: {blob.name} to {destination_blob_name}")

if __name__ == "__main__":
    # Define your parameters
    raw_zone_bucket = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    raw_zone_folder_path = "thparty/MFVS/GFV/SFGDrop"
    DP_consumer_bucket = "tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a"
    consumer_folder_path = "thparty/MFVS/CAP"
    project_id = 'tnt01-odycda-bld-01-1b81'

    # Move the files
    print("**************** Files Moving Started  *****************")
    move_files_with_prefixes(raw_zone_bucket, raw_zone_folder_path, DP_consumer_bucket, consumer_folder_path, prefixes)
    print("**************** Files Moving Completed *****************")
