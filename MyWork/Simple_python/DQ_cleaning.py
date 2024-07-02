from google.cloud import storage

class GCSFileDeleter:
    def __init__(self, project_id, bucket_name, folder_path):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.folder_path = folder_path
        self.client = storage.Client(project=self.project_id)
        self.bucket = self.client.bucket(self.bucket_name)

    def list_files(self):
        blobs = self.bucket.list_blobs(prefix=self.folder_path)
        return [blob.name for blob in blobs]

    def delete_files(self):
        files_to_delete = self.list_files()
        for file_name in files_to_delete:
            blob = self.bucket.blob(file_name)
            blob.delete()
            print(f"Deleted: {file_name}")

if __name__ == "__main__":
    project_id = 'tenu-wiue2-k'
    raw_zone_bucket = "raw_zone_bucket"
    raw_zone_folder_path = 'raw_zone_folder_path'

    deleter = GCSFileDeleter(project_id, raw_zone_bucket, raw_zone_folder_path)
    deleter.delete_files()
