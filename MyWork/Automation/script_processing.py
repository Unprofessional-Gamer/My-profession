import time
import datetime
import subprocess
import os
import shutil
from git import Repo  # Make sure to install the 'GitPython' package using pip
from google.cloud import storage

# Function to clone the Git repository
def clone_repository(repo_url, destination_dir):
    if os.path.exists(destination_dir):
        shutil.rmtree(destination_dir)
    Repo.clone_from(repo_url, destination_dir)

def monitor_bucket(bucket_name, folder_path):
    checking_number = 1  # Initialize checking number
    print(f"Monitoring folder '{folder_path}' in bucket '{bucket_name}' for new files...")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # Keep track of files seen so far
    seen_files = set()

    # Replace 'git_repo_url' with the URL of your Git repository
    git_repo_url = 'https://github.com/your_username/your_repo.git'
    # Replace 'temp_repo_dir' with the directory where you want to clone the repository
    temp_repo_dir = 'temp_repo'

    # Clone the Git repository
    clone_repository(git_repo_url, temp_repo_dir)

    while True:
        print(f"Checking #{checking_number} - {datetime.datetime.now()}")  # Print checking number and current date/time
        blobs = bucket.list_blobs(prefix=folder_path)
        for blob in blobs:
            if blob.name not in seen_files:
                print(f"New file detected: {blob.name}")
                # Handle the new file here, e.g., process it, move it, etc.
                seen_files.add(blob.name)
                # Run other scripts
                run_script1()
                run_script2()

        # Increment checking number
        checking_number += 1
        
        # Sleep for a certain interval before checking again
        time.sleep(60)  # Poll every 60 seconds

# Function to run script1.py
def run_script1():
    # Replace 'python script1.py' with the command to run your first script
    subprocess.run(['python', 'temp_repo/script1.py'])

# Function to run script2.py
def run_script2():
    # Replace 'python script2.py' with the command to run your second script
    subprocess.run(['python', 'temp_repo/script2.py'])

def main():
    # Replace 'raw-zone-bucket' and 'folder/path' with your bucket name and folder path
    monitor_bucket('raw-zone-bucket', 'folder/path')

if __name__ == "__main__":
    main()
