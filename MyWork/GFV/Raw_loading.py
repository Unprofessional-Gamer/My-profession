import os
from google.cloud import storage
import logging
import datetime

logging.basicConfig(filename='/home/appuser/clean.log', encoding='utf-8', level=logging.INFO, format = '%(asctime)s:%(levelname)s:%(message)s')

storage_client = storage.Client("tnt01-odycda-bld-01-1b81")

def data_upload_to_gcs(bucket_name, destination_file, source_file):
    file_type = source_file.split('-')[-1]
    if file_type == "csv":
        content_type = "text/csv"
    try:
        logging.info(" Inside Try ")
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_file)
        blob.upload_from_filename(source_file, content_type)
        logging.info(f" {destination_file} Uploaded Successfully ")
    except Exception as e:
        logging.error(f" {destination_file} Upload Failed {e} ")

def start_data_loading():
    bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181"
    base_path = "thParty/MFVS/GFV/Monthly/"
    data_dir = "/home/appuser/GFV/dummy_data"
    for subdir,dirs, files in os.walk(data_dir):
        for file in files:
            logging.info(f"Loading {file} into Raw zone ")
            filepath = os.path.join(subdir, file)
            data_upload_to_gcs(bucket_name, base_path+file, filepath)
            logging.info(f"Loading {file} into Raw zone Completed ")
    return ("file loading finished")


def read_log_file(gcs_path, log_path):
    print("************ pushing the log file to the Raw Zone bucket***************")
    bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181"
    client = storage.Client()
    try:
        bucket = client.get_bucket(bucket_name)
        blob= bucket.blob(gcs_path)
        blob.upload_from_filename(log_path)
        logging.info(f'logging added to {bucket_name}/{gcs_path}')
    except Exception as e:
        logging.error(f'error uploading logs to gcs bucket {bucket_name}: {e}') 
    return("************ pushed the log file to the Raw zone bucket done***************")

if __name__== "__main__":
    print("Data_loding initiated to Raw zone")
    logging.info("Data_loding initiated to Raw zone")
    start_data_loading()
    print("Data loading has been successfull")
    logging.info("Data_loding succesfull")
    Load_Date = datetime.today().strftime('%Y-%m-%d-%H:%M')
    gcs_path=f'thParty/MFVS/GFV/Monthly/logs/(Load_Date)/GFV_clean.log'
    log_path='/home/appuser/clean.log'
    print("************ Reading the logs started***************")
    read_log_file(gcs_path, log_path)
    print("************ Logs pushed to GCS Rawzone***************")