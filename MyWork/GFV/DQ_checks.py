import pandas as pd
from google.cloud import storage
import checks_config as cfg
import apache_beam as beam
from datetime import datetime
import logging

logging.basicConfig(filename='/home/appuser/clean.log', encoding='utf-8', level=logging.INFO, format = '%(asctime)s:%(levelname)s:%(message)s')

class removal_double_quotes(beam.DoFn):
    def process(self, element):
        custom=[ele.replace('"','')  for ele in element]
        return [custom]
    
class Filterfn(beam.DoFn):
    def process(self, element):
        custom=[ele for ele in element]
        updated_custom=[' 'if ele in ['null','None','Nan',"NONE",'Null','n/a', 'N/A',''] else ele for ele in element]
        return [updated_custom]
    
class Unfilterfn(beam.DoFn):
    def process(self, element):
        custom=[ele for ele in element]
        removetable=str.maketrans('','','#$%^!&+*')
        updated_custom=[ele.translate(removetable) for ele in custom]
        return [updated_custom]
    
class Dataquality:
    def __init__(self,data,name,dataset):
        self.data=data
        self.name=name
        self.dataset=dataset

    def activate_data_cleaning(self,bucket_name,base_path):

        keys = self.data.columns.to_list()
        result_keys= ','.join(keys)

        argv = [
            '--project=tnt01-odycda-bld-01-0b81',
            '--region=europe-west-2',
            '--runner=DataflowRunner',
            '--staging_location=gs://tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181/External/MFVS/Pricing/Files/2023-03-20/Proccessed/stage',
            '--temp_location=gs://tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181/External/MFVS/Pricing/Files/2023-03-20/Proccessed/temp'
        ]
        with beam.Pipeline(argv=argv) as pipe:
            file_read=(pipe
                   |'Read data from bucket' >> beam.io.ReadFromText(f'gs://{bucket_name}/{base_path}/Proccessed/{self.name}',skip_header_lines=True)
                   |'Removing Duplicates from File data' >> beam.Distinct()
                   )
        
        processed_file_read=(file_read
                             |"splitting the input data into computational units">>beam.Map(lambda x:x.split(','))
                             |"Making the data into standard iteratable units">>beam.ParDo(removal_double_quotes())
                             |"Removing Null values in the file data">>beam.ParDo(Filterfn)
                             |"Removing the unwanted characters in the files data">>beam.ParDo(Unfilterfn())
                             )
        
        Output=(processed_file_read
                               |"Formatting as csv outputt format">>beam.Map(lambda x:','.join(x))
                               |beam.Map(print)
                               |"Writing to certified zone bucket">>beam.io.WriteToText(f'gs://tnt01-odycda-bld-01-stb-eu-certzone-3067f5f0/{base_path}Proccessed/{self.name[-12:-7]}/{self.name[:-4]}',file_name_suffix=".csv",num_shards=1,shard_name_template='',header=f'{result_keys}')
                               )
        
    def unprocessed(self,bucket_name,base_path):
        storage_client=storage.Client("tnt01-odycda-bld-01-0b81")
        logging.error(f"Pushing the file to error folder {self.name}")
        s_bucket = storage_client.bucket(bucket_name)
        blob = s_bucket.blob(f"{base_path}{bucket_name}")
        new_blob=s_bucket.copy_blob(blob,s_bucket,f"{base_path}ERROR/{self.name}")
        logging.error("Pushing the {self.name} to the error folder")

    def processed(self,bucket_name,base_path):
        storage_client=storage.Client("tnt01-odycda-bld-01-0b81")
        logging.info(f"Pushing the file to Proccessed folder {self.name}")
        s_bucket = storage_client.bucket(bucket_name)
        blob = s_bucket.blob(f"{base_path}{bucket_name}")
        new_blob=s_bucket.copy_blob(blob,s_bucket,f"{base_path}Processed/{self.name}")
        logging.error("Pushing the {self.name} to the Processed folder")

    def null_check(self):
        logging.info(f"Null check process started for file: {self.name}")
        if self.data.isnull().all():
            logging.error(f" found {self.name} do not have any values")
            return False
        else:
            null_values=self.data.isnull().sum()
            logging.info(null_values)
            return True

    def volume_check(self):
        logging.info(f"volume check started for {self.name}")
        if(len(self.data)==0):
            logging.error(f"File : {self.name} is empty")
            return False
        if len(self.data)<cfg.min_volume:
            logging.warn(f"File didn't pass volume check : {self.name}")
            logging.error(f"Received the failed on volume check {self.name}")
            return False
        elif len(self.data)>cfg.max_volume:
            logging.warn(f"File exceeds max volume check : {self.name}")
            logging.error(f"Received the failed on volume check {self.name}")
            return False
        else:
            logging.info(f"Volume passed for {self.name}")
            return True
        
    def schema_check(self):
        logging.warn(f"schema check started : {self.name}")
        logging.error(f"checking Received file {self.name} with config file")

        if self.name not in cfg.Book_Map[self.dataset]['files']:
            logging.error(f"The File Name Mismatch Error : Config and Data doesn't match {self.name}")
            return False
    
        expected_column=cfg.Book_Map[self.dataset]['schema']
        if set(self.data.colums.tolist())==set(expected_column):
            logging.info(f"Schema Passes For {self.name}")
            logging.info(f"Schema expected: {expected_column}")
            logging.info(f"Schema received: {self.data.columns.tolist()}")
            return True
        else:
            logging.error(f"Schema check failed for {self.name}")
            logging.error("Expected Schema: {expected_column}")
            logging.error(f"Recieved Schema: {self.data.columns.tolist()} ")
            logging.error(f"Missing columns are: {list(set(self.data.columns.to_list())-set(expected_column))}") 
            return False
    
def start_data_lister():
    bucket_name="tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181"
    client=storage.Client("tnt01-odycda-bld-01-0b81")
    base_path="thParty/MFVS?GFV/Monthly"
    blobs = client.list_blobs(bucket_name, prefix=base_path)

    for blob in blobs:
        try:
            if blob.name.endswith('.csv'):
                file_value=blob.name
                filename=file_value.spilt("/"[-1])
                dataset = filename[:6]
                if dataset in cfg.Book_Map:
                    df1=pd.read_csv(f'gs://{bucket_name}/{blob.name}', skiprows=1, names=cfg.Book_Map[dataset]['schema'])
                    logging.info(f"Quality checks for {filename}")
                    Dataquality
                    checker=Dataquality(df1,filename,dataset)
                    if(checker.volume_check() and checker.schema_check() and checker.null_check()):
                        logging.info(f"volume check passed {filename}")
                        logging.info(f"{filename} Passed all checks moving {filename} to processed folder.")
                        checker.processed(bucket_name,base_path)
                        logging.info(f"Data cleaning started {filename}")
                        checker.activate_data_cleaning(bucket_name,base_path)
                        logging.info(f"Data cleaning completed {filename}")
                    else:
                        logging.error(f"Volume Check Failed for {filename}")
                        checker.unprocessed(bucket_name,base_path,dataset)
                else:
                    logging.error(f"Dataset {dataset} not found in config file")
            else:
                logging.error(f"File type not supported")
        except Exception as e :
                logging.error(f"Error occured while processing the data {filename}")
                logging.error(f"(e) on the {filename}")
            
def read_log_files(gcs_path,log_path):
    print("Pushing log file to Raw zone")
    bucket_name="tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181"
    client = storage.Client()
    try:
        bucket = client.get_bucket(bucket_name)
        blob  = bucket.blob(gcs_path)
        blob.upload_from_filename(log_path)
        logging.info(f'logging added to {bucket_name}/{gcs_path}')
    except Exception as e:
        logging.exception(f'An error occurred while uploading a file to {bucket_name},{e}')
    return("Pushed log files to Raw zone")

if  __name__ == "__main__":
    print("starting the Data lister")
    start_data_lister()
    print("Data lister completed")

    load_date = datetime.today().strftime('%Y-%m-%d-%H: %M:%S')
    gcs_path = f'thParty/MFVS/GFV/Monthly/logs/{load_date}/GFV_clean.log'
    log_path ='/home/appuser/clean.log'
    print("Reading logs")
    read_log_files(gcs_path,log_path)
    print("Logs pushed to Raw")
                              

                                    