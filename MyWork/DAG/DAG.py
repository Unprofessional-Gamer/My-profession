from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GoogleCloudStoragePrefixSensor
import pendulum

# Importing custom functions from your project
from DAG.zipping import copy_and_transfer_csv, zip_and_transfer_csv_files,delete_files
from DAG.unzipping import run_pipeline

# Setting up timezone and project variables
local_tz = pendulum.timezone('Europe/Lisbon')
project_id = "tnt01-odycda-bld-01-1b81"
gce_zone = "europe-west2"
bucket_name = "tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181"
prefix = "INTERNAL/MFVS/GFV/DAILY/RECEIVED/"
today = datetime.now().strftime("%Y-%m-%d")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Defining the DAG
with DAG(
    dag_id='GfxZipping',
    default_args=default_args,
    description="Zipping the CAP files and moving GFV files",
    schedule_interval=None,  # Triggered by sensor, no schedule
    max_active_runs=1,
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start', dag=dag)
    
    date_folder = datetime.now().strftime('%m-%Y')

    # Sensor to check for new files in the GCS bucket
    wait_for_files = GoogleCloudStoragePrefixSensor(
        task_id="wait_for_files",
        bucket=bucket_name,
        prefix=prefix,
        timeout=600,  # Wait for up to 10 minutes
        poke_interval=60,  # Check every 60 seconds
        mode='poke',
        dag=dag,
    )
    
    unzipping_job = PythonOperator(
        task_id="unzipping_job",
        python_callable=run_pipeline,
        op_args=[
            project_id,
            bucket_name,'INTERNAL/MFVS/GFV/DAILY/RECEIVED','INTERNAL/MFVS/CAP/DAILY/RECEIVED','thParty/MFVS/GFV/EXTRACT'],
        dag=dag,
    )

    moving_gfvfiles_job = PythonOperator(
        task_id="moving_gfvfiles_job",
        python_callable=copy_and_transfer_csv,
        op_args=[
            project_id,
            bucket_name,'thParty/MFVS/GFV/EXTRACT','tnt01-odycda-bld-01-stb-eu-tdip-consumer-78a45ff3','thParty/GFV/Monthly/SEGDrop',f'thParty/MFVS/GFV/Monthly/{date_folder}/ARCHIEVE',{"CPRRVU", "CPRRVN", "LPRRVU", "LPRRVN"}],
        dag=dag,
    )

    zipfiles_job = PythonOperator(
        task_id="zipfiles_job",
        python_callable=zip_and_transfer_csv_files,
        op_args=[
            project_id,
            bucket_name,
            'thParty/MFVS/GFV/EXTRACT','tnt01-odycda-bld-01-stb-eu-tdip-consumer-78a45ff3','thParty/GFV/Monthly/SEGDrop',f'thParty/MFVS/GFV/Monthly/{date_folder}/ARCHIEVE'],
        dag=dag,
    )

    deleting_job = PythonOperator(
        task_id="sourcefiles_deleting_job",
        python_callable=run_pipeline,
        op_args=[
            project_id,bucket_name,'thParty/MFVS/GFV/EXTRACT'],
        dag=dag,
    )

    end = EmptyOperator(task_id='end', dag=dag)

    # Setting task dependencies
    start >> wait_for_files >> unzipping_job >> moving_gfvfiles_job >> zipfiles_job >> deleting_job >> end
