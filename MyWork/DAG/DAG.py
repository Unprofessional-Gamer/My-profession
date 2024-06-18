from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pendulum

# Import your Python scripts
from pricing_gfv.zipping_cap import zip_and_transfer_csv_files, run_pipeline
from pricing_gfv.raw_backup import move_files_with_prefixes
from pricing_gfv.unzip_gfv import unzipping
from pricing_gfv.dataquality_check import dataquality_pipeline
from pricing_gfv.cleaning_dq import cleaning_pipeline
from pricing_gfv.DP_backup import dp_pipeline

# Define constants and default arguments
local_tz = pendulum.timezone('Europe/Lisbon')
project_id = "tnt01-odycda-bld-01-1681"
gce_zone = "europe-west2"
today = datetime.now().strftime("%Y-%m-%d")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
with DAG(
    dag_id='GfvZipping',
    default_args=default_args,
    description='Zipping the CAP files and moving GFV files',
    schedule_interval=None,  # No schedule, triggered by sensors
    max_active_runs=1,
    catchup=False,
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    # Sensor to check for file existence in the first folder
    wait_for_files_folder1 = GCSObjectExistenceSensor(
        task_id='wait_for_files_folder1',
        bucket='tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181',
        object='INTERNAL/MFVS/GFV/DAILY/RECEIVED/*',
        google_cloud_conn_id='google_cloud_default',
        timeout=600,
        poke_interval=30,
        mode='poke'
    )

    # Sensor to check for file existence in the second folder
    wait_for_files_folder2 = GCSObjectExistenceSensor(
        task_id='wait_for_files_folder2',
        bucket='tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181',
        object='INTERNAL/MFVS/CAP/DAILY/RECEIVED/*',
        google_cloud_conn_id='google_cloud_default',
        timeout=600,
        poke_interval=30,
        mode='poke'
    )

    # EmptyOperator to join the sensors with any_success trigger rule
    join_sensors = EmptyOperator(
        task_id='join_sensors',
        trigger_rule='any_success',
    )

    unzipping_job = PythonOperator(
        task_id="unzipping_job",
        python_callable=unzipping,
        op_args=[
            'tnt01-odycda-bld-01-1681', 
            'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181', 
            'INTERNAL/MFVS/GFV/DAILY/RECEIVED', 
            'INTERNAL/MFVS/CAP/DAILY/RECEIVED', 
            'thParty/MFVS/GFV/Extract'
        ],
    )

    dataquality_job = PythonOperator(
        task_id="dataquality_job",
        python_callable=dataquality_pipeline,
        op_args=[
            'tnt01-odycda-bld-01-1681', 
            'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181', 
            'thParty/MFVS/GFV/Extract', 
            'thParty/MFVS/GFV/Received'
        ],
    )

    moving_files_archive_job = PythonOperator(
        task_id="moving_files_archive_job",
        python_callable=move_files_with_prefixes,
        op_args=[
            'tnt01-odycda-bld-01-1681', 
            'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181', 
            'thParty/MFVS/GFV/Daily/processed', 
            'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181', 
            'INTERNAL/MFVS/GFV/DAILY', 
            'INTERNAL/MFVS/CAP/DAILY', 
            ["CPRNEW", "CDENEW", "CPRVAL", "CDEVAL", "LDENEW", "LPRNEW", "LDEVAL", "LPRVAL"], 
            ["CPRRVU", "CPRRVN"]
        ],
    )

    dp_cleaning_job = PythonOperator(
        task_id="dp_cleaning_job",
        python_callable=cleaning_pipeline,
        op_args=[
            'tnt01-odycda-bld-01-1681', 
            'tnt01-odycda-bld-01-stb-eu-certzone-3067f5f0', 
            'thParty/MFVS/GFV/Received'
        ],
    )

    end = EmptyOperator(
        task_id='end',
    )

    # Set task dependencies
    start >> [wait_for_files_folder1, wait_for_files_folder2] >> join_sensors >> unzipping_job >> dataquality_job >> moving_files_archive_job >> dp_cleaning_job >> end
