from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pendulum
import holidays

# Import your Python scripts
from pricing_gfv.zipping_cap import zip_and_transfer_csv_files, run_pipeline
from pricing_gfv.raw_backup import move_files_with_prefixes
from pricing_gfv.unzip_gfv import unzipping
from pricing_gfv.dataquality_check import dataquality_pipeline
from pricing_gfv.DP_backup import dp_pipeline
from pricing_gfv.cleaning_dq import cleaning_pipeline

# Define constants and default arguments
local_tz = pendulum.timezone('Europe/Lisbon')
project_id = "tnt01-odycda-bld-01-1681"
gce_zone = "europe-west2"
today = datetime.now().strftime("%Y-%m-%d")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Start date set to a past date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Define the list of holidays
HOLIDAYS = holidays.Portugal(years=datetime.now().year)

def is_last_working_day_of_month(execution_date, **kwargs):
    # Find the last day of the month
    last_day = execution_date.replace(day=1) + timedelta(days=32)
    last_day = last_day.replace(day=1) - timedelta(days=1)

    # Find the last working day
    while last_day.weekday() >= 5 or last_day in HOLIDAYS:
        last_day -= timedelta(days=1)

    # Check if today is the last working day of the month after the 27th
    if execution_date.day > 27 and execution_date == last_day:
        return 'start'
    else:
        return 'do_nothing'

# Define the DAG
with DAG(
    dag_id='GfvZipping',
    default_args=default_args,
    description='zipping the CAP files and moving GFV files',
    schedule_interval='0 13 27-31 * *',  # Runs every day at 1 PM from the 27th to the 31st
    max_active_runs=1,
    catchup=False,
) as dag:

    check_last_working_day = BranchPythonOperator(
        task_id='check_last_working_day',
        python_callable=is_last_working_day_of_month,
        provide_context=True
    )

    start = EmptyOperator(
        task_id='start'
    )

    do_nothing = EmptyOperator(
        task_id='do_nothing'
    )

    unzipping_job = PythonOperator(
        task_id="unzipping_job",
        python_callable=unzipping,
        op_args=[
            'tnt01-odycda-bld-01-1b81', 
            'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181', 
            'INTERNAL/MFVS/GFV/DAILY/RECEIVED', 
            'INTERNAL/MFVS/CAP/DAILY/RECEIVED', 
            'thParty/MFVS/GFV/Extract'
        ]
    )

    dataquality_job = PythonOperator(
        task_id="dataquality_job",
        python_callable=dataquality_pipeline,
        op_args=[
            'tnt01-odycda-bld-01-1681', 
            'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181', 
            'thParty/MFVS/GFV/Extract', 
            'thParty/MFVS/GFV/Da_eu-certzone-3067f5f0', 
            'thParty/MFVS/GFV/Received'
        ]
    )

    moving_files_archive_job = PythonOperator(
        task_id="moving_files_archive_job",
        python_callable=move_files_with_prefixes,
        op_args=[
            'tnt01-odycda-bld-01-1b81', 
            'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181', 
            'thParty/MFVS/GFV/Daily/processed', 
            'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181', 
            'INTERNAL/MFVS/GFV/DAILY', 
            'INTERNAL/MFVS/CAP/DAILY', 
            ["CPRNEW", "CDENEW", "CPRVAL", "CDEVAL", "LDENEW", "LPRNEW", "LDEVAL", "LPRVAL"], 
            ["CPRRVU", "CPRRVN"]
        ]
    )

    dp_cleaning_job = PythonOperator(
        task_id="dp_cleaning_job",
        python_callable=cleaning_pipeline,
        op_args=[
            'tnt01-odycda-bld-01-1b81', 
            'tnt01-odycda-bld-01-stb-eu-certzone-3067f5f0', 
            'thParty/MFVS/GFV/Received'
        ]
    )

    end = EmptyOperator(
        task_id='end'
    )

    # Set task dependencies
    check_last_working_day >> [start, do_nothing]
    start >> unzipping_job >> dataquality_job >> moving_files_archive_job >> dp_cleaning_job >> end
