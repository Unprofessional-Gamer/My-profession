from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import pendulum
import calendar

# Function to check if today is the last working day of the month
def is_last_working_day_of_month(execution_date):
    year = execution_date.year
    month = execution_date.month
    last_day_of_month = calendar.monthrange(year, month)[1]
    last_day_date = datetime(year, month, last_day_of_month)

    # Adjust if the last day is a weekend
    while last_day_date.weekday() > 4:  # 0=Monday, 4=Friday
        last_day_date -= timedelta(days=1)

    return execution_date.date() == last_day_date.date()

# Task to check if today is the last working day of the month
def check_last_working_day(**context):
    execution_date = context['execution_date']
    if is_last_working_day_of_month(execution_date):
        return 'start_tasks'
    else:
        return 'end'

# Define constants and default arguments
local_tz = pendulum.timezone('Europe/Lisbon')
project_id = "tnt01-odycda-bld-01-1681"
gce_zone = "europe-west2"
today = datetime.now().strftime("%Y-%m-%d")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='GfvZipping',
    default_args=default_args,
    description="Zipping the CAP files and moving GFV files",
    schedule_interval='0 14 * * *',  # Run every day at 2 PM
    max_active_runs=1,
    catchup=False,
    tags=['example'],
) as dag:

    check_date = PythonOperator(
        task_id='check_date',
        python_callable=check_last_working_day,
        provide_context=True,
    )

    start_tasks = DummyOperator(
        task_id='start_tasks'
    )

    end = DummyOperator(
        task_id='end'
    )

    # Define your other tasks as usual
    move_files_with_prefixes_task = PythonOperator(
        task_id="move_files_with_prefixes_task",
        python_callable=move_files_with_prefixes,
        op_args=[
            'tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a',
            'thparty/MFVS/GFV/SFGDrop',
            'tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a',
            'thparty/MFVS/GFV',
            'thparty/MFVS/CAP',
            [
                "CPRNEW", "CDENEW", "CPRVAL", "CDEVAL",
                "LDENEW", "LPRNEW", "LDEVAL", "LPRVAL"
            ],
            [
                "CPRRVU", "CPRRVN"
            ]
        ]
    )

    beam_pipeline_task = PythonOperator(
        task_id="beam_pipeline_task",
        python_callable=run_beam_pipeline,
        op_args=[
            project_id,
            'tnt01-odycda-bld-01-stb-eu-rawzone-d90dce7a',
            'thparty/MFVS/GFV/SFGDrop',
            'thparty/MFVS/GFV',
            'thparty/MFVS/CAP',
            ["CPRNEW", "CDENEW", "CPRVAL", "CDEVAL", "LDENEW", "LPRNEW", "LDEVAL", "LPRVAL"]
        ]
    )

    zipfiles_job = PythonOperator(
        task_id="zipfiles_job",
        python_callable=zip_and_transfer_csv_files,
        op_args=[
            'tnt01-odycda-bld-01-1681', 
            'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181',
            "thParty/MFVS/CAP/MONTHLY/2024-May/Archive",
            "tnt01-odycda-bld-01-stb-eu-tdip-dp-consumer-78a45ff3",
            'thParty/MFVS/GFV/update'
        ]
    )

    dp_movement_job = PythonOperator(
        task_id="dp_movement_job",
        python_callable=run_pipeline,
        op_args=[
            'tnt01-odycda-bld-01-1681', 
            'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181',
            "thParty/MFVS/CAP/MONTHLY/2024-May/Archive",
            "tnt01-odycda-bld-01-stb-eu-tdip-dp-consumer-78a45ff3",
            'thParty/MFVS/GFV/update'
        ]
    )

    # Set task dependencies
    check_date >> [start_tasks, end]
    start_tasks >> move_files_with_prefixes_task >> beam_pipeline_task >> zipfiles_job >> dp_movement_job >> end
