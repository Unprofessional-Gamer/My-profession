from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'my_dag_with_messages',
    default_args=default_args,
    description='A simple DAG to run two Python scripts with start and end messages',
    schedule_interval='@hourly',
)

# Define the functions to run the scripts
def run_script1():
    script_path = '/path/to/thParty/CAP/script1.py'
    exec(open(script_path).read())

def run_script2():
    script_path = '/path/to/thParty/CAP/script2.py'
    exec(open(script_path).read())

# Create tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

task1 = PythonOperator(
    task_id='run_script1',
    python_callable=run_script1,
    dag=dag,
)

task2 = PythonOperator(
    task_id='run_script2',
    python_callable=run_script2,
    dag=dag,
)

# Define the task sequence
start >> task1 >> task2 >> end
