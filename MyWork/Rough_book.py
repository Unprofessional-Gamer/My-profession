from airflow.utils.dates import days ago

from datetime import datetime.timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.operators.empty import EmptyOperator

import pendulum

from pricing gfv.test gfy import copy and transfer_csv, zip and transfer csv files, cun pipeline

from archive gfy import move_files_with_prefixes

local tz pendulum.timezone('Europe/Lisbon')

project id="tnt01-odycda-bld-01-1681"

gce zone "europe-west2"

today datetime.now().strftime("%Y-%m-%d")

default_args-{

owner: airflow',

start date today,

*email_on_failur': False,

email on retry: False,

'retries':2,

retry delay: timedelta (minutes-5),

}

with DAG(dag id 'GfvZipping',

default args-default args,

description="zipping the CAP files and moving GFV files',

#schedule_interval '0/2

schedule-'@once', 
max active runs=1.

catchup=False,

) as dag:

start = EmptyOperator(

task_id= 'start',

dag-dag,

)

zipfiles job PythonOperator(

task _id="moving gfvfiles_job",

python_callable=copy_and_trarisfer.csv,

op_args=['tnt01-odycda-bld-01-1b81', 'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181", "thParty/MFVS/CAP/MONTHLY/2024-May/Archive", "tnt01-odycda-bld-01-stb-eu-tdip-dp-consumer-78a45ff3", 'thParty/MFVS/GFV/update'],

dag-dag,

) )

zipfiles job PythonOperator(

task_id="zipfiles_job",

python_callable zip_and_transfer_csv_files,

op_args=['tnt01-odycda-bld-01-1681', 'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181', "thParty/MFVS/CAP/MONTHLY/2024-May/Archive"," "tnt01-odycda-bld-01-stb-eu-tdip-dp-consumer-78a45ff3", 'thParty/MFVS/GFV/update'],

dag-dag,

)

dp movement job PythonOperator(

task_id="dp_movement_job",

python_callable-run_pipeline,

op args=['tnt01-odycda-bld-01-1b81', 'tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181', "thParty/MFVS/CAP/MONTHLY/2024-May/Archive", "tnt01-odycda-bld-01-stb-eu-tdip-dp-consumer-78a45ff3", 'thParty/MFVS/GFV/update'],

dag-dag,

)

end EmptyOperator( task id 'end", dag-dag,

)

# set task dependencies

start >> move files with_prefixes >> zipfiles job >> dp_movement job >> end
