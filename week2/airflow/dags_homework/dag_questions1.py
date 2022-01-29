import os

from datetime import datetime
from pkgutil import get_data


from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from question1_ingest_week2 import helloworld


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


local_workflow = DAG(
    "HomeWorkWeek2",
    schedule_interval="0 6 2 * *",
    start_date = datetime(2019,1,1)
)

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
FILE_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + 'output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}'


with local_workflow:
    get_task = BashOperator(
        task_id = "get_data",
        bash_command = 'echo "helloworld!!"'
    )

    get_task = PythonOperator(
        task_id = "ingest_data",
        python_callable=helloworld,
    )
    
    get_data >> ingest_data