import os

from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from question1_ingest_week2 import aha
from ingest_fhv import fhv_csv_to_parquet,upload_to_gcs

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
FILE_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + 'yellow_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
PARQUET_FILE = 'yellow_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
PATH_PARQUET_FILE = AIRFLOW_HOME

TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}'

local_workflow = DAG(
    "HomeWorkWeek2",
    schedule_interval="0 6 2 * *",
    start_date = datetime(2019,1,1),
    end_date = datetime(2021,1,1),
    catchup = True,
    max_active_runs = 3
)

with local_workflow:
    dowdata=BashOperator(
        task_id="dowdata",
        retries=1,
        bash_command=f"curl -sSLf {FILE_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    csv_to_parquet=PythonOperator(
        task_id="csv_to_parquet",
        retries=1,
        python_callable=fhv_csv_to_parquet,
        op_kwargs=dict(
            srcfile = f"{OUTPUT_FILE_TEMPLATE}"
        )
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{PARQUET_FILE}",
            "local_file": f"{PATH_PARQUET_FILE}{PARQUET_FILE}",
        },
    )

    halo=PythonOperator(
        task_id='halo',
        retries=1,
        python_callable=aha
    )

    dowdata >> csv_to_parquet >> local_to_gcs_task >> halo