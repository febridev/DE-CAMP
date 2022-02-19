import os

from datetime import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
INPUT_PART = 'raw'

PARQUET_FILE = 'output_{{ execution_date.strftime(\'%Y-%m\') }}_'
DATASET='parquet'
parquet_file = '.parquet'
TABLE_NAME_TEMPLATE = 'yellow_tripdata'

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022,2,9),
    "depends_on_past": False,
    "retries":1
}


local_workflow = DAG(
    "yellow_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup = False,
    max_active_runs = 1,
    tags=['dtc-de']
)

with local_workflow:
    gcs_to_gcs_task = GCSToGCSOperator(
        task_id='gcs_to_gcs_task',
        source_bucket=BUCKET,
        source_object=f'{INPUT_PART}/output_*',
        destination_bucket=BUCKET,
        destination_object=f'yellow_trips/yellow',
        move_object=True
    )
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id="bigquery_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{TABLE_NAME_TEMPLATE}",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": "PARQUET",
                    "sourceUris": f"gs://{BUCKET}/yellow_trips/*{parquet_file}",
                },
            },
    )



    gcs_to_gcs_task >> bigquery_external_table_task