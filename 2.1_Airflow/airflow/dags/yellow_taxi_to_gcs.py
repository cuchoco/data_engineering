# Create a new dag for transferring the FHV data
# Create another dag for the Zones data

import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", '/opt/airflow/')
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
URL_TEMPLATE = URL_PREFIX + "yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

PARQUET_SAVE_POSTFIX = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
PARQUET_OUTPUT_FILE_TEMPLATE = os.path.join(AIRFLOW_HOME, PARQUET_SAVE_POSTFIX)


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


dag = DAG(
    "yellow_taxi_data_DAG",
    schedule_interval = "@monthly",  
    start_date = datetime(2018, 12, 31),
    end_date= datetime(2019,12,31),
    max_active_runs=3,
    tags=['dtc-de']
)

with dag:
    wget_task = BashOperator(
        task_id ='wget',
        bash_command = f'wget {URL_TEMPLATE} -O {PARQUET_OUTPUT_FILE_TEMPLATE}'
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"ny_taxi/{PARQUET_SAVE_POSTFIX}",
            "local_file": f"{PARQUET_OUTPUT_FILE_TEMPLATE}",
        }
    )

    wget_task >> local_to_gcs_task
