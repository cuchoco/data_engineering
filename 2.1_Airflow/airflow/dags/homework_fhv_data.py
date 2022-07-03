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


URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/csv_backup/"
URL_TEMPLATE = URL_PREFIX + "fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"

CSV_SAVE_POSTFIX = '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
PARQUET_SAVE_POSTFIX = '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

CSV_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + CSV_SAVE_POSTFIX
PARQUET_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + PARQUET_SAVE_POSTFIX


def format_to_parquet(src_file, dst_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dst_file)


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


dag = DAG(
    "FHV_to_GCS",
    start_date = datetime(2019, 1, 1),
    end_date= datetime(2019,12,31),
    schedule_interval = "0 6 2 * *",   # 매월
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de']
)


with dag:
    wget_task = BashOperator(
        task_id ='wget',
        bash_command = f'wget {URL_TEMPLATE} -O {CSV_OUTPUT_FILE_TEMPLATE}'
    )

    csv_to_parquet_task = PythonOperator(
        task_id = "csv_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": CSV_OUTPUT_FILE_TEMPLATE,
            "dst_file": PARQUET_OUTPUT_FILE_TEMPLATE
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"FHV_tripData + {PARQUET_SAVE_POSTFIX}",
            "local_file": f"{PARQUET_OUTPUT_FILE_TEMPLATE}",
        },
    )


    wget_task >> csv_to_parquet_task >> local_to_gcs_task