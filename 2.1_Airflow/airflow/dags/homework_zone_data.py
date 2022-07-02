
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

URL_TEMPLATE = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

SAVE_POSTFIX = '/zone.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + SAVE_POSTFIX



def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))



def upload_to_gcs(bucket, object_name, local_file):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)



dag = DAG(
    "ZoneDataToGCS",
    schedule_interval = "@once",   #한번
    tags=['zone-to-gcs'],
    start_date=days_ago(1)
)


with dag:
    wget_task = BashOperator(
        task_id ='wget',
        bash_command = f'wget {URL_TEMPLATE} -O {OUTPUT_FILE_TEMPLATE}'
    )

    csv_to_parquet_task = PythonOperator(
        task_id='csv_convert',
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": OUTPUT_FILE_TEMPLATE
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/zone.parquet",
            "local_file": f"{AIRFLOW_HOME + '/zone.parquet'}"
        },
    )


    wget_task >> csv_to_parquet_task >> local_to_gcs_task