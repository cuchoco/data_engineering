# Create a new dag for transferring the FHV data
# Create another dag for the Zones data

import os
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", '/opt/airflow/')
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip+data/"
URL_TEMPLATE = URL_PREFIX + "fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

SAVE_POSTFIX = '/fhv_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + SAVE_POSTFIX


def upload_to_gcs(bucket, object_name, local_file):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


dag = DAG(
    "GCSIngestionDag",
    start_date = datetime(2019, 1, 1),
    end_date= datetime(2019,12,31),
    schedule_interval = "0 6 2 * *",   # 매월
    catchup=True,
    tags=['fhv-to-gcs']
)


with dag:
    wget_task = BashOperator(
        task_id ='wget',
        bash_command = f'wget {URL_TEMPLATE} -O {OUTPUT_FILE_TEMPLATE}'
    )


    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{SAVE_POSTFIX}",
            "local_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )


    wget_task >> local_to_gcs_task