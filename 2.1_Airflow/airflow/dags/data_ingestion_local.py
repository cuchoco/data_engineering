from datetime import datetime
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_script import ingest_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", '/opt/airflow/')

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval = "0 6 2 * *",   # 매년 매월 2일 6시에 실행
    start_date = datetime(2021, 1, 1)
)

URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
URL_TEMPLATE = URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

with local_workflow:

    wget_task = BashOperator(
        task_id ='wget',
        bash_command = f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_task = PythonOperator(
        task_id ='ingest',
        python_callable=ingest_callable,

    )

    wget_task >> ingest_task