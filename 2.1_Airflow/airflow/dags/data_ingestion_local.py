from datetime import datetime
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", '/opt/airflow/')

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval = "0 6 2 * *",   # 매년 매월 2일 6시에 실행
    start_date = datetime(2021, 1, 1)
)

url = "https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-01.parquet"

with local_workflow:

    wget_task = BashOperator(
        task_id ='wget',
        bash_command=f'curl -sSL {url} > {AIRFLOW_HOME}/output.csv'
    )

    ingest_task = BashOperator(
        task_id ='ingest',
        bash_command=f'ls {AIRFLOW_HOME}'
    )

    wget_task >> ingest_task