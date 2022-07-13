import os
from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
INPUT_FILETYPE= "parquet"
DATASET = "tripdata"
COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="gcs_to_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for color, ds_col in COLOUR_RANGE.items():
        gcs_to_bq_ext_task = BigQueryCreateExternalTableOperator(
            task_id=f"gcs_to_bq_{color}_ext_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{color}_{DATASET}",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/{color}/*"],
                },
            },
        )

        CREATE_PART_TBL_QUERY = f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{color}_{DATASET}_partitioned \
            PARTITION BY DATE({ds_col}) AS \
            SELECT * FROM {BIGQUERY_DATASET}.{color}_{DATASET};"

        bq_ext_to_part_task = BigQueryInsertJobOperator(
            task_id=f"bq_ext_{color}_to_part_task",
            configuration={
                "query": { 
                    "query":CREATE_PART_TBL_QUERY,
                    "useLegacySql": False,
                }
            },
        )

        gcs_to_bq_ext_task >> bq_ext_to_part_task 
