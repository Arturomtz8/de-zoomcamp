import os 
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import datetime

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
GITHUB_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"


dag = DAG(
    dag_id="dag_hw_ingest_for_taxi_zones",
    schedule_interval="@once",
    start_date=datetime.datetime(2019, 1, 1),
    max_active_runs=1,
    tags=["dtc-de"],
)

wget_task = BashOperator(
    task_id="wget",
    bash_command=f"curl -sSL {GITHUB_URL} > {path_to_local_home}/taxi_zone_lookup.csv",
    dag=dag,
)


def format_to_parquet(src_file):
    if not src_file.endswith(".csv"):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace(".csv", ".parquet"))

format_to_parquet_task = PythonOperator(
    task_id="format_to_parquet_task",
    python_callable=format_to_parquet,
    op_kwargs={
        "src_file": f"{path_to_local_home}/taxi_zone_lookup.csv"
    },
    dag=dag,
)

def upload_to_gcs(bucket, object_name, local_file):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


local_to_gcs_task = PythonOperator(
    task_id="local_to_gcs_task",
    python_callable=upload_to_gcs,
    op_kwargs={
        "bucket": BUCKET,
        "object_name": f"raw/taxi_zone_lookup.parquet",
        "local_file": f"{path_to_local_home}/taxi_zone_lookup.parquet",
    },
    dag=dag
)

wget_task >> format_to_parquet_task >> local_to_gcs_task