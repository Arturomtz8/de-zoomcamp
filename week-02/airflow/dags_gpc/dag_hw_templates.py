"""
A cleaner version using templates and a function to
avoid repeating code
"""
import os 
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import datetime

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq


PATH_LOCAL_DIR = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

def format_to_parquet(src_file):
    if not src_file.endswith(".csv"):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace(".csv", ".parquet"))


def upload_to_gcs(bucket, object_name, local_file):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def download_parquetize_upload(dag_info, github_url, template_file_compressed, template_file_decompressed, file_parquet_format, gcs_path):
    dag = dag_info

    wget_task = BashOperator(
        task_id="wget",
        bash_command=f"curl -sSL {github_url}{template_file_compressed} > "
        f"{PATH_LOCAL_DIR}/{template_file_compressed}",
        dag=dag,
    )


    gzip_task = BashOperator(
        task_id="gzip",
        bash_command=f"gunzip -f {PATH_LOCAL_DIR}/{template_file_compressed}",
        dag=dag,
    )


    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{PATH_LOCAL_DIR}/{template_file_decompressed}"
        },
        dag=dag,
    )



    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"{gcs_path}{file_parquet_format}",
            "local_file": f"{PATH_LOCAL_DIR}/{file_parquet_format}",
        },
        dag=dag
    )

    wget_task >> gzip_task >> format_to_parquet_task >> local_to_gcs_task




github_url_yellow = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
template_file_compressed_yellow= "yellow_tripdata_{{execution_date.year}}-{{'%02d' % execution_date.month}}.csv.gz"
template_file_decompressed_yellow = "yellow_tripdata_{{execution_date.year}}-{{'%02d' % execution_date.month}}.csv"
file_yellow_parquet = template_file_decompressed_yellow.replace(".csv", ".parquet")
gcs_path_yellow = "yellow_tripdata/2019/"


dag_yellow_tripdata = DAG(
    dag_id="yellow_tripdata",
    schedule_interval="@monthly",
    start_date=datetime.datetime(2019, 1, 1),
    end_date=datetime.datetime(2019, 3, 1),
    max_active_runs=1,
    tags=["dtc-de"],
)

download_parquetize_upload(
    dag_info=dag_yellow_tripdata, 
    github_url=github_url_yellow, 
    template_file_compressed=template_file_compressed_yellow, 
    template_file_decompressed=template_file_decompressed_yellow, 
    file_parquet_format=file_yellow_parquet, 
    gcs_path=gcs_path_yellow)



github_url_fhv = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"
template_file_compressed_fhv = "fhv_tripdata_{{execution_date.year}}-{{'%02d' % execution_date.month}}.csv.gz"
template_file_decompressed_fhv = "fhv_tripdata_{{execution_date.year}}-{{'%02d' % execution_date.month}}.csv"
file_fhv_parquet = template_file_decompressed_fhv.replace(".csv", ".parquet")
gcs_path_fhv = "fhv_tripdata/2019/"


dag_fhv_tripdata = DAG(
    dag_id="fhv_tripdata",
    schedule_interval="@monthly",
    start_date=datetime.datetime(2019, 1, 1),
    end_date=datetime.datetime(2019, 3, 1),
    max_active_runs=1,
    tags=["dtc-de"],
)

download_parquetize_upload(
    dag_info=dag_fhv_tripdata,
    github_url=github_url_fhv, 
    template_file_compressed=template_file_compressed_fhv, 
    template_file_decompressed=template_file_decompressed_fhv, 
    file_parquet_format=file_fhv_parquet, 
    gcs_path=gcs_path_fhv
)