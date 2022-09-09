from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from ingest_to_pg import ingest_callable

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")


URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
URL_TEMPLATE = URL_PREFIX + "/yellow_tripdata_2021-{{ execution_date.strftime(\'%m\') }}.csv.gz"
TABLE_NAME_TEMPLATE = "yellow_taxi_2021_{{ execution_date.strftime(\'%m\') }}"

OUTPUT_FILE_TEMPLATE_COMPRESSED = AIRFLOW_HOME + '/output_2021-{{ execution_date.strftime(\'%m\') }}.csv.gz'
OUTPUT_FILE_TEMPLATE_DECOMPRESSED = AIRFLOW_HOME + '/output_2021-{{ execution_date.strftime(\'%m\') }}.csv'

# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-05.csv.gz
local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval = "0 6 2 * *",
    start_date=datetime(2019, 1, 1)
)

with local_workflow:
    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE_COMPRESSED}',

    
    )


    gzip_task = BashOperator(
        task_id = "gzip",
        bash_command= f"gunzip {OUTPUT_FILE_TEMPLATE_COMPRESSED}"
    )

    ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=ingest_callable,
        op_kwargs = dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST, 
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=OUTPUT_FILE_TEMPLATE_DECOMPRESSED,
        )
    )

    wget_task >> gzip_task >> ingest_task