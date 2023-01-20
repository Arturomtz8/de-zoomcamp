#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector



@task(log_prints=True, tags=["extract raw data"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url: str):
    if url.endswith('.csv.gz'):
        csv_name = 'green_tripdata_2019-01.csv.gz'
    else:
        csv_name = 'taxi_zone_lookup.csv'
    
    os.system(f"wget {url} -O {csv_name}")
    df = pd.read_csv(csv_name)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    return df


@task(log_prints=True, tags=["transform raw data"])
def transform_data(df: pd.DataFrame):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, tags=["load data into postgres"])
def load_data(df: pd.DataFrame, table_name:str):
    connection_block = SqlAlchemyConnector.load("pgadmin-docker")
    with connection_block.get_connection(begin=False) as engine:
        df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=100000)



@flow(name="Ingest Data")
def main_flow():
    gzip_url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
    csv_url="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    raw_data = extract_data(gzip_url)
    clean_data = transform_data(raw_data)
    load_data(clean_data, "prefect_test")


if __name__ == '__main__':
    main_flow()