#!/usr/bin/env python
# coding: utf-8

import argparse
import os
# import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine
import gzip
import io

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    filename_gz = "green_tripdata_2019-01.csv.gz"
    filename_csv = "taxi_zone_lookup.csv"
    
    # parquet_table = pq.read_table(filename)
    # df = parquet_table.to_pandas()
    if url.endswith(".gz"):
        os.system(f"wget {url} -O {filename_gz} --no-check-certificate")
        with gzip.open(filename_gz, 'rb') as f:
            file_content = f.read()
            engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
            # df = pd.read_csv(file_content)
            df = pd.read_csv(io.StringIO(file_content.decode("utf-8")))

            df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=100000)
    else:
        os.system(f"wget {url} -O {filename_csv} --no-check-certificate")
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        df_csv = pd.read_csv(filename_csv)
        df_csv.to_sql(name=table_name, con=engine, if_exists='replace', chunksize=100000)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the parquet file')
  
    args = parser.parse_args()

    main(args)