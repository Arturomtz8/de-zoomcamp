FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2-binary pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py 

CMD ["python", "ingest_data.py", "--user", "root", "--password", "root", "--host", "pgdatabase", "--port", "5432", "--db", "ny_taxi", "--table_name", "yellow_taxi_data", "--url", "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.parquet"]