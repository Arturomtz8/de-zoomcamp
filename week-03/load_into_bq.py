from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()
table_id = "yellow_taxi_data.nyc_taxi"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
)
uri = "gs://dtc_data_lake_de-zoomcamp-355322/yellow_tripdata/2019/yellow_tripdata_2019-*.parquet"

load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))
