docker run -it \
    taxi_ingest:v1 \
    --user=root \
    --password=root \
    --host=host.docker.internal \
    --port=5433 \
    --db=ny_taxi \
    --table_name=ny_taxi_zones \
    --url=https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv