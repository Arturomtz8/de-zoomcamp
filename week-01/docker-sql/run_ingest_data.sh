docker run -it \
    taxi_ingest:v1 \
    --user=root \
    --password=root \
    --host=host.docker.internal \
    --port=5433 \
    --db=ny_taxi \
    --table_name=ny_taxi_data \
    --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
 


#  docker run -it \
#     --network=docker-sql_default \
#     taxi_ingest:v001 \
#     --user=root \
#     --password=root \
#     --host=pgdatabase \
#     --port=5432 \
#     --db=ny_taxi \
#     --table_name=taxi_zones \
#     --url=${URL}
 