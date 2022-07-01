import pyarrow.parquet as pq
from sqlalchemy import create_engine


def main():
    engine = create_engine(f'postgresql://root:root@localhost:5431/ny_taxi')
    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print("engine created")
    parquet_table = pq.read_table("/Users/arturomartinez/Desktop/de-zoomcamp/week-01/docker-sql/output.parquet")
    df = parquet_table.to_pandas()
    print(df)

    df.to_sql(name="yellow_taxi_data", con=engine, if_exists='append', chunksize=10000)
    
main()