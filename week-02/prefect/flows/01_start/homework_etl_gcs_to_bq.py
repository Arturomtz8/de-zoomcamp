from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from typing import List


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("bucket-zoomcamp")
    gcs_block.get_directory(from_path=gcs_path, local_path=".")
    print(gcs_path)
    return Path(f"{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame, color: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials = GcpCredentials.load("gcp-credentials-zoomcamp")
    google_project_id = Secret.load("google-project-id")
    bq_table = f"trips_data_all.trips_{color}"

    df.to_gbq(
        destination_table=bq_table,
        project_id=google_project_id.get(),
        credentials=gcp_credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df, color)


@flow()
def etl_parent_flow():
    years = [2019, 2020]
    months = range(1, 13)
    color: str = "yellow"
    for year in years:
        for month in months:
            etl_gcs_to_bq(year, month, color)


if __name__ == "__main__":
    etl_parent_flow()
