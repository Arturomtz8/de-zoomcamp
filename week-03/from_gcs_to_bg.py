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
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.csv.gz"
    gcs_block = GcsBucket.load("bucket-zoomcamp")
    gcs_block.get_directory(from_path=gcs_path, local_path=".")
    print(gcs_path)
    return Path(f"{gcs_path}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_csv(path)
    print(df.columns.tolist())
    df.drop(columns=["Unnamed: 0"], inplace=True)

    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials = GcpCredentials.load("gcp-credentials-zoomcamp")
    google_project_id = Secret.load("google-project-id")
    bq_table = "trips_data_all.fhv_data"
    # print(google_project_id.get())
    # secret_block = Secret.load("big-query-table")
    # print(secret_block.get())
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
    write_bq(df)


@flow()
def etl_parent_flow():
    year = 2019
    months = range(1, 13)
    color: str = "fhv"
    for month in months:
        etl_gcs_to_bq(year, month, color)


if __name__ == "__main__":
    etl_parent_flow()
