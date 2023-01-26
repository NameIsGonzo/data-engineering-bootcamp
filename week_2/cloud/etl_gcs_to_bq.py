from pathlib import Path
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.credentials import GcpCredentials
from prefect import flow, task
import pandas as pd
import sys


@task(log_prints=True)
def clean_data(path: Path) -> pd.DataFrame:
    """Gets rid of record without passengers"""
    df = pd.read_parquet(path=path, engine='pyarrow')
    print(f"Pre Missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"Post Missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df.to_parquet(path, engine='pyarrow')
    return df


@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Extract data from Data Lake"""
    gcs_path = f'{color}/{color}_tripdata_{year}_{month:02}.parquet'
    gcs_block = GcsBucket.load("datatalks-bootcamp")
    gcs_block.get_directory(from_path=gcs_path, local_path=f'./dataset/')
    return Path(f'./dataset/{gcs_path}')


@task()
def write_to_bq(df: pd.DataFrame) -> None:
    """Write Dataframe to Big Query"""
    gcp_credentials = GcpCredentials.load('prefect-crews')
    df.to_gbq(destination_table='nyc_dataset.rides',
              project_id='dtc-data-eng-course',
              credentials=gcp_credentials.get_credentials_from_service_account(),
              if_exists='append',
              chunksize=500_000)
    return


@flow()
def etl_gcs_to_bq():
    """Main ETL flow"""
    color = 'yellow'
    year = 2022
    month = 9

    path = extract_from_gcs(color, year, month)
    df = clean_data(path)
    write_to_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()
