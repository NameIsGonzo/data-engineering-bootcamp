from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pandas as pd
import os
import sys


@task(log_prints=True)
def filename_from_url(url: str) -> str:
    """Extract filename from url that downloads a .parquet file"""
    try:
        output = url.split('/')
        output = output[4].replace('.parquet', '').replace('-', '_')
        return output
    except ValueError as e:
        print('The url doesnt have the correct format')
        print(e)
        return sys.exit(1)


def get_subdir(filename: str) -> str:
    try:
        output = f"{filename.split('_')[0]}"
        return output
    except ValueError as e:
        print('The filename doesnt have the correct format')
        print(e)
        return sys.exit(1)


@task()
def extract_data(url: str, file_name: str, color: str) -> pd.DataFrame:
    """Downloads a parquet file and returns it as a pandas Dataframe"""
    try:
        os.system(f'curl {url} -o dataset/{color}/{file_name}.parquet')
        df = pd.read_parquet(f'dataset/{color}/{file_name}.parquet', engine='pyarrow')
        return df
    except ValueError as e:
        print(e)
        return sys.exit(1)


@task()
def write_gcs(path: Path, filename: str):
    """Loads a GCS block with our credentials and uploads the file to our GCS Bucket"""
    gcs_path = f'{get_subdir(filename)}/{filename}.parquet'
    gcs_block = GcsBucket.load("datatalks-bootcamp")
    gcs_block.upload_from_path(from_path=path, to_path=gcs_path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """Main ETL flow"""
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02}.parquet'
    file_name = filename_from_url(url)
    df = extract_data(url, file_name, color)
    path = f'dataset/{color}/{file_name}.parquet'
    write_gcs(path, file_name)


@flow()
def etl_parent_flow(year: int, months: list[int], color: str):
    """Parametrized flow"""
    for month in months:
        etl_web_to_gcs(year, month, color)
    return


if __name__ == '__main__':
    year = 2021
    months = [1, 2, 3]
    color = 'yellow'

    etl_parent_flow(year, months, color)