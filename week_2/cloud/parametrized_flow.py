from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd
import os
import sys
import yaml


def get_subdir(filename: str) -> str:
    try:
        output = f"{filename.split('_')[0]}"
        return output
    except ValueError as e:
        print('The filename doesnt have the correct format')
        print(e)
        return sys.exit(1)


@task(log_prints=True)
def enforce_schema(df: pd.DataFrame, color: str, dir='/opt/prefect/schemas/data_schemas.yaml') -> pd.DataFrame:

    with open(dir, 'rb') as f:
        color_schema = yaml.safe_load(f)[color]
    print(f'Working with schema for dataset {color}')
    print(color_schema)
    enforced_df = df.astype(color_schema)
    if color == 'yellow':
        enforced_df = enforced_df.drop(columns=['airport_fee'])
    return enforced_df


@task(log_prints=True)
def filter_out_of_bounds(directory: str) -> pd.DataFrame:
    """Filters out of bounds records from parquet files."""
    table = pq.read_table(directory)
    try:
        table = table.filter(
            pc.less_equal(table['dropOff_datetime'], pa.scalar(pd.Timestamp.max))
        )
        filter_df = table.filter(
            pc.less_equal(table['pickup_datetime'], pa.scalar(pd.Timestamp.max))
        ).to_pandas()
        return filter_df
    except:
        print('Field not found, skipping filter')
        return table.to_pandas()


@task(log_prints=True)
def extract_data(url: str, file_name: str, color: str) -> pd.DataFrame:
    """Downloads a parquet file and returns it as a pandas Dataframe"""
    file_dir = f'dataset/{color}/{file_name}.parquet'
    try:
        os.system(f'curl {url} -o {file_dir}')
        return
    except ValueError as e:
        print(e)
        return sys.exit(1)


@task(log_prints=True)
def write_gcs(path: Path, filename: str) -> None:
    """Loads a GCS block with our credentials and uploads the file to our GCS Bucket"""
    gcs_path = f'{get_subdir(filename)}/{filename}.parquet'
    gcs_block = GcsBucket.load("datatalks-bootcamp")
    gcs_block.upload_from_path(from_path=path, to_path=gcs_path)
    return


@task(log_prints=True)
def save_df(df: pd.DataFrame, directory: str)-> None:
    df.to_parquet(directory, engine='pyarrow', compression='snappy')
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """Main ETL flow"""
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02}.parquet'
    file_name = f'{color}_tripdata_{year}-{month:02}'
    path = f'dataset/{color}/{file_name}.parquet'

    extract_data(url, file_name, color)
    df = filter_out_of_bounds(path)
    enforced_df = enforce_schema(df, color)
    save_df(enforced_df, path)
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
