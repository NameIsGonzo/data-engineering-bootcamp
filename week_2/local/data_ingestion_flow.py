from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import os
import sys
import pandas as pd
from prefect_sqlalchemy import SqlAlchemyConnector


def url_to_filename(url: str) -> str:
    try:
        output = url.split('/')
        # Get rid of .parquet and apply name convention for db table.
        output = output[4].replace('.parquet', '').replace('-', '_')
        return output
    except ValueError:
        print('The url doesn\'t have the correct format')
        return sys.exit(1)


@task(log_prints=True, tags=['extract'], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url: str, output: str) -> pd.DataFrame:
    try:
        os.system(f'curl {url} > dataset/{output}')  # Download nyc data
        df = pd.read_parquet(f'dataset/{output}', engine='pyarrow')  # Read parquet into Dataframe
        return df
    except ConnectionError as e:
        print('Can\'t download file')
        print(e)
        return sys.exit(1)


@task(log_prints=True)
def transform_data(data) -> pd.DataFrame:
    print(f"Pre Missing passenger count: {data['passenger_count'].isin([0]).sum()}")
    data = data[data['passenger_count'] != 0]
    print(f"Post Missing passenger count: {data['passenger_count'].isin([0]).sum()}")
    return data


@task(log_prints=True, retries=3)
def load_data(tb_name, data):

    conn_block = SqlAlchemyConnector.load("postgres-connector")

    with conn_block.get_connection(begin=False) as engine:
        data.head(n=0).to_sql(name=tb_name, con=engine, if_exists='replace')  # We get rid of this later with dbt.
        data.to_sql(name=tb_name,
                    con=engine,
                    if_exists='append',
                    chunksize=100_000)


@flow(name='Data ingestion')
def main_flow():
    tb_name = 'yellow_taxi_trips'
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet'

    output = url_to_filename(url)
    raw_data = extract_data(url, output)
    data = transform_data(raw_data)
    load_data(tb_name, data)


if __name__ == '__main__':
    main_flow()
