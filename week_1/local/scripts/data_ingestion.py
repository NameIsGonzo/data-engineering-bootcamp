from sqlalchemy import create_engine
import pandas as pd
import os
import argparse
import logging
import sys
from tqdm import tqdm

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def url_format(url) -> bool:
    if url.endswith('.parquet'):
        return True
    else:
        return False


def url_to_filename(url: str) -> str:
    try:
        output = url.split('/')
        # Get rid of .parquet and apply name convention for db table.
        output = output[4].replace('.parquet', '').replace('-', '_')
        return output
    except ValueError:
        logging.warning('The url doesn\'t have the correct format')
        return sys.exit(1)


def url_to_tablename(url: str) -> str:
    try:
        output = url.split('/')[4]
        # Get rid of .parquet and apply name convention for db table.
        output = output.split('_')[0]
        return output
    except ValueError:
        logging.warning('The url doesn\'t have the correct format')
        return sys.exit(1)


def download_data(url: str, output: str) -> bool:
    try:
        os.system(f'curl {url} > dataset/{output}') # Download nyc data
        return True
    except ConnectionError as e:
        logging.critical('Can\'t download file')
        logging.info(e)
        return False


def main():

    tables = {'yellow':'yellow_tripdata',
              'green': 'green_tripdata',
              'fhv': 'for_hire_vehicles_tripdata',
              'fhvhv': 'high_volume_for_hire_vehicle_tripdata'}

    # Set Logging level -> INFO
    logging.basicConfig(level=logging.INFO)

    # Set argparse description and flags
    parser = argparse.ArgumentParser(description='Ingest Parquet data into PostgreSQL Database')

    parser.add_argument('--user', required = True, help='Username for Postgres')
    parser.add_argument('--password', required = True, help='Password for Postgres user')
    parser.add_argument('--host', required = True, help='Host for Postgres')
    parser.add_argument('--port', required = True, help='Port for Postgres')
    parser.add_argument('--db', required = True, help='Database name for Postgres')
    parser.add_argument('--url', required = True, help='Url of the parquet file')

    args = parser.parse_args()

    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    url = args.url    
    
    if not url_format(url): # If the provided url uses cvs file return a warning and exit with code 1 
        logging.warning('NY Gov has changed ALL files into the Parquet format, please use the updated version of the url.')
        return sys.exit(1)

    file_name = url_to_filename(url) # Get file name
    table_name = tables.get(url_to_tablename(url)) # Get table name
    output = f'{file_name}.parquet' # Get file name with extension
    chunksize = 100000

    if download_data(url, output):
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}') # Create engine connection
        
        df = pd.read_parquet(f'dataset/{output}', engine='pyarrow') # Read parquet into Dataframe

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')  # We get rid of this later with dbt.

        with tqdm(total=len(df)) as pbar:
            for _, cdf in enumerate(chunker(df, 100000)):

                cdf.to_sql(name=table_name,
                    con=engine,
                    if_exists='append',
                    chunksize=100000)

                pbar.update(chunksize)
                tqdm._instances.clear()
    else:
        logging.critical('Something went wrong while donwloading the file')
        return sys.exit(1)


if __name__ == '__main__':
    main()