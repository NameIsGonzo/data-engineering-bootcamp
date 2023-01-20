FROM python:3.10

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR week_1/local/scripts
COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py" ]