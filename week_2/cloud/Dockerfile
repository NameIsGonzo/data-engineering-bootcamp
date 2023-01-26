FROM prefecthq/prefect:2.7.7-python3.10

COPY cloud/docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir
RUN apt-get update && apt-get install -y \
curl
CMD /bin/bash

COPY ./nothing_to_see_here /opt/prefect/nothing_to_see_here
COPY . /opt/prefect/flows
COPY ./dataset /opt/prefect/dataset