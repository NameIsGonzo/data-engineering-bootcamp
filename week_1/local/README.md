# Local data pipeline with Docker + PostgreSQL

To run this data pipeline, first we need to open our terminal or command prompt and navigate to week_1/local directory
and then run the following.

### Docker-Compose 

Run it:

```bash
docker-compose up
```

Run in detached mode:

```bash
docker-compose up -d
```

Shutting it down:

```bash
docker-compose down
```

For this local batch data pipeline we are going to use the following tools and libraries:

1. Docker
2. Anaconda
3. PostgreSQL
4. Pg Admin
5. Python
    1. Pandas
    2. SQLAlchemy
    3. TQDM
    4. PyArrow

First we are going to create a new virtual enviroment with python 3.10.9 with the following command:

```bash
conda create --name name_goes_here python=3.10.9
conda activate name_goes_here
```

After creating our virtual enviroment, venv for shot. We are going to clone the following git repo.

```bash
git clone https://github.com/GonzaloAlcalaGD/data-engineering-bootcamp
```

We should have the following working directory

```scheme
de_bootcamp:
	|
	|------ README.md
	|------ week_1:
                |
                |------ cloud
                |------ local:
                            |
                            |------ dev
                            |------ test
```

Lest move into de_bootcamp/week_1/local.

Before we dive into the project lest check our docker-compose.yaml which has all the services that we are going to use.

```yaml
services:
  pgdatabase:
    image: postgres:13
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - "./ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
    ports:
      - "5432:5432"
  pgadmin:
    image: dpage/pgadmin4
    volumes:
      - "./data_pgadmin:/var/lib/pgadmin:rw"
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8080:80"
```

As you can see for our PostgreSQL database we are going to use the following credentials:
**username = root
password = root
database = ny_taxi**

And for our PgAdmin server we are going to use the following credentials:
**email = admin@admin.com
password = root**

Once we are in local we need to run our docker-compose! (For this you will need to install Docker Desktop first).

```bash
docker compose up
```

Wait a few minutes and open another tab in the terminal and write the following to check bot services health

```bash
docker ps
```

You should see something like this. (I’ve been working on a while on this, that explains the Up 10 hours)

```bash
CONTAINER ID   IMAGE            COMMAND                  CREATED        STATUS        PORTS                           NAMES
3ef6608f4246   postgres:13      "docker-entrypoint.s…"   10 hours ago   Up 10 hours   0.0.0.0:5432->5432/tcp          local-pgdatabase-1
cbfda0da2374   dpage/pgadmin4   "/entrypoint.sh"         10 hours ago   Up 10 hours   443/tcp, 0.0.0.0:8080->80/tcp   local-pgadmin-1
```

Let’s check our database! 
Lets get inside the docker container with the name ‘local-pgdatabase-1’

```bash
docker exec -it CONTAINER_ID bash
```

Now let’s get inside postgresql in our terminal.

```bash
psql -d ny_taxi 
```

Here we can start playing around with our database, you should see something like this in your terminal

```bash
root@3ef6608f4246:/ psql -d ny_taxi
psql (13.7 (Debian 13.7-1.pgdg110+1))
Type "help" for help.

ny_taxi=#
```

But…using our terminal is not the most efficient way of working with our database. What about our other service PgAdmin?

First, lets open a new window in your favorite web browser. 
Then type the following: [http://localhost:8080/](http://localhost:8080/)

Once you are inside you should use the credentials we declared in our docker-compose.yaml

```yaml
environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
```

Once we are inside PgAdmin we need to stablish our connection between PgAdmin and our PostgreSQL database. Let’s click ‘Add New Server’ and then write ny_taxi into the name.

Then lets move into the connection section in the top part of the menu. Finally we just need to write our credentials which can also be found in our docker-compose.yaml (both username and password are root)

You should start seeing the PgAdmin ny_taxi dashboard. Let’s move into Python and ingest some data!

Here is our python code which is going to download, decompress, generate schema and upload into PostgreSQL through SQLAlchemy + Panda.

Let’s get to work! 

Let’s download some data from January 2022

```python
python data_ingestion.py \
 --user=root \
 --password=root \
 --host=localhost \
 --port=5432 \
 --db=ny_taxi \
 --url=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet
```

If we go to our PgAdmin dashboard and write the following query

```sql
SELECT 
count(*)
FROM yellow_tripdata;
```

We should start seeing out data being ingested in batches of 100,00

If we run the following query we will see the first 50 records in our database

```sql
SELECT
*
FROM yellow_tripdata
LIMIT 50;
```