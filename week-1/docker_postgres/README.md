# Week 1: Docker + Postgres

Guiding references:
- https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup
- https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/2_docker_sql

## Dataset

Dataset website (including data dictionaries): https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Download dataset if desired considering this is downloaded in notebook and script:
```bash
wget -qO dataset.csv.gz https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
# or
curl -Lso dataset.csv.gz https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
```

## Python environment
Create local Python environment if using `pgcli`, notebook, or script directly from shell:
```bash
python3.9 -m venv venv
source venv/bin/activate
```

Install `pgcli` if desired:
```bash
pip install pgcli==3.5.0
```

## Running Postgres with Docker

Run container:
```bash
docker run -d \
    --name pg \
    -e POSTGRES_DB=ny_taxi \
    -e POSTGRES_PASSWORD=root \
    -e POSTGRES_USER=root \
    -v ${PWD}/dbdata:/var/lib/postgresql/data  \
    -p 5432:5432 \
    postgres:14
```

Accessing database using `pgcli`:
```bash
PGPASSWORD=root pgcli -h localhost -p 5432 -u root -d ny_taxi
```

## Notebook and script
- All following has been run from Ubuntu; using Windows is easily adapted as shown in course videos.
- Note that the ingestion in both notebook and script uses DataFrame iterator as an example to avoid demanding too many resources for greater-size files. Consider the gzipped CSV file used here could be fully loaded into memory as it is only around 24 MB.

Install requirements in Python environment:
- install development requirements if using notebook.
- notebook is expected to be used in VSCode by installing `ipykernel` package. Optionally, install and use [JupyterLab](https://jupyter.org/install).
- after installing requirements select Python interpreter in VSCode.
```bash
pip install -r requirements.txt -r requirements.dev.txt
```

Notebook has two main sections:
- Exploration: explore columns and values from dataset, and show parsing of datetime columns.
- Ingestion: ingest dataset to database.

Script allows ingesting data from shell:
```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips_from_script \
    --url="${URL}"
```

## Using Docker Compose
- in course video for ingestion with Docker Compose ([DE Zoomcamp 1.2.5 - Running Postgres and pgAdmin with Docker-Compose](https://www.youtube.com/watch?v=hKI6PkPhpa0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)), instructor maps host port to database container port and then run ingestion container without sharing any network by defining the URL argument with host inet. You can run `ifconfig` on Linux or `ipconfig` on Windows from host to look for inet.
- [docker-compose.yaml](./docker-compose.yaml) creates a named volume for pgadmin so it has persistent state.

Lifting up services using Docker Compose v1 (only database and pgadmin):
- running [docker-compose.yaml](./docker-compose.yaml) manifest with Docker Compose v1 won't start the ingestion service as it states `profiles` field from v2 specification.
```bash
docker-compose up -d
docker-compose down --volumes
```

Lifting up services using Docker Compose v2:
- Only database and pgadmin:
    ```bash
    docker compose up
    docker compose down --volumes
    ```
- Database, pgadmin, and ingestion service:
    ```bash
    docker compose build ingestion
    docker compose --profile ingestion up
    docker compose --profile ingestion down --volumes
    ```


## Homework

Lifting database and pgadmin using Docker Compose v2:
```bash
docker compose up
```

Ingesting green taxi trips from January 2019:
- Note that ingestion is slow as of using DataFrame iterator on a gzipped file.
```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url="${URL}"
```

Ingesting taxi zones:
```bash
URL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=taxi_zones \
    --url="${URL}"
```

Queries:
- ```sql
    SELECT COUNT(*)
    FROM green_taxi_trips
    WHERE
        CAST(lpep_pickup_datetime AS DATE) = '2019-01-15'
        AND CAST(lpep_dropoff_datetime AS DATE) = '2019-01-15';
    ```

- ```sql
    SELECT
        CAST(lpep_pickup_datetime AS DATE) AS pickup_date,
        MAX(Trip_distance) AS max_distance
    FROM green_taxi_trips
    GROUP BY pickup_date
    ORDER BY max_distance DESC;
    ```

- ```sql
    SELECT passenger_count, COUNT(*)
    FROM green_taxi_trips
    WHERE
        CAST(lpep_pickup_datetime AS DATE) = '2019-01-01'
        AND passenger_count IN (2, 3)
    GROUP BY passenger_count;
    ```

- ```sql
    -- single 'Astoria' zone
    WITH astoria_zone AS (
        SELECT
            "LocationID" AS location_id,
            "Zone" AS location_name
        FROM taxi_zones
        WHERE "Zone" = 'Astoria'
    ),
    -- trips picking up in 'Astoria'
    astoria_trips AS (
        SELECT
            trips."PULocationID" AS pu_location_id,
            astoria_zone.location_name AS pu_location_name,
            trips."DOLocationID" AS du_location_id,
            trips.tip_amount
        FROM
            green_taxi_trips AS trips
        JOIN astoria_zone
            ON trips."PULocationID" = astoria_zone.location_id
    )
    SELECT
        pu_location_id,
        pu_location_name,
        du_location_id,
        zones."Zone" AS du_location_name,
        MAX(tip_amount) AS max_tip_amount
    FROM astoria_trips
        LEFT JOIN taxi_zones AS zones
        ON astoria_trips.du_location_id = zones."LocationID"
    GROUP BY 1, 2, 3, 4
    ORDER BY max_tip_amount DESC;
    ```
