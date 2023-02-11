# Week 3: Data warehouse with BigQuery

Guiding references:
- https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_3_data_warehouse
- https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_3_data_warehouse/homework.md

Datasets:
- https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv


----
<center><b>IMPORTANT NOTE</b></center>

All following queries in this README are based on shell substitution in here documents escaping backticks as these are evaluated as shell commands.

This brings two options using here documents:

- Display resulting query to manually copy and execute in BigQuery interface:
    ```bash
    cat << EOF
    #standardSQL
    CREATE SCHEMA IF NOT EXISTS \`${PROJECT_ID}.${DATASET_NAME}\
    OPTIONS (
        location = 'US',
    );
    EOF
    ```
- Execute queries using `bq query` command:
    ```bash
    bq query << EOF
    #standardSQL
    CREATE SCHEMA IF NOT EXISTS \`${PROJECT_ID}.${DATASET_NAME}\`
    OPTIONS (
        location = 'US'
    );
    EOF
    ```
----


## Setup
- Prefer running commands from Cloud Shell as downloading data files from repository and uploading to Cloud Storage is faster.

Environment variables:
```bash
PROJECT_ID=

BUCKET_NAME=data-$PROJECT_ID
export BUCKET_URL=gs://$BUCKET_NAME

DATASET_NAME=rides_ny
TABLE_NAME=fhv_2019

TABLE_ID=$PROJECT_ID.$DATASET_NAME.$TABLE_NAME

gcloud config set project $PROJECT_ID
```

Copy data files into Cloud Storage:
```bash
chmod +x upload-csv-to-gs.sh
./upload-csv-to-gs.sh
```

Create dataset and tables:
```sql
#standardSQL
CREATE SCHEMA IF NOT EXISTS \`${PROJECT_ID}.${DATASET_NAME}\`
OPTIONS (
    location = 'US'
);

CREATE OR REPLACE EXTERNAL TABLE ${TABLE_ID}_external
OPTIONS (
  format = 'CSV',
  uris = ['${BUCKET_URL}/fhv_2019/*']
);

CREATE OR REPLACE TABLE ${TABLE_ID}
AS
SELECT * FROM ${TABLE_ID}_external;
```


## Working with queries

Selects:
```sql
#standardSQL
SELECT COUNT(DISTINCT affiliated_base_number)
FROM ${TABLE_ID}_external;

SELECT COUNT(DISTINCT affiliated_base_number)
FROM ${TABLE_ID};

SELECT COUNT(*)
FROM ${TABLE_ID}
WHERE
    PUlocationID IS NULL
    AND POlocationID IS NULL;
```

Partitioning and clustering:
```sql
#standardSQL
CREATE OR REPLACE TABLE ${TABLE_ID}_partitioned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY (affiliated_base_number)
AS
SELECT * FROM ${TABLE_ID};
```

Assessing non-partitioned table:
```sql
#standardSQL
SELECT COUNT(DISTINCT affiliated_base_number)
FROM ${TABLE_ID}
WHERE
    pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';
```

Assessing partitioned table:
```sql
#standardSQL
SELECT COUNT(DISTINCT affiliated_base_number)
FROM ${TABLE_ID}_partitioned_clustered
WHERE
    pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';
```

## Working with Parquet files
- reading of CSV files in this exercise is not optimized for commodity memory capacity (e.g. 4 GB). Consider using Cloud Shell offering an instance with 15 GB memory for free.
- using `fastparquet==2023.2.0` brought performance issues for the biggest Parquet file corresponding to fhv_tripdata_2019-01, causing excesive usage of memory and raising out-of-memory error in BigQuery slot when loading from external table. This was resolved using `pyarrow==11.0.0`.

Python environment:
```bash
python3.8 -m venv venv
source venv/bin/activate
pip install -r upload-parquet-to-gs.requirements.txt
```

Application credentials:
```bash
gcloud auth application-default login
```

Destination path in bucket:
```bash
DEST_PATH_PARQUET=fhv_2019_parquet
```

Running script:
- `upload-parquet-to-gs.py` enforces data types of `` and `` columns to avoid mismatching between files once saved as Parquet files. Read doc string of script for further explanation.
```bash
python upload-parquet-to-gs.py --bucket-name $BUCKET_NAME --dest-path $DEST_PATH_PARQUET
```

Creating table:
```sql
#standardSQL
CREATE OR REPLACE EXTERNAL TABLE ${TABLE_ID}_parquet_external
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://${BUCKET_NAME}/${DEST_PATH_PARQUET}/*.parquet']
);

CREATE OR REPLACE TABLE ${TABLE_ID}_parquet
AS
SELECT * FROM ${TABLE_ID}_parquet_external
```
