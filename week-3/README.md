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
    CREATE SCHEMA IF NOT EXISTS \`${PROJECT_ID}.${DATASET_NAME}\`
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
- `upload-parquet-to-gs.py` enforces data types of `PUlocationID` and `DOlocationID` columns to avoid mismatching between files once saved as Parquet files. Read doc string of script for further explanation.
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


## Comparison



## draft
```sql
bq query <<EOF
#standardSQL
CREATE OR REPLACE TABLE $TABLE_ID;
EOF
```
Error in query string: Error processing job 'dtc-dataeng-375600:bqjob_r7d3824d4b210b68a_000001863ca31c7b_1': No column
definitions in CREATE TABLE at [2:1]



```
CSV_FILES="""
[
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-02.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-03.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-04.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-05.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-06.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-07.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-08.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-09.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-11.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-12.csv.gz',
]
"""
```




Working

python3.9 -c 'urls = [f"https//{month:02}" for month in range(1,13)]; print(urls)'

echo "$(for month in $(seq -f '%02g' 1 12); do echo \'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-$month.csv.gz\',; done;)"






Not working
echo "$(for x in {1..3}; do echo \'$x\',; done;)"

echo "$(for x in $(seq -f '%02g' 1 12); do echo \'$x\',; done;)"

FORMAT=\'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-\$MONTH.csv.gz\',
echo "$(for month in $(seq -f '%02g' 1 12); do echo $FORMAT; done;)"





```sql
bq query <<EOF
#standardSQL
CREATE OR REPLACE EXTERNAL TABLE $TABLE_ID
OPTIONS (
  format = 'CSV',
  uris = ['https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-02.csv.gz']
);
EOF
```



```sql
bq query <<EOF
#standardSQL
CREATE TABLE $TABLE_ID (
  x INT64 OPTIONS (description = 'An optional INTEGER field'),
  y STRUCT <
    a ARRAY <STRING> OPTIONS (description = 'A repeated STRING field'),
    b BOOL
  >
) OPTIONS (
    expiration_timestamp = TIMESTAMP '2023-01-01 00:00:00 UTC',
    description = 'a table that expires in 2023',
    labels = [('org_unit', 'development')]);
EOF
```




Waiting on bqjob_r4bde01d159325a7e_000001863eed8a17_1 ... (16s) Current status: DONE
BigQuery error in query operation: Error processing job 'dtc-dataeng-375600:bqjob_r4bde01d159325a7e_000001863eed8a17_1': Resources exceeded during query execution: Out of memory. Failed to import values for
column 'DOlocationID' This might happen if the file contains a row that is too large, or if the total size of the pages loaded for the queried columns is too large.; Failed to read Parquet file /bigstore/data-dtc-dataeng-375600/fhv_2019_parquet/fhv_tripdata_2019-01.parquet. This might happen if the file contains a row that is too large, or if the total size of the pages loaded for the queried columns is too large.







## another section

fsspec==2023.1.0
gcsfs==2023.1.0



Not working
# bucket = storage.Client().bucket(bucket_url)



# with tempfile.TemporaryDirectory() as d:
#     dest_dir = Path(d)
#     for month in range(2, 3):
#         file_basename = f"fhv_tripdata_2019-{month:02}"
#         print(f"\n{file_basename}")

#         print("Downloading into memory...")
#         file_url = f"{BASE_URL}/{file_basename}.csv.gz"
#         df = pd.read_csv(file_url)

#         with tempfile.TemporaryFile('w') as f:
#         print("Writing Parquet file...")
#         file_path = dest_dir / f"{file_basename}.parquet"
#         df.to_parquet(file_path, compression='gzip')

#         # object_url = f"{bucket_url}/fhv_2019_parquet/{file_basename}.parquet"
#         # object_url = f"{bucket_url}/fhv_2019_parquet/{file_basename}.csv"
#         # print(f"Uploading into bucket: {object_url}")

#         print("Uploading to storage...")
#         blob = bucket.blob(f"fhv_2019_parquet/{file_basename}.parquet")
#         # blob = bucket.blob(f"fhv_2019_shell/{file_basename}.parquet")

#         blob.upload_from_filename(file_path)




# df.to_csv(object_url)
# df.to_parquet(object_url)

# with tempfile.TemporaryDirectory() as d:
#     dest_dir = Path(d)
#     for month in range(2, 3):
    # for month in range(1, 13):
        # file_basename = f"fhv_tripdata_2019-{month:02}"

        # file_url = f"{BASE_URL}/{file_basename}.csv.gz"
        # df = pd.read_csv(file_url)

        # # object_url = f"{bucket_url}/fhv_2019_parquet/{file_basename}.parquet"
        # object_url = f"{bucket_url}/fhv_2019_parquet/{file_basename}.csv"
        # df.to_csv(object_url)
        # # df.to_parquet(object_url)















## Another section

PERFECTLY WORKING!!!
bq query << EOF                                                                                                                                                  #standardSQL
CREATE OR REPLACE TABLE ${TABLE_ID}_partitioned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY (affiliated_base_number)
AS
SELECT * FROM ${TABLE_ID};
EOF

#standardSQL
CREATE OR REPLACE TABLE dtc-dataeng-375600.rides_ny.fhv_2019_partitioned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY (affiliated_base_number)
AS
SELECT * FROM dtc-dataeng-375600.rides_ny.fhv_2019;














BigQuery error in query operation: Error processing job 'dtc-dataeng-375600:bqjob_r4c5f693f3420d55_000001863d191eed_1': Invalid dataset ID "rides-ny". Dataset IDs must be alphanumeric (plus underscores) and
must be at most 1024 characters long.





#standardSQL
CREATE SCHEMA IF NOT EXISTS dtc-dataeng-375600.rides_ny
OPTIONS (
    location = 'US'
)
Error in query string: Error processing job 'dtc-dataeng-375600:bqjob_r39b24ad3e6fc1711_000001863d13749d_1': Syntax error: Expected end of input but got "-" at [2:32]





Tables does support hypenhs "-"









-- both are valid
CREATE OR REPLACE TABLE \`${TABLE_ID}\`
AS
SELECT * FROM ${TABLE_ID}_external;

-- CREATE OR REPLACE TABLE \`${TABLE_ID}\`
-- AS
-- SELECT * FROM \`${TABLE_ID}_external\`;


















Error:

CREATE OR REPLACE TABLE `dtc-dataeng-375600.rides_ny.fhv_2019_partitioned_clustered`
PARTITION BY pickup_datetime  -- TIMESTAMP
CLUSTER BY (affiliated_base_number)
AS
SELECT * FROM dtc-dataeng-375600.rides_ny.fhv_2019;

PARTITION BY expression must be DATE(<timestamp_column>), DATE(<datetime_column>), DATETIME_TRUNC(<datetime_column>, DAY/HOUR/MONTH/YEAR), a DATE column, TIMESTAMP_TRUNC(<timestamp_column>, DAY/HOUR/MONTH/YEAR), DATE_TRUNC(<date_column>, MONTH/YEAR), or RANGE_BUCKET(<int64_column>, GENERATE_ARRAY(<int64_value>, <int64_value>[, <int64_value>]))


Success:

CREATE OR REPLACE TABLE `dtc-dataeng-375600.rides_ny.fhv_2019_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY (affiliated_base_number)
AS
SELECT * FROM dtc-dataeng-375600.rides_ny.fhv_2019;