"""
Upload Parquet files to Cloud Storage and create BigQuery tables.

Script sets data type of integer columns explicitly to Int64 to avoid
mismatching in Parquet files given the following considerations:

- actual data type of such columns is integer

- most CSV source files contains null values except at least for one month

- Pandas sets float dtype when an integer-dtype column contains null values

- not explicitly setting dtype to float for such columns will result in some
Parquet files with float dtype and at least one Parquet file with integer type
"""

import argparse
import tempfile

import pandas as pd

from google.cloud import bigquery, storage


# Example source files URLs
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-02.csv.gz
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-02.csv.gz

BASE_URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"


def create_table(bq_client, bq_dataset_id, category, bucket_path):
    partition_fields = {
        'fhv': 'Pickup_datetime',
        'green': 'lpep_pickup_datetime',
        'yellow': 'tpep_pickup_datetime',
    }
    assert category in partition_fields, f'Invalid category "{category}"'

    table_id = f"{bq_dataset_id}.{category}_all"
    query = f"""
    #standardSQL
    CREATE OR REPLACE EXTERNAL TABLE `{table_id}_external`
    OPTIONS (
        format = 'PARQUET',
        uris = ['gs://{bucket_path}/*.parquet']
    );

    CREATE OR REPLACE TABLE `{table_id}`
    AS
    SELECT * FROM `{table_id}_external`;
    """

    print(f"\nCreating tables `{table_id}_external` and `{table_id}`...")
    bq_client.query(query).result()


def load_files(bucket, category, year, bucket_sub_path):
    assert category in ['fhv', 'green', 'yellow'], f'Invalid category "{category}"'

    # enforce dtypes
    dtype = {"PUlocationID": 'Int64', "DOlocationID": 'Int64'}
    if category in ['green', 'yellow']:
        dtype['passenger_count'] = float
        dtype['payment_type'] = 'Int64'
        dtype['RatecodeID'] = 'Int64'
        dtype['VendorID'] = 'Int64'

        if category == 'green':
            dtype['trip_type'] = 'Int64'

    for month in range(1, 13):
        file_basename = f"{category}_tripdata_{year}-{month:02}"
        print(f"\n{file_basename}")

        print("Downloading into memory...")
        file_url = f"{BASE_URL}/{category}/{file_basename}.csv.gz"
        df = pd.read_csv(
            file_url,
            dtype=dtype
        )

        with tempfile.NamedTemporaryFile(mode='w+b') as local_file:
            print("Writing Parquet file...")
            df.to_parquet(local_file.name, compression='gzip')

            print("Uploading to storage...")
            blob = bucket.blob(f"{bucket_sub_path}/{file_basename}.parquet")
            blob.upload_from_filename(local_file.name)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--bucket-name", required=True)
    argparser.add_argument("--bq-dataset-id", required=True)
    args = argparser.parse_args()

    bq_client = bigquery.Client()
    bq_dataset_id = args.bq_dataset_id
    bucket = storage.Client().bucket(args.bucket_name)

    datasets = [
        ('fhv', [2019]),
        ('green', [2019, 2020]),
        ('yellow', [2019, 2020])
    ]
    for (category, years) in datasets:
        bucket_sub_path = f"{category}_all"
        for year in years:
            bucket_path = load_files(bucket, category, year, bucket_sub_path)

        bucket_path = f"{bucket.name}/{bucket_sub_path}"
        create_table(bq_client, bq_dataset_id, category, bucket_path)
