"""
Upload Parquet files to Google Cloud Storage.

Script sets data type of "PUlocationID" and "DOlocationID" columns as float to
avoid mismatching in Parquet files given the following considerations:

- actual data type of such columns is integer

- most CSV source files contains null values except at least for month 05

- Pandas sets float dtype when an integer-dtype column contains null values

- not explicitly setting dtype to float for such columns will result in some
Parquet files with float dtype and at least one Parquet file with integer type
"""

import argparse
import tempfile

import pandas as pd

from google.cloud import storage


# Example file URL
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-02.csv.gz

BASE_URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"

if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--bucket-name", required=True)
    argparser.add_argument("--dest-path", required=True)
    args = argparser.parse_args()

    bucket = storage.Client().bucket(args.bucket_name)
    bucket_dest_path = args.dest_path

    for month in range(1, 13):
        file_basename = f"fhv_tripdata_2019-{month:02}"
        print(f"\n{file_basename}")

        print("Downloading into memory...")
        file_url = f"{BASE_URL}/{file_basename}.csv.gz"
        df = pd.read_csv(
            file_url,
            # set PUlocationID and DOlocationID dtype to avoid mismatching
            dtype={"PUlocationID": float, "DOlocationID": float}
        )

        with tempfile.NamedTemporaryFile(mode='w+b') as local_file:
            print("Writing Parquet file...")
            df.to_parquet(local_file.name, compression='gzip')

            print("Uploading to storage...")
            blob = bucket.blob(f"{bucket_dest_path}/{file_basename}.parquet")
            blob.upload_from_filename(local_file.name)
