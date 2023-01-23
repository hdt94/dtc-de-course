
import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def download_csv_dataset(url):
    if url.endswith(".csv.gz"):
        file_path = "dataset.csv.gz"
    elif url.endswith(".csv"):
        file_path = "dataset.csv"
    else:
        raise ValueError("Unsupported file extension")

    os.system(f"wget -q {url} -O {file_path}")

    return file_path


def preprocess_in_place(df):
    datetime_cols = [
        # yellow taxi
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",

        # green taxi
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime"
    ]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M:%S")


def main(user, password, host, port, db, table_name, url, chunksize=100_000):

    file_path = download_csv_dataset(url)

    # Using DataFrame iterator as an example to avoid demanding too many resources for greater-size files
    df_iter = pd.read_csv(file_path, iterator=True, chunksize=chunksize)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    conn = engine.connect()

    # insert first chunk by replacing table if existing
    t_start = time()
    df = next(df_iter)
    preprocess_in_place(df)
    df.to_sql(name=table_name, con=conn, if_exists="replace")
    t_end = time()
    print(f"Inserted first chunk, took {t_end - t_start:.3f} second")

    # insert resting chunks by appending data
    t_start = time()  # here to also time next chunk reading
    for df in df_iter:
        preprocess_in_place(df)
        df.to_sql(name=table_name, con=conn, if_exists="append")

        t_end = time()
        print(f"Inserted another chunk, took {t_end - t_start:.3f} second")
        t_start = time()  # here to also time next chunk reading

    print("Finished ingesting data into the Postgres database")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest NY Taxi data to Postgres")

    parser.add_argument("--user", required=True, help="user name for Postgres")
    parser.add_argument("--password", required=True, help="password for Postgres")
    parser.add_argument("--host", required=True, help="host for Postgres")
    parser.add_argument("--port", required=True, help="port for Postgres")
    parser.add_argument("--db", required=True, help="database name for Postgres")
    parser.add_argument("--table_name", required=True, help="name of the table where we will write the results to")
    parser.add_argument("--url", required=True, help="url of the file")

    args = parser.parse_args()
    main(**vars(args))
