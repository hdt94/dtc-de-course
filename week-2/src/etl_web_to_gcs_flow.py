import tempfile

from datetime import timedelta
from pathlib import Path

import pandas as pd

from prefect import flow, task
from prefect.tasks import task_input_hash

from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""

    print(f"Fetching dataframe from: {dataset_url}")
    df = pd.read_csv(dataset_url)
    return df


@task()
def preprocess(df_base: pd.DataFrame) -> pd.DataFrame:
    df = df_base.copy()

    # casting datetime columns
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

    print(f"rows: {len(df)}")

    return df


@task()
def write_gcs(file_path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-datalake-bucket")
    gcs_block.upload_from_path(from_path=file_path)


@task()
def write_local(dest_dir: Path, dataset: str, df: pd.DataFrame) -> Path:
    """Write DataFrame out locally as parquet file"""
    file_path = dest_dir / f"{dataset}.parquet"
    df.to_parquet(file_path, compression="gzip")

    print(f"Local file: {file_path}")

    return file_path


@flow(log_prints=True)
def etl_web_to_gcs(
    color: str,
    year: int,
    month: int
) -> None:
    dataset = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset}.csv.gz"

    df = fetch(dataset_url)
    df = preprocess(df)

    with tempfile.TemporaryDirectory() as d:
        dest_dir = Path(d)
        file_path = write_local(dest_dir, dataset, df)
        write_gcs(file_path)
