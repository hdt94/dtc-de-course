import tempfile

from pathlib import Path

import pandas as pd

from prefect import flow, task
from prefect_gcp import GcpCredentials, GcsBucket


@task()
def extract_from_gcs(dataset: str, dest_dir: Path) -> Path:
    """Download trip data from GCS"""

    file_name = f"{dataset}.parquet"

    gcs_block = GcsBucket.load("gcs-datalake-bucket")
    gcs_block.get_directory(from_path=file_name, local_path=dest_dir)

    file_path = dest_dir / file_name

    return file_path


@task()
def write_bq(path: Path, dest_table: str) -> None:
    """Write DataFrame to BiqQuery"""

    credentials = GcpCredentials.load("gcp-prefect-credentials")

    df = pd.read_parquet(path)
    df.to_gbq(
        destination_table=dest_table,
        project_id=credentials.project,
        credentials=credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

    return len(df)


@flow(log_prints=True)
def etl_gcs_to_bq(
    color: str,
    year: int,
    month: int,
    dest_table: str,
):
    """Main ETL flow to load data into Big Query"""

    dataset = f"{color}_tripdata_{year}-{month:02}"
    with tempfile.TemporaryDirectory() as d:
        dest_dir = Path(d)
        file_path = extract_from_gcs(dataset, dest_dir)
        row_count = write_bq(file_path, dest_table)

    return row_count


@flow(log_prints=True)
def etl_gcs_to_bq_multiple_months(
    color: str,
    year: int,
    months: list[int],
    dest_table: str,
):
    """Wrapper flow for loading mulitple datasets to BigQuery"""

    total_row_count = 0
    for month in months:
        row_count = etl_gcs_to_bq(color, year, month, dest_table)
        total_row_count += row_count

    print(f"Total row count: {total_row_count}")
