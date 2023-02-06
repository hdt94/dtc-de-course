import os

from prefect_gcp import GcpCredentials, GcsBucket


GCS_BUCKET = os.environ.get("GCS_BUCKET")

assert GCS_BUCKET, "Undefined 'GCS_BUCKET'"

gcp_credentials = GcpCredentials.load("gcp-prefect-credentials")
gcs_bucket = GcsBucket(
    bucket=GCS_BUCKET,
    gcp_credentials=gcp_credentials
)
gcs_bucket.save("gcs-datalake-bucket", overwrite=True)
