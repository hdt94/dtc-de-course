import os

from prefect_gcp import GcpCredentials


GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCP_SA_CREDENTIALS_FILE = os.environ.get("GCP_SA_CREDENTIALS_FILE")

assert GCP_PROJECT, "Undefined 'GCP_PROJECT'"
assert GCP_SA_CREDENTIALS_FILE, "Undefined 'GCP_SA_CREDENTIALS_FILE'"

block = GcpCredentials(
    project=GCP_PROJECT,
    service_account_file=GCP_SA_CREDENTIALS_FILE,
)
block.save("gcp-prefect-credentials", overwrite=True)
