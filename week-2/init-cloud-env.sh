#!/bin/bash

set -e

if [[ -z "$GCP_PROJECT" ]]; then
    >&2 echo 'Undefined "GCP_PROJECT"'
    exit 1
fi
if [[ -z "$GCP_SA_CREDENTIALS_FILE" ]]; then
    >&2 echo 'Undefined "GCP_SA_CREDENTIALS_FILE"'
    exit 1
fi
if [[ -z "$BQ_DATASET" ]]; then
    >&2 echo 'Undefined "BQ_DATASET"'
    exit 1
fi
if [[ -z "$GCS_BUCKET" ]]; then
    >&2 echo 'Undefined "GCS_BUCKET"'
    exit 1
fi

BASE_DIR="$(realpath $(dirname $0))"
TERRAFORM_DIR=$BASE_DIR/terraform

# Provisioning
terraform -chdir=$TERRAFORM_DIR init
terraform -chdir=$TERRAFORM_DIR apply \
    -var "bq_dataset=$BQ_DATASET"\
    -var "gcs_data_lake_bucket=$GCS_BUCKET"\
    -var "project=$GCP_PROJECT"
TERRAFORM_OUTPUT="$(terraform -chdir="$TERRAFORM_DIR" output -json)"

# Service account credentials file
GCP_SA_PREFECT="$(jq -r '.prefect_sa.value' <<< "$TERRAFORM_OUTPUT")"
mkdir -p "$(dirname $GCP_SA_CREDENTIALS_FILE)"
gcloud iam service-accounts keys create "$GCP_SA_CREDENTIALS_FILE" --iam-account="$GCP_SA_PREFECT"
