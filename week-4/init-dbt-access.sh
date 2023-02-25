#!/bin/bash

set -e

if [[ -z $PROJECT_ID ]]; then
    echo 'Undefined PROJECT_ID' >&2
    exit
fi
if [[ -z $DBT_SA ]]; then
    echo 'Undefined DBT_SA' >&2
    exit
fi
if [[ -z $DATASET_DEV_NAME ]]; then
    echo 'Undefined DATASET_DEV_NAME' >&2
    exit
fi
if [[ -z $DATASET_PROD_NAME ]]; then
    echo 'Undefined DATASET_PROD_NAME' >&2
    exit
fi


echo "Updating IAM policies at project-level..."

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
            --member="serviceAccount:${DBT_SA}" \
            --role='roles/bigquery.jobUser'


echo "Updating IAM policies at datasets-level..."

JQ_UPDATE_PATH=$(cat <<EOF
.access +=[
    {"role":"roles/bigquery.dataEditor", "userByEmail": "$DBT_SA"},
    {"role":"roles/bigquery.user", "userByEmail": "$DBT_SA"}
]
EOF
)

for dataset_name in "${DATASET_DEV_NAME}" "${DATASET_PROD_NAME}"
do
    IAM_FILE=$(mktemp --suffix=".${dataset_name}.json")

    bq show --format=prettyjson "${PROJECT_ID}:${dataset_name}" \
    | jq "$JQ_UPDATE_PATH" > "$IAM_FILE"
    
    bq update --source "$IAM_FILE" "${PROJECT_ID}:${dataset_name}"
    rm "$IAM_FILE"
done
