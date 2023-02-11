#!/bin/bash

set -e

if [[ -z $BUCKET_URL ]]; then
    echo 'Undefined "BUCKET_URL"' >&2
    exit 1
fi

gsutil mb $BUCKET_URL

# Example file URL
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-02.csv.gz

BASE_URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv
FILES_NAMES=$(for MONTH in $(seq -f '%02g' 1 12); do echo fhv_tripdata_2019-$MONTH.csv.gz; done)

TEMP_DIR=$(mktemp -d -t dtc-files-XXXXX)

pushd $TEMP_DIR
for FILE in $FILES_NAMES
do
    echo "Downloading: $FILE"
    curl -LO "$BASE_URL/$FILE"
done
popd

gsutil rsync $TEMP_DIR $BUCKET_URL/fhv_2019
rm -r $TEMP_DIR
