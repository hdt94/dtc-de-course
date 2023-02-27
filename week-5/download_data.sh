#!/bin/bash

set -e

DATASETS="
https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz
"

BASE_DIR=$(realpath $(dirname $0))

for url in $(echo "$DATASETS" | sed '/^$/d')
do
    echo "Downloading: ${url}"
    wget -qP "${BASE_DIR}/data/" $url
done
