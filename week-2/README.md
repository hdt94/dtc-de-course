# Week 2: Workflow orchestration with Prefect

Guiding references:
- https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_2_workflow_orchestration
- https://github.com/discdiver/prefect-zoomcamp
- https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_2_workflow_orchestration/homework.md


## Setup
- All following commands consider directory containing this README.md file as current directory.

### Google Cloud environment

Log in to setup default application credentials:
- Note that in a production environment a service account should be created for provisioning with Terraform aiming to restrict access instead of granting editor permissions with user login
```bash
gcloud auth application-default login
```

Define envvars:
- Note that `.secrets/` has been added to [.gitignore](/.gitignore)
```bash
export GCP_PROJECT=
export GCP_SA_CREDENTIALS_FILE="$PWD/.secrets/gcp_sa_prefect_credentials.json"
export BQ_DATASET="ny_taxis"
export GCS_BUCKET="dtc_data_lake_$GCP_PROJECT"
```

Update project:
```bash
gcloud config set project "$GCP_PROJECT"
```

Provision cloud infrastructure:
```bash
chmod +x init-cloud-env.sh
./init-cloud-env.sh
```
- shell script requires `jq` binary utility: https://stedolan.github.io/jq/download/


### Local environment

Create Python virtual environment:
```bash
# Using venv
python3 -m venv venv
source venv/bin/activate

# Using conda
conda create -n prefect python=3.9
conda activate prefect
```
Notes:
- Prefect requires `sqlite >= 3.24.0`, so, it may be required updating SQLite binary: https://docs.prefect.io/getting-started/installation/#sqlite
- Using a conda environment setups up a Python distribution that uses latest SQLite binary available on system.
- If working on a system with up to date SQLite system library, prefer Python standard virtual environment.


Install requirements using `pip`:
```bash
pip install -r requirements.txt
```
Notes:
- Although it is recommended to use `conda install` for installing packages in a conda environment, it may have severe performance problems that are not present when using `pip`.
- [requirements.txt](./requirements.txt) set `prefect==2.7.11` as using `prefect==2.7.7` -as the guiding course lectures do- may throw `alembic.script.revision.ResolutionError` on Ubuntu Xenial64.
- `pyarrow` is included in [requirements.txt](./requirements.txt) as a dependency for writing Pandas dataframes as Parquet files and for writing Pandas dataframes into BigQuery by using `pandas-gbq` package.

Create blocks and deployments in local Prefect:
```bash
# create blocks
export GCP_PROJECT=
export GCP_SA_CREDENTIALS_FILE=
prefect block register --file src/gcp_credentials_block.py

export GCS_BUCKET=
prefect block register --file src/gcs_bucket_block.py

# create deployments
python src/etl_gcs_to_bq_deployment.py
python src/etl_web_to_gcs_deployment.py
```


## Running flows

Running Prefect agent:
```
prefect agent start --work-queue default
```

Optionally, run Orion UI server:
```bash
prefect orion start

# Running on custom port
PREFECT_ORION_API_PORT=8080 prefect orion start --port 8080

# Running behind a proxy such as on Cloud Shell or virtual machine (e.g. WSL, Vagrant + VirtualBox)
PREFECT_ORION_API_PORT=8080 prefect orion start --host 0.0.0.0 --port 8080
```

Running deployments:
- WARNING: using `"color": "yellow:"` with local runner for `etl-web-to-gcs` flow may crash the running process using commodity memory capacity (e.g. 4 GB) due to Pandas gzip-decompresssion is performed in memory and yellow datasets are near x10 heavier than green datasets. Consider using Cloud Shell offering 15 GB for free.
- `"color": "green:"` and `"color": "yellow:"` are written to different tables using `etl-gcs-to-bq-multiple-months` considering schemas for each color differ.
```bash
prefect deployment run etl-web-to-gcs/parameterized \
    --params '{"color": "green", "year": 2020, "month": 1}'

prefect deployment run etl-web-to-gcs/parameterized \
    --params '{"color": "yellow", "year": 2019, "month": 2}'

prefect deployment run etl-web-to-gcs/parameterized \
    --params '{"color": "yellow", "year": 2019, "month": 3}'

prefect deployment run etl-gcs-to-bq-multiple-months/parameterized \
    --param "dest_table=$BQ_DATASET.rides_green_cabs" \
    --params '{"color": "green", "year": 2019, "months": [1]}'

prefect deployment run etl-gcs-to-bq-multiple-months/parameterized \
    --param "dest_table=$BQ_DATASET.rides_yellow_cabs" \
    --params '{"color": "yellow", "year": 2019, "months": [2, 3]}'

prefect deployment run etl-web-to-gcs/parameterized \
    --params '{"color": "green", "year": 2020, "month": 11}'

prefect deployment run etl-web-to-gcs/green-2019-04-slack-notify
```
