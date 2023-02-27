# Week 5: Batch Processing with Spark

Guiding references:
- https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_5_batch_processing
- https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_5_batch_processing/homework.md

Datasets:
- https://github.com/DataTalksClub/nyc-tlc-data/releases/
- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz

Download datasets:
- script will download datasets into `./data` directory which has been added to `.gitignore`
```bash
chmod +x download_data.sh
./download_data.sh
```

Install Java 11 as shown in guiding videos: https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_5_batch_processing

Python environment:
- note that `pyspark` installs and setups up Spark and Py4J binaries + jars + paths
```bash
python3.8 -m venv venv
source venv/bin/activate
pip install -r run_analysis.requirements.txt
```

Run script:
```bash
# Analyzing datasets and saving into Parquet files
python run_analysis.py --source-dir data/ --analyze True --parquet-dest-dir data/parquet/

# Analyzing datasets
python run_analysis.py --source-dir data/ --analyze True

# Saving into Parquet files
python run_analysis.py --source-dir data/ --parquet-dest-dir data/parquet/
```
