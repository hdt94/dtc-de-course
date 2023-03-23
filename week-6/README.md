# Week 6: Stream Processing with Kafka + Spark Structured Streaming

Guiding references:
- https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_6_stream_processing
- https://docs.confluent.io/platform/current/clients/index.html
- https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
- https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

Download data:
```bash
wget -q -P data/ https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-02.parquet
```

Start single Kafka broker + Zookeper:
- WARNING: [docker-compose.yaml](./docker-compose.yaml) is a minimal local unsecure configuration not suitable for production
- https://github.com/confluentinc/cp-all-in-one
```bash
# Using Compose v2
docker compose up

# Using Compose v1
docker-compose up
```

Local virtual environment:
```bash
python3.8 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create Kafka topics: "rides", "rides_metrics", "rides_mirror"
```bash
python src/kafka/create_topics.py
```

Consume Kafka messages in console for debugging:
```bash
# subscribe to single topic (one per shell session)
python src/kafka/consume_in_console.py -t rides
python src/kafka/consume_in_console.py -t rides_metrics
python src/kafka/consume_in_console.py -t rides_mirror

# subscribe to multiple topics
python src/kafka/consume_in_console.py -t rides rides_metrics rides_mirror
```

Start processing with Spark Structured Streaming consuming from "rides" topic and producing into "rides_metrics" and "rides_mirror" topics:
```bash
python src/transform.py --metrics --checkpoint-location ./checkpoints/streaming/metrics
python src/transform.py --mirror --checkpoint-location ./checkpoints/streaming/mirror
```

Ingest data into "rides" topic using Spark Structured Streaming producing one event/message for each record in file:
```bash
python src/ingest.py -f data/fhv_tripdata_2019-02.parquet -d 5
```
