import argparse
import os

import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql import SparkSession


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_CHECKPOINT_LOCATION = os.path.realpath("./checkpoints/streaming/")

RIDE_FHV_SCHEMA = T.StructType([
    T.StructField("dispatching_base_num", T.StringType()),
    T.StructField("pickup_datetime", T.TimestampType()),
    T.StructField("dropoff_datetime", T.TimestampType()),
    # T.StructField("pulocationid", T.DoubleType()),
    # T.StructField("dolocationid", T.DoubleType()),
    T.StructField("pulocationid", T.IntegerType()),
    T.StructField("dolocationid", T.IntegerType()),
    T.StructField("sr_flag", T.DoubleType()),
    T.StructField("affiliated_base_number", T.StringType()),
])


def sink_kafka(
    df,
    topic,
    output_mode,
    checkpoint_location,
    key_col=None,
    query_name=None,
):
    df = df.withColumn("value", F.to_csv(F.struct(df.columns)))

    if key_col is None:
        df = df.select("value")
    else:
        df = (
            df
            .withColumn("key", F.expr(f"CAST({key_col} AS STRING)"))
            .select("key", "value")
        )

    if query_name is None:
        query_name = topic

    kafka_sink_options = {
        # required
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "checkpointLocation": checkpoint_location,
        "topic": topic,

        # optional
        # https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#batch-size
        "kafka.batch.size": 250,  # bytes

        # optional
        # https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#linger-ms
        "kafka.linger.ms": 0,
    }

    try:
        query = (
            df.writeStream
            .queryName(query_name)
            .outputMode(output_mode)
            .format("kafka")
            .options(**kafka_sink_options)
            .start()
        )
    except Exception as ex:
        print("Exception creating stream writer:")
        raise(ex)

    return query


def stream_from_rides(spark):
    # https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-streaming-queries
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "rides")
        .load()
        .selectExpr("timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)")
        .withColumn("value", F.from_csv("value", RIDE_FHV_SCHEMA.simpleString()))
        .select("key", "timestamp", "value.*")
    )

    return stream_df


def stream_to_metrics(stream_df, checkpoint_location):
    metrics_df = (
        stream_df
        .withWatermark("timestamp", "15 seconds")
        .groupby(F.window("timestamp", "10 seconds"), "pulocationid")
        .count()
        .select("window.start", "window.end", "pulocationid", "count")
    )

    return sink_kafka(
        metrics_df,
        topic="rides_metrics",
        output_mode="update",
        checkpoint_location=checkpoint_location,
        key_col="pulocationid",
    )


def stream_to_mirror(stream_df, checkpoint_location):
    return sink_kafka(
        stream_df,
        topic="rides_mirror",
        output_mode="append",
        checkpoint_location=checkpoint_location,
        key_col="pulocationid",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--checkpoint-location', required=True)
    parser.add_argument('--metrics', default=False, action='store_true')
    parser.add_argument('--mirror', default=False, action='store_true')
    args = parser.parse_args()
    checkpoint_location = os.path.realpath(args.checkpoint_location)

    if args.metrics is True and args.mirror is True:
        print("Use --metrics or --mirror flags")
        exit()
    elif args.metrics is False and args.mirror is False:
        if __debug__:
            args.metrics = False
            args.mirror = True
        else:
            print("Use --metrics or --mirror flags")
            exit()

    spark = (
        SparkSession.builder
        .appName("spark-streaming")
        .master('local[*]')
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
        .getOrCreate()
    )
    stream_df = stream_from_rides(spark)

    if args.mirror:
        # mirror_query = stream_to_mirror(stream_df)
        write_query = stream_to_mirror(stream_df, checkpoint_location)
    elif args.metrics:
        # metrics_query = stream_to_metrics(stream_df)
        write_query = stream_to_metrics(stream_df, checkpoint_location)
    else:
        print("Unknown input arguments condition")
        exit()

    try:
        write_query.awaitTermination()
        # spark.streams.awaitAnyTermination()
        # metrics_query.awaitTermination()
        # mirror_query.awaitTermination()
    except KeyboardInterrupt:
        print(f"\nCancelled by user")

    print(f"Finishing process...")
