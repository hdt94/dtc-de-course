import argparse
import os
import time

import pyspark.sql.functions as F

from pyspark.sql import SparkSession


def transform_for_kafka_sink(df):
    df = (
        df
        .withColumn("value", F.to_csv(F.struct(df.columns)))
        .withColumnRenamed("Affiliated_base_number", "key")
        .select(["key", "value"])
    )

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True, type=argparse.FileType('r'))
    parser.add_argument("-d", "--delay", default=2.5, type=float)
    args = parser.parse_args()

    file_path = os.path.realpath(args.file.name)
    sleep_delay = args.delay

    print(f"File path: {file_path}")

    spark = (
        SparkSession.builder
        .appName("app-ingest")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
        .getOrCreate()
    )

    df = (
        spark.read
        .format("parquet")
        .load(file_path)
        .limit(50)
        .withColumn("PUlocationID", F.expr("CAST(PUlocationID AS INTEGER)"))
        .withColumn("DOlocationID", F.expr("CAST(DOlocationID AS INTEGER)"))
    )
    df = transform_for_kafka_sink(df)

    kafka_writer_options = {
        "kafka.bootstrap.servers": "localhost:9092",
        "topic": "rides",
    }

    print("Publishing...")
    for row in df.collect():
        try:
            (
                spark.createDataFrame([row], schema=df.schema)
                .write
                .format("kafka")
                .options(**kafka_writer_options)
                .save()
            )
        except KeyboardInterrupt:
            print("\nCanceled by user")
        except Exception as ex:
            print(f"Failed to publish single message: {ex}")

        time.sleep(sleep_delay)

    print("Finished")
