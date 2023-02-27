import argparse
import os

import pyspark.sql.types as T

from pyspark.sql import SparkSession


def analyze(spark):
    print("\nAnalyzing datasets...")

    spark.sql("""
        SELECT
            15 AS day,
            COUNT(*) AS total_rides
        FROM rides
        WHERE EXTRACT(DAY FROM pickup_datetime) = 15
    """).show()

    spark.sql("""
        SELECT
            EXTRACT(DAY FROM pickup_datetime) AS pickup_day,
            MAX(
                (UNIX_TIMESTAMP(dropoff_datetime) - UNIX_TIMESTAMP(pickup_datetime))
                /
                3600
            ) AS duration_longest_hours
        FROM rides
        WHERE
            pickup_datetime IS NOT NULL
            AND dropoff_datetime IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 4
    """).show()

    spark.sql("""
        WITH pickup_locations AS (
            SELECT
                PULocationID,
                COUNT(PULocationID) AS location_count
            FROM rides
            GROUP BY 1
        )
        SELECT
            pickup_locations.location_count,
            pickup_locations.PULocationID,
            zones.*
        FROM pickup_locations
        JOIN zones
            ON pickup_locations.PULocationID = zones.LocationID
        ORDER BY 1 DESC
        LIMIT 5
    """).show()


def read_files(spark, source_dir, sql_views=True):
    # schema fields order must match CSV headers order
    rides_schema = T.StructType([
        T.StructField('dispatching_base_num', T.StringType(), True),
        T.StructField('pickup_datetime', T.TimestampType(), True),
        T.StructField('dropoff_datetime', T.TimestampType(), True),
        T.StructField('PULocationID', T.IntegerType(), True),
        T.StructField('DOLocationID', T.IntegerType(), True),
        T.StructField('SR_Flag', T.IntegerType(), True),
        T.StructField('Affiliated_base_number', T.StringType(), True),
    ])
    zones_schema = T.StructType([
        T.StructField('LocationID', T.IntegerType(), True),
        T.StructField('Borough', T.StringType(), True),
        T.StructField('Zone', T.StringType(), True),
        T.StructField('service_zone', T.StringType(), True)
    ])

    print("\nReading files...")
    rides_df = spark.read.csv(
        f"{source_dir}/fhvhv_tripdata_2021-06.csv.gz",
        header=True,
        schema=rides_schema
    )
    zones_df = spark.read.csv(
        f"{source_dir}/taxi_zone_lookup.csv",
        header=True,
        schema=zones_schema
    )

    if sql_views:
        rides_df.createOrReplaceTempView('rides')
        zones_df.createOrReplaceTempView('zones')

    return (rides_df, zones_df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-dir', required=True)
    parser.add_argument('--analyze', default=False, required=False)
    parser.add_argument('--parquet-dest-dir', default=None, required=False)
    args = parser.parse_args()

    spark = (
        SparkSession
        .builder
        .master('local[*]')
        .appName('tcl-analysis')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    (rides_df, _) = read_files(spark, args.source_dir, sql_views=args.analyze)

    if args.analyze:
        analyze(spark)

    if args.parquet_dest_dir:
        print("\nRepartitioning dataframe...")
        # repartition using pickup_datetime lowers files' sizes but not suitable for homework
        rides_df = rides_df.repartition(12)

        print("\nSaving dataframe into Parquet files...")
        rides_df.write.parquet(args.parquet_dest_dir, mode='overwrite')
