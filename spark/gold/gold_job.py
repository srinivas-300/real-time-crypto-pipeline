"""
Gold layer streaming job: S3 Silver -> S3 Gold.

Reads Silver via the file streaming source (not a second Kafka consumer),
and computes 5-minute tumbling-window aggregates per symbol: average/min/max
price, average 24h volume and 24h price change (both already rolling metrics
from the source, so an average is the meaningful per-window figure rather
than a sum), and the number of events observed in the window.

Usage (on a small dev cluster, dynamic allocation must be disabled with fixed,
modest executor sizing - otherwise Spark tries to scale executors to match
input file count, which a small cluster can never satisfy and the job hangs
forever waiting for resources):
    spark-submit --deploy-mode cluster \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.executor.instances=1 --conf spark.executor.cores=2 --conf spark.executor.memory=1g \
        --conf spark.sql.shuffle.partitions=4 \
        gold_job.py <s3_bucket>
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, max as spark_max, min as spark_min, to_date, window
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

# Deliberately excludes event_date/symbol: those are Silver's partition columns
# (encoded in the S3 path, not stored inside the Parquet files themselves).
# Including partition columns in an explicit .schema() for a *streaming* file
# source silently breaks file discovery - the source finds zero rows with no
# error (see CLAUDE.md gotcha #6). Spark still recovers both columns from the
# path itself, so `symbol` remains available below for the groupBy.
SILVER_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_timestamp", TimestampType()),
        StructField("ingestion_timestamp", TimestampType()),
        StructField("price", DoubleType()),
        StructField("market_cap", DoubleType()),
        StructField("volume_24h", DoubleType()),
        StructField("price_change_24h", DoubleType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
    ]
)


def main():
    if len(sys.argv) != 2:
        print("Usage: gold_job.py <s3_bucket>", file=sys.stderr)
        sys.exit(1)

    bucket = sys.argv[1]
    spark = SparkSession.builder.appName("crypto-pipeline-gold").getOrCreate()

    silver = (
        spark.readStream.schema(SILVER_SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .parquet(f"s3://{bucket}/silver/crypto/")
    )

    # Same 2-minute watermark as Silver's own dedup - it's the pipeline's
    # established tolerance for out-of-order arrival, and it must be at least
    # that large here too or windows would close before Silver's own late
    # data could ever reach Gold.
    gold = (
        silver.withWatermark("event_timestamp", "2 minutes")
        .groupBy(window(col("event_timestamp"), "5 minutes"), col("symbol"))
        .agg(
            avg("price").alias("avg_price"),
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            avg("volume_24h").alias("avg_volume_24h"),
            avg("price_change_24h").alias("avg_price_change_24h"),
            count("*").alias("event_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "symbol",
            "avg_price",
            "min_price",
            "max_price",
            "avg_volume_24h",
            "avg_price_change_24h",
            "event_count",
        )
        .withColumn("window_date", to_date(col("window_start")))
    )

    # Windowed aggregation is a stateful operator, same as Silver's
    # dropDuplicates - the checkpoint MUST be local HDFS, not S3, for the same
    # atomic-rename-semantics reason (CLAUDE.md gotcha #7). Output DATA still
    # goes to S3; only the state checkpoint needs HDFS. Tradeoff: a cluster
    # restart loses in-flight window state and Gold reprocesses Silver from
    # scratch (self-consistent, just not free) - same as Silver's own tradeoff.
    #
    # Only one streaming query runs in this application, so the FIFO-vs-FAIR
    # scheduler starvation issue that affected Silver (two queries in one app)
    # doesn't apply here - no second query to starve against.
    gold_query = (
        gold.writeStream.format("parquet")
        .option("path", f"s3://{bucket}/gold/crypto/")
        .option("checkpointLocation", "/checkpoints/spark/gold/")
        .partitionBy("window_date", "symbol")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    gold_query.awaitTermination()


if __name__ == "__main__":
    main()
