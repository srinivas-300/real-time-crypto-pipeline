"""
Silver layer streaming job: S3 Bronze -> S3 Silver (+ errors/crypto DLQ).

Reads Bronze via the file streaming source (not a second Kafka consumer),
parses the raw JSON event payload, validates each record, deduplicates by
event_id within a watermark window, and writes clean typed Parquet to Silver.
Records that fail validation are written to errors/crypto/ with the specific
reason(s) they failed, rather than silently dropped.

Usage (on a small dev cluster, dynamic allocation must be disabled with fixed,
modest executor sizing - otherwise Spark tries to scale executors to match
input file count, which a small cluster can never satisfy and the job hangs
forever waiting for resources):
    spark-submit --deploy-mode cluster \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.executor.instances=1 --conf spark.executor.cores=2 --conf spark.executor.memory=1g \
        --conf spark.sql.shuffle.partitions=4 \
        silver_job.py <s3_bucket>
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    from_json,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Deliberately excludes ingest_date/ingest_hour: those are partition columns
# (encoded in the S3 path, not stored inside the Parquet files themselves).
# Including partition columns in an explicit .schema() for a *streaming* file
# source silently breaks file discovery - the source finds zero rows with no
# error, even though the same schema works fine for a batch read. Neither
# column is needed by Silver's own logic anyway.
BRONZE_SCHEMA = StructType(
    [
        StructField("kafka_key", StringType()),
        StructField("kafka_value", StringType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("ingestion_timestamp", StringType()),
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("market_cap", DoubleType()),
        StructField("volume_24h", DoubleType()),
        StructField("price_change_24h", DoubleType()),
    ]
)


def main():
    if len(sys.argv) != 2:
        print("Usage: silver_job.py <s3_bucket>", file=sys.stderr)
        sys.exit(1)

    bucket = sys.argv[1]
    spark = (
        SparkSession.builder.appName("crypto-pipeline-silver")
        .config("spark.scheduler.mode", "FAIR")
        .getOrCreate()
    )

    bronze = (
        spark.readStream.schema(BRONZE_SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .parquet(f"s3://{bucket}/bronze/crypto/")
    )

    parsed = (
        bronze.withColumn("event", from_json(col("kafka_value"), EVENT_SCHEMA))
        .select(
            "event.event_id",
            to_timestamp("event.event_timestamp").alias("event_timestamp"),
            to_timestamp("event.ingestion_timestamp").alias("ingestion_timestamp"),
            "event.symbol",
            "event.price",
            "event.market_cap",
            "event.volume_24h",
            "event.price_change_24h",
            "kafka_partition",
            "kafka_offset",
        )
    )

    is_valid = (
        col("event_id").isNotNull()
        & col("symbol").isNotNull()
        & (col("symbol") != "")
        & col("price").isNotNull()
        & (col("price") > 0)
        & col("event_timestamp").isNotNull()
    )

    # concat_ws automatically skips null arguments, so this yields a clean
    # comma-separated list of only the reasons that actually failed (avoids an
    # earlier bug: array(when(...), when(...)) collapses to a NULL array - not
    # an array of nulls - when every when() is unmatched, and size(NULL) is -1
    # in Spark, not 0, which silently rejected every single row through the
    # size(...) == 0 filter this replaced).
    validation_errors = concat_ws(
        ",",
        when(col("event_id").isNull(), "missing_event_id"),
        when(col("symbol").isNull() | (col("symbol") == ""), "missing_symbol"),
        when(col("price").isNull() | (col("price") <= 0), "invalid_price"),
        when(col("event_timestamp").isNull(), "missing_event_timestamp"),
    )

    valid = parsed.filter(is_valid)
    invalid = parsed.filter(~is_valid).withColumn("validation_errors", validation_errors)

    # Watermarked dropDuplicates + append mode only emits a row once the watermark
    # advances past its event time (Spark's way of being sure no duplicate/late
    # arrival can still show up) - so this delay is also the minimum end-to-end
    # latency before data appears in Silver. 2 minutes balances real dedup/late-data
    # protection against staying meaningfully "near real-time" for this pipeline.
    silver = (
        valid.withWatermark("event_timestamp", "2 minutes")
        .dropDuplicates(["event_id"])
        .withColumn("event_date", to_date(col("event_timestamp")))
    )

    # This query's checkpoint MUST be on the cluster's local HDFS, not S3: the
    # dropDuplicates+watermark above is a stateful operator, and Spark's default
    # state store relies on atomic-rename filesystem semantics that S3 doesn't
    # actually provide (it silently breaks with "delta file ... does not exist"
    # under retries). HDFS provides those semantics; S3 does not. Output DATA
    # still goes to S3 - only the state checkpoint needs HDFS. Tradeoff: HDFS is
    # ephemeral to this cluster, so a cluster restart loses dedup state and
    # Silver reprocesses Bronze from scratch (self-consistent, just not free) -
    # documented as a known limitation, see docs/BUILD_LOG.md Phase 9.
    # Two queries in one Spark app default to FIFO scheduling, which can let one
    # query starve the other for cores on a small cluster (observed: the errors
    # query kept making progress while silver never got a turn). FAIR scheduling
    # with separate pools - Spark's documented pattern for concurrent streaming
    # queries - makes both queries actually share the available cores.
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver")
    silver_query = (
        silver.writeStream.format("parquet")
        .option("path", f"s3://{bucket}/silver/crypto/")
        .option("checkpointLocation", "/checkpoints/spark/silver/")
        .partitionBy("event_date", "symbol")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "errors")
    errors = invalid.withColumn("error_date", to_date(current_timestamp()))
    errors_query = (
        errors.writeStream.format("parquet")
        .option("path", f"s3://{bucket}/errors/crypto/")
        .option("checkpointLocation", f"s3://{bucket}/checkpoints/spark/errors/")
        .partitionBy("error_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    _ = (silver_query, errors_query)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
