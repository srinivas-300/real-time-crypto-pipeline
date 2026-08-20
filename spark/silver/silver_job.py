"""
Silver layer streaming job: S3 Bronze -> Iceberg table crypto_pipeline.silver_crypto
(+ errors/crypto DLQ, still plain Parquet).

Reads Bronze via the file streaming source (not a second Kafka consumer),
parses the raw JSON event payload, validates each record, deduplicates by
event_id within a watermark window, and writes clean typed rows to an Iceberg
table registered in the Glue Data Catalog (Glue itself acts as Iceberg's
catalog implementation - no crawler needed, Spark keeps Glue's table metadata
pointer in sync on every commit). Records that fail validation are written to
errors/crypto/ with the specific reason(s) they failed, rather than silently
dropped - this DLQ path is unrelated to the Silver table and stays plain
Parquet since it's not part of the Glue/Athena-queryable catalog.

Usage (on a small dev cluster, dynamic allocation must be disabled with fixed,
modest executor sizing - otherwise Spark tries to scale executors to match
input file count, which a small cluster can never satisfy and the job hangs
forever waiting for resources). The Iceberg Spark runtime and AWS SDK v2
bundle both ship pre-installed on this EMR release under /usr/share/aws/ -
no --packages/Maven fetch needed:
    spark-submit --deploy-mode cluster \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.executor.instances=1 --conf spark.executor.cores=2 --conf spark.executor.memory=1g \
        --conf spark.sql.shuffle.partitions=4 \
        --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.10.0-amzn-1.jar,/usr/share/aws/aws-java-sdk-v2/aws-sdk-java-bundle-2.42.12.jar \
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


def upsert_to_silver(micro_batch_df, batch_id):
    # MERGE, not a plain append: if the checkpoint is ever lost (e.g. an EMR
    # cluster restart - HDFS checkpoints don't survive that, see the note below)
    # and this query reprocesses Bronze's full history again, WHEN NOT MATCHED
    # THEN INSERT means already-committed rows are silently skipped instead of
    # duplicated. Found the hard way: a checkpoint loss during a pause/resume
    # cycle left ~40% of Silver's rows as exact duplicates before this existed.
    micro_batch_df.createOrReplaceTempView("silver_updates")
    micro_batch_df.sparkSession.sql(
        """
        MERGE INTO glue_catalog.crypto_pipeline.silver_crypto t
        USING silver_updates s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main():
    if len(sys.argv) != 2:
        print("Usage: silver_job.py <s3_bucket>", file=sys.stderr)
        sys.exit(1)

    bucket = sys.argv[1]
    spark = (
        SparkSession.builder.appName("crypto-pipeline-silver")
        .config("spark.scheduler.mode", "FAIR")
        # This app runs two concurrent streaming queries (silver_query,
        # errors_query) against the same S3 bucket. Hadoop's default
        # FileSystem cache shares one S3AFileSystem object per bucket+user
        # across both queries' checkpoint managers - when one query's commit
        # closes that shared object mid-batch, the other sees it as null
        # (NullPointerException in S3AFileSystem.getStore(), or
        # IllegalStateException: FlagSet is immutable), killing both queries
        # at once. Disabling the cache gives each query its own instance.
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{bucket}/warehouse/")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )

    # CREATE TABLE IF NOT EXISTS is idempotent - safe to run on every job start
    # (a plain step restart is a no-op here; only a genuinely fresh table gets
    # created). Explicit column list + PARTITIONED BY, rather than letting
    # writeStream.toTable() auto-create an unpartitioned table on first write.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS glue_catalog.crypto_pipeline.silver_crypto (
            event_id STRING,
            event_timestamp TIMESTAMP,
            ingestion_timestamp TIMESTAMP,
            symbol STRING,
            price DOUBLE,
            market_cap DOUBLE,
            volume_24h DOUBLE,
            price_change_24h DOUBLE,
            kafka_partition INT,
            kafka_offset BIGINT,
            event_date DATE
        )
        USING iceberg
        PARTITIONED BY (event_date, symbol)
        """
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
    # still goes to S3 (via Iceberg) - only the state checkpoint needs HDFS.
    # Tradeoff: HDFS is ephemeral to this cluster, so a cluster restart loses
    # dedup state and Silver reprocesses Bronze from scratch - documented as a
    # known limitation, see BUILD_LOG.md Phase 9. Reprocessing itself is genuinely
    # self-consistent now that the sink is upsert_to_silver's MERGE rather than a
    # plain append (see that function's docstring) - the earlier "self-consistent"
    # claim was true for the original plain-Parquet sink but silently stopped
    # being true once this moved to Iceberg's persistent, accumulating table with
    # unconditional appends, until the MERGE fix.
    # Two queries in one Spark app default to FIFO scheduling, which can let one
    # query starve the other for cores on a small cluster (observed: the errors
    # query kept making progress while silver never got a turn). FAIR scheduling
    # with separate pools - Spark's documented pattern for concurrent streaming
    # queries - makes both queries actually share the available cores.
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver")
    silver_query = (
        silver.writeStream.foreachBatch(upsert_to_silver)
        .option("checkpointLocation", "/checkpoints/spark/silver_iceberg/")
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
