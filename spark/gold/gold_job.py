"""
Gold layer streaming job: Iceberg table crypto_pipeline.silver_crypto ->
Iceberg table crypto_pipeline.gold_crypto.

Reads Silver as an Iceberg streaming source (not a second Kafka consumer, and
not a raw Parquet path+schema - Iceberg carries its own schema in the Glue
Catalog, so no manual schema/partition-column workaround is needed here the
way Silver once needed for reading raw Bronze Parquet), and computes 5-minute
tumbling-window aggregates per symbol: average/min/max price, average 24h
volume and 24h price change (both already rolling metrics from the source, so
an average is the meaningful per-window figure rather than a sum), and the
number of events observed in the window. Output is written to another Iceberg
table, registered in the same Glue Catalog.

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
        gold_job.py <s3_bucket>
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, max as spark_max, min as spark_min, to_date, window


def upsert_to_gold(micro_batch_df, batch_id):
    # MERGE, not a plain append - see silver_job.py's upsert_to_silver for why:
    # same root cause (HDFS checkpoint loss on cluster restart -> full source
    # reprocess -> duplicate rows under a plain append), same fix. Gold's key is
    # the window+symbol pair rather than event_id.
    micro_batch_df.createOrReplaceTempView("gold_updates")
    micro_batch_df.sparkSession.sql(
        """
        MERGE INTO glue_catalog.crypto_pipeline.gold_crypto t
        USING gold_updates s
        ON t.window_start = s.window_start AND t.symbol = s.symbol
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main():
    if len(sys.argv) != 2:
        print("Usage: gold_job.py <s3_bucket>", file=sys.stderr)
        sys.exit(1)

    bucket = sys.argv[1]
    spark = (
        SparkSession.builder.appName("crypto-pipeline-gold")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{bucket}/warehouse/")
        # HadoopFileIO (routes through Hadoop's already-proven S3AFileSystem / s3a://
        # stack), not Iceberg's native S3FileIO (AWS SDK v2-based): S3FileIO's
        # existence-check HEAD request (used by the streaming source to find its
        # initial offset) failed with a bare "Bad Request" (400, no error body) against
        # this EMR AMI's bundled AWS SDK v2 version - Silver's S3FileIO *writes* to its
        # own Iceberg table work fine, so this is narrowly a read-path/HeadObject issue,
        # not a blanket S3FileIO incompatibility. HadoopFileIO sidesteps it entirely by
        # using the same s3a:// implementation every other job in this pipeline already
        # relies on. Each Spark app's io-impl is independent, purely a client-side
        # config - Silver (the writer) keeps S3FileIO since its write path already works.
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .getOrCreate()
    )

    # CREATE TABLE IF NOT EXISTS is idempotent - safe to run on every job start.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS glue_catalog.crypto_pipeline.gold_crypto (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            symbol STRING,
            avg_price DOUBLE,
            min_price DOUBLE,
            max_price DOUBLE,
            avg_volume_24h DOUBLE,
            avg_price_change_24h DOUBLE,
            event_count BIGINT,
            window_date DATE
        )
        USING iceberg
        PARTITIONED BY (window_date, symbol)
        """
    )

    # Iceberg's streaming read only knows how to process pure-append snapshots by
    # default - it throws IllegalStateException rather than guess what an
    # overwrite/delete snapshot means for its incremental state. Hit this for real:
    # a one-off DQ cleanup DELETE on Silver (removing duplicate rows found after a
    # checkpoint-loss reprocess) produced exactly such a snapshot, and Gold's reader
    # correctly refused to process it. These options tell it to skip non-append
    # snapshots instead of failing - the right call here since Gold's own MERGE-based
    # write (see upsert_to_gold) never needs to see the skipped rows: a Silver DELETE
    # only removes rows that were already duplicates, so nothing new for Gold to
    # aggregate is lost by skipping the snapshot that removed them.
    silver = (
        spark.readStream.format("iceberg")
        .option("streaming-skip-overwrite-snapshots", "true")
        .option("streaming-skip-delete-snapshots", "true")
        .load("glue_catalog.crypto_pipeline.silver_crypto")
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
    # goes to S3 (via Iceberg); only the state checkpoint needs HDFS. Tradeoff:
    # a cluster restart loses in-flight window state and Gold reprocesses
    # Silver from scratch - same as Silver's own tradeoff. Reprocessing is
    # genuinely self-consistent now that the sink is upsert_to_gold's MERGE
    # rather than a plain append (see that function's docstring).
    #
    # Only one streaming query runs in this application, so the FIFO-vs-FAIR
    # scheduler starvation issue that affected Silver (two queries in one app)
    # doesn't apply here - no second query to starve against.
    gold_query = (
        gold.writeStream.foreachBatch(upsert_to_gold)
        .option("checkpointLocation", "/checkpoints/spark/gold_iceberg/")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    gold_query.awaitTermination()


if __name__ == "__main__":
    main()
