"""
Bronze layer streaming job: crypto.raw (Kafka) -> S3 Bronze.

Preserves the raw event payload plus Kafka's own metadata (topic, partition,
offset, broker timestamp) with minimal transformation - Bronze is the raw
landing zone, not where validation/typing happens (that's Silver, Phase 9).

Usage:
    spark-submit --deploy-mode cluster \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,software.amazon.msk:aws-msk-iam-auth:2.2.0 \
        bronze_job.py <kafka_bootstrap_servers> <s3_bucket>
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format


def main():
    if len(sys.argv) != 3:
        print("Usage: bronze_job.py <kafka_bootstrap_servers> <s3_bucket>", file=sys.stderr)
        sys.exit(1)

    bootstrap_servers, bucket = sys.argv[1], sys.argv[2]

    spark = SparkSession.builder.appName("crypto-pipeline-bronze").getOrCreate()

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
        .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
        .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
        .option("subscribe", "crypto.raw")
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze_df = (
        kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("kafka_value"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("ingest_date", date_format(col("kafka_timestamp"), "yyyy-MM-dd"))
        .withColumn("ingest_hour", date_format(col("kafka_timestamp"), "HH"))
    )

    query = (
        bronze_df.writeStream.format("parquet")
        .option("path", f"s3://{bucket}/bronze/crypto/")
        .option("checkpointLocation", f"s3://{bucket}/checkpoints/spark/bronze/")
        .partitionBy("ingest_date", "ingest_hour")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
