"""
Bronze layer batch-polling job: crypto.raw (Kafka) -> S3 Bronze.

Preserves the raw event payload plus Kafka's own metadata (topic, partition,
offset, broker timestamp) with minimal transformation - Bronze is the raw
landing zone, not where validation/typing happens (that's Silver, Phase 9).

NOT a Structured Streaming query. spark.readStream.format("kafka") on this
cluster silently returns numInputRows: 0 forever while reporting healthy,
advancing checkpoints - see CLAUDE.md gotcha #22 for the full investigation
(five ruled-out hypotheses, plus a real-but-unrelated executor SLF4J binding
bug found while DEBUG-tracing the connector). spark.read.format("kafka")
(batch) has been proven to work reliably in this exact environment, so this
job polls on a fixed interval with spark.read instead of a streaming query,
tracking per-partition offsets itself in a small JSON state file on S3
(bronze_batch_state/offsets.json) rather than relying on Spark's internal
streaming checkpoint.

Usage:
    spark-submit --deploy-mode cluster \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,software.amazon.msk:aws-msk-iam-auth:2.3.2,org.apache.kafka:kafka-clients:3.9.2 \
        bronze_job.py <kafka_bootstrap_servers> <s3_bucket>

NOTE on the --packages versions: this EMR release ships its own kafka-clients
and aws-msk-iam-auth jars bundled in __spark_libs__ (likely added for native
MSK integration). Originally pinned at 2.2.0/transitive-3.4.1, which silently
collided with EMR's bundled 2.3.2/3.9.2 on the executor classpath - both
versions loaded simultaneously, with no error anywhere. Explicitly requesting
the same versions EMR already bundles makes the two copies identical instead
of conflicting. If this EMR release ever changes its bundled versions, check
them first via SSM (`ls /path/to/__spark_libs__ | grep -E
"kafka-clients|msk-iam"` on the master) rather than assuming these stay
correct.
"""

import json
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from pyspark.sql.functions import max as spark_max

TOPIC = "crypto.raw"
NUM_PARTITIONS = 3  # fixed at topic creation, see scripts/create_kafka_topics.py
POLL_INTERVAL_SECONDS = 30


def read_state(spark, state_path):
    try:
        rows = spark.read.text(state_path).collect()
    except Exception:
        return None
    if not rows:
        return None
    return json.loads(rows[0]["value"])


def write_state(spark, state_path, state):
    spark.createDataFrame([(json.dumps(state),)], ["value"]).coalesce(1).write.mode(
        "overwrite"
    ).text(state_path)


def poll_once(spark, bootstrap_servers, bucket, state_path):
    state = read_state(spark, state_path)
    if state:
        # Spark's batch Kafka source requires every assigned partition to be
        # present once you use specific per-partition offsets - a partition
        # with no rows in a prior batch would otherwise be silently missing
        # from state and fail the next read with an AssertionError.
        topic_offsets = dict(state.get(TOPIC, {}))
        for partition in range(NUM_PARTITIONS):
            topic_offsets.setdefault(str(partition), 0)
        state[TOPIC] = topic_offsets
        starting_offsets = json.dumps(state)
    else:
        starting_offsets = "earliest"

    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
        .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
        .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("endingOffsets", "latest")
        .load()
        .cache()
    )

    if kafka_df.isEmpty():
        kafka_df.unpersist()
        print("[bronze-batch-poll] no new data this cycle", file=sys.stderr)
        return

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

    next_offsets = kafka_df.groupBy("partition").agg(spark_max("offset").alias("max_offset")).collect()

    bronze_df.write.format("parquet").option("path", f"s3://{bucket}/bronze/crypto/").partitionBy(
        "ingest_date", "ingest_hour"
    ).mode("append").save()

    new_state = dict(state) if state else {}
    topic_offsets = dict(new_state.get(TOPIC, {}))
    for row in next_offsets:
        topic_offsets[str(row["partition"])] = row["max_offset"] + 1
    new_state[TOPIC] = topic_offsets
    write_state(spark, state_path, new_state)

    kafka_df.unpersist()
    print(f"[bronze-batch-poll] wrote batch, new offsets: {new_state}", file=sys.stderr)


def main():
    if len(sys.argv) != 3:
        print("Usage: bronze_job.py <kafka_bootstrap_servers> <s3_bucket>", file=sys.stderr)
        sys.exit(1)

    bootstrap_servers, bucket = sys.argv[1], sys.argv[2]
    state_path = f"s3://{bucket}/checkpoints/spark/bronze_batch_state/offsets.json"

    spark = SparkSession.builder.appName("crypto-pipeline-bronze").getOrCreate()

    while True:
        cycle_start = time.time()
        try:
            poll_once(spark, bootstrap_servers, bucket, state_path)
        except Exception as e:
            print(f"[bronze-batch-poll] ERROR during poll cycle: {e}", file=sys.stderr)

        elapsed = time.time() - cycle_start
        time.sleep(max(0, POLL_INTERVAL_SECONDS - elapsed))


if __name__ == "__main__":
    main()
