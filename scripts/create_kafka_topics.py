"""
One-time bootstrap script: creates the crypto.raw and crypto.dlq topics on the
crypto-pipeline-msk cluster using IAM authentication.

Must run from inside the VPC (MSK brokers are private) on a host whose IAM role
has kafka-cluster:CreateTopic on both topics (crypto-pipeline-kafka-admin-role).
Credentials come from botocore's standard chain (the host's IAM role) via
kafka-python's native AWS_MSK_IAM SASL mechanism - nothing to configure explicitly.

Usage:
    KAFKA_BOOTSTRAP_SERVERS="b-1.xxx:9098,b-2.xxx:9098" AWS_DEFAULT_REGION=us-east-1 \
        python create_kafka_topics.py

Note: AWS_DEFAULT_REGION must be set explicitly - the AWS_MSK_IAM SASL mechanism
resolves region via botocore's Session.get_config_variable('region'), which reads
that env var (or ~/.aws/config); it does not query EC2 instance metadata itself.
Without it, the failure surfaces as a generic KafkaTimeoutError on bootstrap rather
than a clear config error.
"""

import os
import sys

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def main():
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        print("KAFKA_BOOTSTRAP_SERVERS is not set", file=sys.stderr)
        sys.exit(1)

    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers.split(","),
        security_protocol="SASL_SSL",
        sasl_mechanism="AWS_MSK_IAM",
        client_id="crypto-pipeline-topic-bootstrap",
    )

    topics = [
        NewTopic(
            name="crypto.raw",
            num_partitions=3,
            replication_factor=2,
            topic_configs={"retention.ms": "86400000"},  # 24h - S3 Bronze is the system of record
        ),
        NewTopic(
            name="crypto.dlq",
            num_partitions=1,
            replication_factor=2,
            topic_configs={"retention.ms": "604800000"},  # 7 days - needs review time
        ),
    ]

    for topic in topics:
        try:
            admin.create_topics([topic])
            print(f"Created topic: {topic.name}")
        except TopicAlreadyExistsError:
            print(f"Topic already exists, skipping: {topic.name}")

    print("Existing topics on cluster:", admin.list_topics())
    admin.close()


if __name__ == "__main__":
    main()
