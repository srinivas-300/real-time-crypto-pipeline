#!/usr/bin/env bash
# Creates the crypto-pipeline-msk cluster: 2x kafka.t3.small brokers (1 per AZ),
# IAM-only client auth, TLS in transit. Takes ~15-30 minutes to reach ACTIVE.
set -euo pipefail

AWS="${AWS_CLI:-aws}"
PRIVATE_SUBNET_A="subnet-0bbc264afd12cb2ce"
PRIVATE_SUBNET_B="subnet-06fd557f5c2f5b99f"
SG_MSK="sg-08f34502670f6b0f9"

"$AWS" kafka create-cluster-v2 \
  --cluster-name crypto-pipeline-msk \
  --provisioned "{
    \"KafkaVersion\": \"3.9.x\",
    \"NumberOfBrokerNodes\": 2,
    \"BrokerNodeGroupInfo\": {
      \"InstanceType\": \"kafka.t3.small\",
      \"ClientSubnets\": [\"$PRIVATE_SUBNET_A\", \"$PRIVATE_SUBNET_B\"],
      \"SecurityGroups\": [\"$SG_MSK\"],
      \"StorageInfo\": { \"EbsStorageInfo\": { \"VolumeSize\": 20 } }
    },
    \"EncryptionInfo\": {
      \"EncryptionInTransit\": { \"ClientBroker\": \"TLS\", \"InCluster\": true }
    },
    \"ClientAuthentication\": {
      \"Sasl\": { \"Iam\": { \"Enabled\": true } }
    },
    \"EnhancedMonitoring\": \"DEFAULT\"
  }" \
  --tags Project=crypto-pipeline

echo "Cluster creation started. Poll status with:"
echo "  aws kafka describe-cluster-v2 --cluster-arn <arn> --query ClusterInfo.State"
echo "Once ACTIVE, get bootstrap brokers with:"
echo "  aws kafka get-bootstrap-brokers --cluster-arn <arn>"
echo ""
echo "Topics are NOT created by this script - MSK has no API for topic management,"
echo "it requires a Kafka client with network access to the (private) brokers."
echo "See scripts/create_kafka_topics.py and docs/BUILD_LOG.md Phase 5 for how this"
echo "project bootstraps topics via a temporary EC2 instance + SSM."
