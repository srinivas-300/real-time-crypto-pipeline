#!/usr/bin/env bash
# EC2 user-data bootstrap for the crypto pipeline producer.
# __KAFKA_BOOTSTRAP_SERVERS__ is substituted with the real MSK broker string at
# launch time (see infrastructure/ec2/launch-producer.sh) - never committed as a
# real value here.
set -euo pipefail

dnf install -y git

REPO_DIR=/opt/crypto-pipeline
git clone https://github.com/srinivas-300/real-time-crypto-pipeline.git "$REPO_DIR"
cd "$REPO_DIR"

python3 -m venv .venv
./.venv/bin/pip install --upgrade pip -q
./.venv/bin/pip install -r requirements.txt -q

cat > "$REPO_DIR/.env" <<EOF
ENVIRONMENT=production
LOG_LEVEL=INFO
COINGECKO_API_BASE_URL=https://api.coingecko.com/api/v3
COINGECKO_SYMBOLS=bitcoin,ethereum,solana,cardano,dogecoin
COINGECKO_POLL_INTERVAL_SECONDS=30
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1
KAFKA_BOOTSTRAP_SERVERS=__KAFKA_BOOTSTRAP_SERVERS__
KAFKA_TOPIC_RAW=crypto.raw
KAFKA_TOPIC_DLQ=crypto.dlq
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=AWS_MSK_IAM
PRODUCER_MAX_RETRIES=5
PRODUCER_RETRY_BACKOFF_SECONDS=2
EOF

chown -R ec2-user:ec2-user "$REPO_DIR"

cp "$REPO_DIR/infrastructure/ec2/crypto-pipeline-producer.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable crypto-pipeline-producer
systemctl start crypto-pipeline-producer
