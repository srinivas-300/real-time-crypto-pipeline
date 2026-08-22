# real-time-crypto-pipeline

A near-real-time data engineering pipeline that ingests live crypto prices from
CoinGecko, streams them through Kafka, and lands validated, deduplicated,
analytics-ready data in S3 — queryable in Athena within seconds of arrival.

```
CoinGecko API
  -> EC2 Python producer (systemd service, IAM-auth Kafka client)
  -> AWS MSK (Kafka, IAM auth, TLS, private subnets)
  -> EMR Spark Structured Streaming (3 separate long-running jobs)
       -> S3 Bronze  (raw + Kafka metadata, minimal transform, plain Parquet)
       -> Iceberg table crypto_pipeline.silver_crypto (validated, typed, deduped)
       -> Iceberg table crypto_pipeline.gold_crypto   (5-min windowed aggregates)
  -> AWS Glue Data Catalog (Iceberg's catalog impl - Spark registers/updates
     tables directly on every commit, no crawler)
  -> Athena
```

Built entirely via raw AWS CLI (no Terraform/CDK) — everything under
`infrastructure/` is a reproducible record of what was run, not auto-applied IaC.
Region: `us-east-1`, hardcoded throughout (scripts, IAM ARNs, `.env`). This is a
personal/learning project, not an open-source library — detailed setup and
operations runbooks are kept as local, untracked reference material rather than
published here.

## Why it's built this way

A few decisions shape everything else in this repo, worth understanding up front:

- **Medallion architecture (Bronze/Silver/Gold), each layer its own long-running
  Spark job**, not one script. Bronze is the raw landing zone / replay buffer;
  Silver validates, types, and deduplicates; Gold aggregates. Each has its own
  checkpoint and can be reasoned about independently.
- **Iceberg tables instead of Hive-partitioned Parquet + a Glue crawler.** Spark
  registers and updates Silver/Gold's table metadata in Glue directly on every
  commit — new data is queryable in Athena immediately, with no separate
  crawl/re-crawl step and no risk of the crawler misreading Spark's internal
  metadata directories as data.
- **`MERGE INTO`, not a plain streaming append, for Silver and Gold.** Their
  stateful operators checkpoint to the EMR cluster's local HDFS (S3 doesn't
  provide the atomic-rename semantics they need), which means checkpoint state
  is lost every time the cluster is recreated — a `MERGE`-based write makes the
  resulting reprocess idempotent instead of duplicating rows.
- **IAM auth end-to-end for Kafka** (MSK's native `AWS_MSK_IAM` mechanism, used
  by the producer, Spark jobs, and any admin tooling) — no secrets to manage or
  rotate; credentials come from whatever identity is running.
- **No SSH anywhere.** Every EC2 instance (producer, EMR nodes, any one-off
  bootstrap instance) uses AWS Systems Manager Session Manager exclusively —
  no key pairs, no open inbound ports for administration.
- **Cost-conscious throughout, and paused between work sessions.** Single NAT
  Gateway, smallest viable instance sizes, SSE-S3 over a customer-managed KMS
  key. The two most expensive pieces — the EMR cluster and the MSK cluster —
  are fully deleted when not in active use and recreated on demand for a work
  session, rather than left running continuously.

## What each layer does

- **Producer** (`producer/`): polls CoinGecko's `/coins/markets` endpoint every
  30s for a fixed list of symbols (BTC, ETH, SOL, ADA, DOGE by default),
  normalizes each coin into an event (`event_id`, timestamps, symbol, price,
  market cap, 24h volume, 24h price change), and publishes it to Kafka topic
  `crypto.raw`, keyed by symbol. Idempotent producer (`acks=all`), retries on
  transient failures with exponential backoff, distinguishes retryable (5xx,
  connection) from non-retryable (4xx) upstream errors, graceful SIGTERM
  shutdown for systemd, structured JSON logging.
- **Bronze** (`spark/bronze/bronze_job.py`): polls Kafka on a fixed 30-second
  interval — deliberately *not* a Structured Streaming `readStream`, which
  silently returned zero rows forever on this cluster for reasons never fully
  root-caused. Writes raw events plus Kafka's own metadata (partition, offset,
  broker timestamp) to S3 as plain Hive-partitioned Parquet
  (`ingest_date=.../ingest_hour=...`). This is the system of record — Silver
  and Gold can always be fully rebuilt by reprocessing Bronze.
- **Silver** (`spark/silver/silver_job.py`): Structured Streaming, reads
  Bronze via the file source, parses the JSON payload, validates each record
  (non-null required fields, `price > 0`), deduplicates by `event_id` within a
  2-minute watermark, writes clean typed rows to Iceberg table
  `crypto_pipeline.silver_crypto`. Records that fail validation are written to
  `errors/crypto/` (plain Parquet, not cataloged) along with the specific
  reason(s) they failed, rather than silently dropped.
- **Gold** (`spark/gold/gold_job.py`): Structured Streaming, reads Silver via
  Iceberg's own streaming source, computes 5-minute tumbling-window aggregates
  per symbol — average/min/max price, average 24h volume, average 24h price
  change, event count — and writes to Iceberg table
  `crypto_pipeline.gold_crypto`.

## Infrastructure at a glance

| Component | Detail |
|---|---|
| VPC | `10.0.0.0/16`, 1 public + 2 private subnets across 2 AZs, single NAT Gateway |
| MSK | `crypto-pipeline-msk` — Kafka 3.9.x, 2× `kafka.t3.small` brokers, IAM auth, TLS |
| Kafka topics | `crypto.raw` (3 partitions, RF=2, 24h retention), `crypto.dlq` (created, currently unused) |
| Producer | `crypto-pipeline-producer` — 1× `t3.micro`, private subnet, systemd-managed |
| EMR | `crypto-pipeline-emr` — `emr-7.13.0`, Spark only, `m5.xlarge` × (1 master + 1 core + 1 task) |
| Storage | 1 S3 bucket — `bronze/`, Iceberg `warehouse/` (Silver + Gold), `errors/`, `checkpoints/`, `spark-jobs/` |
| Catalog | AWS Glue database `crypto_pipeline`, tables `silver_crypto` / `gold_crypto` (Iceberg) |
| Query | Amazon Athena against the Glue catalog |
| IAM | 4 active roles, one per service identity (producer, EMR service, EMR EC2, Kafka topic-admin) — least privilege, no shared roles |
| Monitoring | 1 SNS topic fanned out to 6 CloudWatch alarms (producer health, MSK controller/partitions/disk, EMR job failures) |
| Access | AWS Systems Manager Session Manager only — no SSH keys, no bastion host |

## Local development

```bash
python -m venv .venv
./.venv/Scripts/python.exe -m pip install -r requirements-dev.txt
cp .env.example .env      # fill in real values if testing against live AWS
./.venv/Scripts/python.exe -m pytest tests/
```

Python 3.13 is fine locally. **The EC2 producer and EMR both run Python 3.9**
(Amazon Linux 2023's default `python3`) — any change to `producer/` or `spark/`
must stay 3.9-compatible (no bare PEP 604 `X | None` union syntax without
`from __future__ import annotations`, for example).

## Project structure

```
infrastructure/   CLI scripts + IAM/S3/VPC JSON policy docs, organized by AWS service
producer/         EC2 Kafka producer (Python) — CoinGecko -> crypto.raw
spark/            PySpark jobs: bronze/ (batch-polling), silver/, gold/ (Structured Streaming)
tests/            pytest unit tests for producer/ (fully mocked, no AWS calls)
scripts/          One-off ops scripts (e.g. create_kafka_topics.py)
sql/              Athena queries: business queries (latest_price, top_movers, etc.)
                   and data-quality checks (dq_*.sql — freshness, dedup, anomalies)
docs/             Local reference notes (not published — see below)
config/           Reserved for future phases — currently empty
```

## Data quality

`sql/dq_*.sql` — run against Athena database `crypto_pipeline`:

- **Completeness** — confirms Silver's own validation guarantees actually hold
  (every count should be 0)
- **Duplicates** — no duplicate `event_id` (Silver) or `(window_start, symbol)`
  (Gold) pairs, independent of the streaming job's own dedup
- **Freshness** — per-symbol and overall staleness in minutes; low single
  digits under healthy operation
- **Missing symbol** — flags any expected symbol going silent for 10+ minutes
- **Price spike** — flags an implausible (>20%) consecutive-reading price swing
  per symbol, catching bad upstream data that still passes basic validation

## Testing

```bash
./.venv/Scripts/python.exe -m pytest tests/ -q
```

Covers `producer/` only — fully mocked (`responses` for HTTP, `unittest.mock` /
`pytest-mock` for Kafka), no real AWS calls, runs in a few seconds. There is
**no automated test suite for the Spark jobs** — they're verified against the
real EMR cluster after every change (fresh S3 output timestamps, Iceberg
metadata commit cadence, a live Athena query). This is intentional for a
project this size, not an oversight.

## Operating the pipeline

This project alternates between **paused** (everything cost-bearing torn down)
and **running** (a full session's infrastructure recreated) — it is not meant
to run continuously. The two most expensive pieces, EMR and MSK, get deleted
between sessions along with the NAT Gateway; the S3 bucket, Glue catalog, VPC,
and IAM roles are cheap to leave alone and are never torn down.

One consequence worth knowing: Silver and Gold's stateful streaming checkpoints
live on the EMR cluster's *local* HDFS (not S3, which can't provide the
atomic-rename guarantees those operators need) — so every time the cluster is
recreated, both jobs lose their progress and reprocess Bronze's entire
accumulated history from scratch before catching back up to real-time. This is
safe (the `MERGE INTO` write logic means no duplicate rows land in the tables)
but takes real wall-clock time, growing with how much history has piled up.
Don't mistake a slow catch-up for a stall — check whether Iceberg is still
committing new snapshots roughly every 30-45 seconds before assuming something
is actually broken.

Detailed step-by-step build and operations runbooks (exact commands for every
resource, the full gotcha list, cost breakdowns) are kept as local reference
notes rather than published in this repo.

## Status

Core pipeline (ingestion through Gold aggregation, Athena querying, data
quality checks, CloudWatch monitoring) is built and verified end-to-end,
including repeated pause/resume cycles. Formal failure-injection testing and
further production hardening were deliberately scoped out as not needed for
this project's goals.
