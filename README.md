# real-time-crypto-pipeline

Near-real-time data engineering pipeline: CoinGecko API → EC2 Kafka Producer → AWS MSK →
EMR Spark Structured Streaming → S3 (Bronze/Silver/Gold) → AWS Glue → Athena.

Full architecture, setup, and operational docs will land in `docs/` as each phase completes
(see Phase 18). This README is a stub during early development.

## Local development

```bash
python -m venv .venv
./.venv/Scripts/python.exe -m pip install -r requirements-dev.txt
cp .env.example .env
./.venv/Scripts/python.exe -m pytest tests/
```

## Project structure

```
infrastructure/   IaC / CLI scripts for VPC, MSK, EMR, IAM, S3
producer/         EC2 Kafka producer (Python)
spark/            Bronze / Silver / Gold Spark Structured Streaming jobs
tests/            Unit tests
sql/              Athena DDL / example queries
scripts/          Deploy, cleanup, verification scripts
config/           Environment-specific config (no secrets)
docs/             Architecture, runbooks, final documentation
```
