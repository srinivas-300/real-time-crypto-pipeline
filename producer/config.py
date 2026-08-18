import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE)


def _get_bool(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes")


def _get_list(name: str, default: str) -> list[str]:
    raw = os.environ.get(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    environment: str = field(default_factory=lambda: os.environ.get("ENVIRONMENT", "local"))
    log_level: str = field(default_factory=lambda: os.environ.get("LOG_LEVEL", "INFO"))

    coingecko_api_base_url: str = field(
        default_factory=lambda: os.environ.get("COINGECKO_API_BASE_URL", "https://api.coingecko.com/api/v3")
    )
    coingecko_symbols: list[str] = field(
        default_factory=lambda: _get_list("COINGECKO_SYMBOLS", "bitcoin,ethereum,solana,cardano,dogecoin")
    )
    coingecko_poll_interval_seconds: int = field(
        default_factory=lambda: int(os.environ.get("COINGECKO_POLL_INTERVAL_SECONDS", "30"))
    )

    aws_region: str = field(default_factory=lambda: os.environ.get("AWS_REGION", "us-east-1"))
    kafka_bootstrap_servers: str = field(default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""))
    kafka_topic_raw: str = field(default_factory=lambda: os.environ.get("KAFKA_TOPIC_RAW", "crypto.raw"))
    kafka_topic_dlq: str = field(default_factory=lambda: os.environ.get("KAFKA_TOPIC_DLQ", "crypto.dlq"))
    kafka_security_protocol: str = field(
        default_factory=lambda: os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    )
    kafka_sasl_mechanism: str = field(default_factory=lambda: os.environ.get("KAFKA_SASL_MECHANISM", "OAUTHBEARER"))

    producer_max_retries: int = field(default_factory=lambda: int(os.environ.get("PRODUCER_MAX_RETRIES", "5")))
    producer_retry_backoff_seconds: float = field(
        default_factory=lambda: float(os.environ.get("PRODUCER_RETRY_BACKOFF_SECONDS", "2"))
    )

    def validate_for_kafka(self) -> None:
        if not self.kafka_bootstrap_servers:
            raise ValueError(
                "KAFKA_BOOTSTRAP_SERVERS is not set. This is expected until Phase 5 (MSK) is complete."
            )


settings = Settings()
