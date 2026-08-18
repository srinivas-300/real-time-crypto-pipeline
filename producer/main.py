import signal
import threading

from kafka.errors import KafkaError
from requests.exceptions import RequestException

from producer.coingecko_client import CoinGeckoRequestError, fetch_market_data
from producer.config import settings
from producer.events import build_event
from producer.kafka_client import build_producer, send_event
from producer.logger import get_logger

logger = get_logger(__name__)

_shutdown = threading.Event()


def _handle_shutdown_signal(signum, _frame):
    logger.info("Shutdown signal received", extra={"event": "shutdown_signal", "signum": signum})
    _shutdown.set()


def run() -> None:
    signal.signal(signal.SIGTERM, _handle_shutdown_signal)
    signal.signal(signal.SIGINT, _handle_shutdown_signal)

    producer = build_producer()
    logger.info(
        "Producer started",
        extra={
            "event": "producer_started",
            "symbols": settings.coingecko_symbols,
            "poll_interval_seconds": settings.coingecko_poll_interval_seconds,
        },
    )

    try:
        while not _shutdown.is_set():
            _run_one_cycle(producer)
            _shutdown.wait(timeout=settings.coingecko_poll_interval_seconds)
    finally:
        logger.info("Flushing producer before exit", extra={"event": "producer_shutdown"})
        producer.flush(timeout=30)
        producer.close(timeout=30)


def _run_one_cycle(producer) -> None:
    try:
        coins = fetch_market_data(settings.coingecko_symbols)
    except (CoinGeckoRequestError, RequestException) as exc:
        logger.error("CoinGecko fetch failed, skipping this cycle", extra={"event": "coingecko_fetch_failed", "error": str(exc)})
        return

    for coin in coins:
        event = build_event(coin)
        if event is None:
            continue
        try:
            send_event(producer, settings.kafka_topic_raw, event)
        except KafkaError as exc:
            logger.error(
                "Kafka publish failed after retries, dropping event",
                extra={"event": "kafka_publish_failed", "error": str(exc), "payload": event},
            )


if __name__ == "__main__":
    run()
