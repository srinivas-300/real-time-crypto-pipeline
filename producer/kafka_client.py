from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer.default import DefaultSerializer
from kafka.serializer.json import JsonSerializer
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from producer.config import settings
from producer.logger import get_logger

logger = get_logger(__name__)


def build_producer() -> KafkaProducer:
    """Builds a KafkaProducer authenticated via MSK's native AWS_MSK_IAM SASL mechanism.

    Credentials come from botocore's standard chain (EC2 instance role in production,
    local ~/.aws credentials for anything run from a dev machine) - no explicit key
    handling needed.
    """
    settings.validate_for_kafka()
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        security_protocol=settings.kafka_security_protocol,
        sasl_mechanism=settings.kafka_sasl_mechanism,
        acks="all",
        enable_idempotence=True,
        retries=5,
        max_in_flight_requests_per_connection=5,
        linger_ms=100,
        key_serializer=DefaultSerializer(),
        value_serializer=JsonSerializer(),
    )


@retry(
    retry=retry_if_exception_type(KafkaError),
    stop=stop_after_attempt(settings.producer_max_retries),
    wait=wait_exponential(multiplier=settings.producer_retry_backoff_seconds, min=1, max=30),
    reraise=True,
)
def send_event(producer: KafkaProducer, topic: str, event: dict) -> None:
    future = producer.send(topic, key=event.get("symbol"), value=event)
    metadata = future.get(timeout=10)
    logger.info(
        "Published event",
        extra={
            "event": "kafka_publish_success",
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
            "symbol": event.get("symbol"),
        },
    )
