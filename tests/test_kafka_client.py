from unittest.mock import MagicMock

from kafka.errors import KafkaError

from producer.kafka_client import send_event

EVENT = {"event_id": "abc-123", "symbol": "BTC", "price": 118000.25}


def _mock_future(metadata):
    future = MagicMock()
    future.get.return_value = metadata
    return future


def test_send_event_calls_producer_with_key_and_value():
    producer = MagicMock()
    metadata = MagicMock(topic="crypto.raw", partition=0, offset=42)
    producer.send.return_value = _mock_future(metadata)

    send_event(producer, "crypto.raw", EVENT)

    producer.send.assert_called_once_with("crypto.raw", key="BTC", value=EVENT)


def test_send_event_retries_on_kafka_error_then_succeeds(mocker):
    mocker.patch("tenacity.nap.sleep")
    metadata = MagicMock(topic="crypto.raw", partition=0, offset=1)

    producer = MagicMock()
    failing_future = MagicMock()
    failing_future.get.side_effect = KafkaError("transient broker error")
    producer.send.side_effect = [failing_future, _mock_future(metadata)]

    send_event(producer, "crypto.raw", EVENT)

    assert producer.send.call_count == 2
