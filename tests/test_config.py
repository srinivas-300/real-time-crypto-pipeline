from producer.config import Settings


def test_defaults_load_without_env_file():
    s = Settings()
    assert s.environment == "local"
    assert s.coingecko_api_base_url.startswith("https://")
    assert "bitcoin" in s.coingecko_symbols
    assert s.kafka_topic_raw == "crypto.raw"
    assert s.kafka_topic_dlq == "crypto.dlq"


def test_validate_for_kafka_raises_when_bootstrap_servers_missing():
    s = Settings(kafka_bootstrap_servers="")
    try:
        s.validate_for_kafka()
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_validate_for_kafka_passes_when_bootstrap_servers_set():
    s = Settings(kafka_bootstrap_servers="b-1.example:9098")
    s.validate_for_kafka()
