from kafka.errors import KafkaError

from producer.coingecko_client import CoinGeckoRequestError
from producer.main import _run_one_cycle

COINS = [{"id": "bitcoin"}, {"id": "ethereum"}]


def test_run_one_cycle_publishes_all_valid_events(mocker):
    mocker.patch("producer.main.fetch_market_data", return_value=COINS)
    mocker.patch("producer.main.build_event", side_effect=[{"symbol": "BTC"}, {"symbol": "ETH"}])
    send_event = mocker.patch("producer.main.send_event")

    _run_one_cycle(producer=mocker.MagicMock())

    assert send_event.call_count == 2


def test_run_one_cycle_skips_coin_with_no_event(mocker):
    mocker.patch("producer.main.fetch_market_data", return_value=COINS)
    mocker.patch("producer.main.build_event", side_effect=[None, {"symbol": "ETH"}])
    send_event = mocker.patch("producer.main.send_event")

    _run_one_cycle(producer=mocker.MagicMock())

    send_event.assert_called_once()


def test_run_one_cycle_handles_api_failure_without_raising(mocker):
    mocker.patch("producer.main.fetch_market_data", side_effect=CoinGeckoRequestError("boom"))
    send_event = mocker.patch("producer.main.send_event")

    _run_one_cycle(producer=mocker.MagicMock())  # should not raise

    send_event.assert_not_called()


def test_run_one_cycle_continues_after_publish_failure(mocker):
    mocker.patch("producer.main.fetch_market_data", return_value=COINS)
    mocker.patch("producer.main.build_event", side_effect=[{"symbol": "BTC"}, {"symbol": "ETH"}])
    send_event = mocker.patch("producer.main.send_event", side_effect=[KafkaError("broker down"), None])

    _run_one_cycle(producer=mocker.MagicMock())  # should not raise despite first failure

    assert send_event.call_count == 2
