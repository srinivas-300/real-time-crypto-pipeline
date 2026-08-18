import responses

from producer.coingecko_client import fetch_market_data

MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"


@responses.activate
def test_fetch_market_data_returns_parsed_json(mocker):
    mocker.patch("tenacity.nap.sleep")
    payload = [{"id": "bitcoin", "symbol": "btc", "current_price": 118000.25}]
    responses.add(responses.GET, MARKETS_URL, json=payload, status=200)

    result = fetch_market_data(["bitcoin"])

    assert result == payload


@responses.activate
def test_fetch_market_data_retries_on_server_error_then_succeeds(mocker):
    mocker.patch("tenacity.nap.sleep")
    payload = [{"id": "bitcoin", "symbol": "btc", "current_price": 118000.25}]
    responses.add(responses.GET, MARKETS_URL, status=503)
    responses.add(responses.GET, MARKETS_URL, json=payload, status=200)

    result = fetch_market_data(["bitcoin"])

    assert result == payload
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_market_data_does_not_retry_on_client_error(mocker):
    mocker.patch("tenacity.nap.sleep")
    responses.add(responses.GET, MARKETS_URL, json={"error": "bad request"}, status=400)

    try:
        fetch_market_data(["not-a-real-coin"])
        assert False, "expected CoinGeckoRequestError"
    except Exception as exc:
        assert type(exc).__name__ == "CoinGeckoRequestError"

    assert len(responses.calls) == 1
