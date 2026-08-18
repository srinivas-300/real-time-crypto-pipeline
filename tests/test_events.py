from producer.events import build_event

VALID_COIN = {
    "id": "bitcoin",
    "symbol": "btc",
    "current_price": 118000.25,
    "market_cap": 2300000000000,
    "total_volume": 45000000000,
    "price_change_percentage_24h": 2.31,
    "last_updated": "2026-08-17T17:00:00.000Z",
}


def test_build_event_maps_fields_correctly():
    event = build_event(VALID_COIN)

    assert event["symbol"] == "BTC"
    assert event["price"] == 118000.25
    assert event["market_cap"] == 2300000000000
    assert event["volume_24h"] == 45000000000
    assert event["price_change_24h"] == 2.31
    assert event["event_timestamp"] == "2026-08-17T17:00:00.000Z"
    assert event["event_id"]
    assert event["ingestion_timestamp"]


def test_build_event_generates_unique_ids():
    a = build_event(VALID_COIN)
    b = build_event(VALID_COIN)
    assert a["event_id"] != b["event_id"]


def test_build_event_returns_none_when_price_missing():
    coin = {**VALID_COIN, "current_price": None}
    assert build_event(coin) is None


def test_build_event_returns_none_when_id_missing():
    coin = {**VALID_COIN, "id": None}
    assert build_event(coin) is None
