from __future__ import annotations

import uuid
from datetime import datetime, timezone

from producer.logger import get_logger

logger = get_logger(__name__)


def build_event(coin: dict) -> dict | None:
    """Build a normalized crypto price event from a CoinGecko /coins/markets entry."""
    coin_id = coin.get("id")
    price = coin.get("current_price")

    if not coin_id or price is None:
        logger.warning("Skipping coin with missing id/price", extra={"event": "skip_invalid_coin", "coin": coin})
        return None

    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": coin.get("last_updated"),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": coin.get("symbol", "").upper(),
        "price": price,
        "market_cap": coin.get("market_cap"),
        "volume_24h": coin.get("total_volume"),
        "price_change_24h": coin.get("price_change_percentage_24h"),
    }
