import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from producer.config import settings
from producer.logger import get_logger

logger = get_logger(__name__)


class CoinGeckoRequestError(Exception):
    """Raised for CoinGecko client errors (4xx) that should not be retried."""


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, requests.exceptions.HTTPError):
        response = exc.response
        return response is not None and response.status_code >= 500
    return isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout))


@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError)),
    stop=stop_after_attempt(settings.producer_max_retries),
    wait=wait_exponential(multiplier=settings.producer_retry_backoff_seconds, min=1, max=30),
    reraise=True,
)
def fetch_market_data(symbols: list[str]) -> list[dict]:
    """Fetch current market data for the given CoinGecko coin ids (e.g. 'bitcoin')."""
    url = f"{settings.coingecko_api_base_url}/coins/markets"
    params = {"vs_currency": "usd", "ids": ",".join(symbols)}

    logger.info("Fetching CoinGecko market data", extra={"event": "coingecko_request", "symbols": symbols})

    response = requests.get(url, params=params, timeout=10)

    if 400 <= response.status_code < 500:
        raise CoinGeckoRequestError(f"CoinGecko returned {response.status_code}: {response.text}")

    response.raise_for_status()
    return response.json()
