-- Anomaly detection: flag consecutive-reading price swings beyond a sanity
-- threshold (20%) for the same symbol - catches bad/garbled upstream data that
-- still passes Silver's basic validation (price > 0, non-null) but is otherwise
-- implausible for a ~30s polling interval. Empty result = healthy.
WITH ordered AS (
    SELECT
        symbol,
        price,
        event_timestamp,
        LAG(price) OVER (PARTITION BY symbol ORDER BY event_timestamp) AS prev_price,
        LAG(event_timestamp) OVER (PARTITION BY symbol ORDER BY event_timestamp) AS prev_event_timestamp
    FROM crypto_pipeline.silver_crypto
    WHERE event_date >= date_add('day', -1, current_date)
)
SELECT
    symbol,
    prev_event_timestamp,
    prev_price,
    event_timestamp,
    price,
    ROUND(ABS(price - prev_price) / prev_price * 100, 2) AS pct_change
FROM ordered
WHERE prev_price IS NOT NULL
  AND ABS(price - prev_price) / prev_price > 0.20
ORDER BY pct_change DESC;
