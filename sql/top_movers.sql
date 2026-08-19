-- Top coins right now, ranked by 24h price change (latest Silver reading per symbol).
WITH latest AS (
    SELECT
        symbol,
        price,
        volume_24h,
        price_change_24h,
        event_timestamp,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY event_timestamp DESC) AS rn
    FROM crypto_pipeline.silver_crypto
    WHERE event_date >= date_add('day', -1, current_date)  -- partition pruning (Iceberg keeps event_date as a real DATE type)
)
SELECT symbol, price, volume_24h, price_change_24h
FROM latest
WHERE rn = 1
ORDER BY price_change_24h DESC;
