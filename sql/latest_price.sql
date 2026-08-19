-- Latest known price per symbol, from the Silver layer (event-level data).
WITH ranked AS (
    SELECT
        symbol,
        price,
        event_timestamp,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY event_timestamp DESC) AS rn
    FROM crypto_pipeline.silver_crypto
    WHERE event_date >= date_add('day', -1, current_date)  -- partition pruning (Iceberg keeps event_date as a real DATE type)
)
SELECT symbol, price, event_timestamp
FROM ranked
WHERE rn = 1
ORDER BY symbol;
