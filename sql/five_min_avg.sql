-- Most recent completed 5-minute window's aggregate stats per symbol (Gold layer).
WITH ranked AS (
    SELECT
        symbol,
        window_start,
        window_end,
        avg_price,
        min_price,
        max_price,
        event_count,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY window_start DESC) AS rn
    FROM crypto_pipeline.gold_crypto
    WHERE window_date >= date_add('day', -1, current_date)  -- partition pruning (Iceberg keeps window_date as a real DATE type)
)
SELECT symbol, window_start, window_end, avg_price, min_price, max_price, event_count
FROM ranked
WHERE rn = 1
ORDER BY symbol;
