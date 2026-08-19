-- Daily high/low price per symbol, rolled up from Gold's per-window min/max.
SELECT
    window_date,
    symbol,
    MIN(min_price) AS day_low,
    MAX(max_price) AS day_high,
    AVG(avg_price) AS day_avg_price
FROM crypto_pipeline.gold_crypto
GROUP BY window_date, symbol
ORDER BY window_date DESC, symbol;
