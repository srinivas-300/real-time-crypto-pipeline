-- 24h volume trend per symbol across recent 5-minute windows (Gold layer).
SELECT
    symbol,
    window_start,
    avg_volume_24h
FROM crypto_pipeline.gold_crypto
WHERE window_date >= date_add('day', -1, current_date)  -- partition pruning (Iceberg keeps window_date as a real DATE type)
ORDER BY symbol, window_start;
