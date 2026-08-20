-- Data quality check: verify no duplicate (window_start, symbol) pairs exist in Gold.
-- Each 5-minute window per symbol should be represented exactly once.
SELECT COUNT(*) AS duplicate_window_symbol_count
FROM (
    SELECT window_start, symbol
    FROM crypto_pipeline.gold_crypto
    GROUP BY window_start, symbol
    HAVING COUNT(*) > 1
);
