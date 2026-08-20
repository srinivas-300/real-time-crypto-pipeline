-- Data quality check: confirm Silver's own validation guarantees actually hold.
-- Every count below should be 0 - if not, bad data is passing validation somehow.
SELECT
    COUNT(*) AS total_rows,
    COUNT_IF(event_id IS NULL) AS null_event_id,
    COUNT_IF(symbol IS NULL OR symbol = '') AS null_or_empty_symbol,
    COUNT_IF(price IS NULL OR price <= 0) AS null_or_invalid_price,
    COUNT_IF(event_timestamp IS NULL) AS null_event_timestamp
FROM crypto_pipeline.silver_crypto;
