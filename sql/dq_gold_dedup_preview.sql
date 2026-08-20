-- Dry run: count how many rows would be removed by keeping exactly one copy of
-- each duplicated (window_start, symbol) pair, ranked by physical file path.
-- Does not modify any data.
SELECT COUNT(*) AS rows_to_delete
FROM (
    SELECT "$path", window_start, symbol,
           ROW_NUMBER() OVER (PARTITION BY window_start, symbol ORDER BY "$path") AS rn
    FROM crypto_pipeline.gold_crypto
) WHERE rn > 1;
