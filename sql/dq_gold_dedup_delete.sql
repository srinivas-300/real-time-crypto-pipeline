-- Remove duplicate rows from Gold, keeping exactly one copy per (window_start, symbol).
-- Safe because duplicate copies were confirmed (via a preview SELECT) to always land
-- in different physical Iceberg data files, so ($path, window_start, symbol) uniquely
-- identifies each physical row with no ambiguity about which copy is removed.
DELETE FROM crypto_pipeline.gold_crypto
WHERE ("$path", window_start, symbol) IN (
    SELECT "$path", window_start, symbol
    FROM (
        SELECT "$path", window_start, symbol,
               ROW_NUMBER() OVER (PARTITION BY window_start, symbol ORDER BY "$path") AS rn
        FROM crypto_pipeline.gold_crypto
    ) WHERE rn > 1
);
