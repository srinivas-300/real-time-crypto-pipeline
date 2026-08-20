-- Remove duplicate rows from Silver, keeping exactly one copy per event_id.
-- Safe because duplicate copies were confirmed (via a preview SELECT) to always
-- land in different physical Iceberg data files, so ($path, event_id) uniquely
-- identifies each physical row with no ambiguity about which copy is removed.
DELETE FROM crypto_pipeline.silver_crypto
WHERE ("$path", event_id) IN (
    SELECT "$path", event_id
    FROM (
        SELECT "$path", event_id,
               ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY "$path") AS rn
        FROM crypto_pipeline.silver_crypto
    ) WHERE rn > 1
);
