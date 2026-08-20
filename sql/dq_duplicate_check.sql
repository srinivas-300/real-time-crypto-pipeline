-- Data quality check: verify no duplicate event_ids exist in Silver, independent of
-- (and beyond the time window covered by) the streaming job's own watermarked dedup.
SELECT COUNT(*) AS duplicate_event_id_count
FROM (
    SELECT event_id
    FROM crypto_pipeline.silver_crypto
    GROUP BY event_id
    HAVING COUNT(*) > 1
);
