-- Data quality check: per-symbol staleness in Silver. The producer polls every
-- 30s, so under healthy operation minutes_since_last_event should stay in low
-- single digits. Anything double digits (or a NULL last_event_timestamp, meaning
-- the symbol never appeared at all) indicates the pipeline has stalled for that
-- symbol somewhere upstream (producer, Bronze, or Silver).
SELECT
    symbol,
    MAX(event_timestamp) AS last_event_timestamp,
    date_diff('minute', MAX(event_timestamp), current_timestamp) AS minutes_since_last_event
FROM crypto_pipeline.silver_crypto
WHERE event_date >= date_add('day', -1, current_date)
GROUP BY symbol
ORDER BY minutes_since_last_event DESC;
