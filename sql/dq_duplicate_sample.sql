-- Diagnostic: are duplicate event_ids exact full-row copies (same price/timestamp),
-- consistent with the same source record being reprocessed and re-appended, rather
-- than something else (e.g. two different events colliding on the same id)?
SELECT event_id, symbol, price, event_timestamp, COUNT(*) AS copies
FROM crypto_pipeline.silver_crypto
GROUP BY event_id, symbol, price, event_timestamp
HAVING COUNT(*) > 1
ORDER BY copies DESC
LIMIT 10;
