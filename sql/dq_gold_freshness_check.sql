-- Data quality check: Gold's overall freshness - how stale the most recent
-- 5-minute window is relative to now. Gold windows close on a 2-minute
-- watermark delay (matching Silver's dedup watermark - see gold_job.py), so
-- some lag is expected even when perfectly healthy; sustained large lag
-- indicates Gold (or something upstream of it) has stalled.
SELECT
    MAX(window_end) AS latest_window_end,
    date_diff('minute', MAX(window_end), current_timestamp) AS minutes_since_latest_window
FROM crypto_pipeline.gold_crypto
WHERE window_date >= date_add('day', -1, current_date);
