-- Dry run: count how many rows would be removed by keeping exactly one copy of
-- each duplicated event_id (ranked by physical file path as a stable tiebreaker,
-- since duplicate copies of the same event were confirmed to land in different
-- Iceberg data files). Does not modify any data.
SELECT COUNT(*) AS rows_to_delete
FROM (
    SELECT "$path", event_id,
           ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY "$path") AS rn
    FROM crypto_pipeline.silver_crypto
) WHERE rn > 1;
