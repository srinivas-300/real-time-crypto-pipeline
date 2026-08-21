-- Data quality check: confirm every expected symbol has reported recently.
-- Expected symbols must be kept in sync with COINGECKO_SYMBOLS in .env
-- (bitcoin,ethereum,solana,cardano,dogecoin -> BTC,ETH,SOL,ADA,DOGE - see
-- events.py, which upper()s the CoinGecko symbol).
-- Empty result = healthy; any row returned means that symbol has gone silent
-- for the last 10 minutes even though the pipeline overall is running.
WITH expected AS (
    SELECT symbol FROM (VALUES ('BTC'), ('ETH'), ('SOL'), ('ADA'), ('DOGE')) AS t(symbol)
),
recent AS (
    SELECT DISTINCT symbol
    FROM crypto_pipeline.silver_crypto
    WHERE event_date >= date_add('day', -1, current_date)
      AND event_timestamp >= date_add('minute', -10, current_timestamp)
)
SELECT expected.symbol AS missing_symbol
FROM expected
LEFT JOIN recent ON expected.symbol = recent.symbol
WHERE recent.symbol IS NULL;
