-- Daily GMV (gross merchandise volume) per currency
CREATE MATERIALIZED VIEW IF NOT EXISTS cur.mv_gmv_daily_currency AS
SELECT
  (created_at AT TIME ZONE 'UTC')::date AS day_utc,
  currency,
  COUNT(*) AS tx_count,
  SUM(amount) AS total_amount
FROM cur.fact_transactions
GROUP BY 1,2;

CREATE INDEX IF NOT EXISTS mv_gmv_daily_currency_day_idx
  ON cur.mv_gmv_daily_currency(day_utc, currency);

-- Top merchants (7d) by amount
CREATE MATERIALIZED VIEW IF NOT EXISTS cur.mv_top_merchants_7d AS
WITH recent AS (
  SELECT * FROM cur.fact_transactions
  WHERE created_at >= NOW() - INTERVAL '7 days'
)
SELECT
  m.merchant_name_norm,
  COUNT(*)     AS tx_count,
  SUM(r.amount) AS total_amount
FROM recent r
JOIN cur.dim_merchant m ON m.merchant_sk = r.merchant_sk AND m.is_current
GROUP BY 1
ORDER BY total_amount DESC
LIMIT 50;
