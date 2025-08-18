-- Create one fake merchant/current customer/account and a fact row to test joins
INSERT INTO cur.dim_customer (customer_bk, email, country, kyc_level, risk_band)
VALUES ('00000000-0000-0000-0000-000000000001', 'demo@example.com', 'VN', 1, 'LOW')
ON CONFLICT DO NOTHING;

INSERT INTO cur.dim_account (account_bk, customer_bk, currency, country, status)
VALUES ('00000000-0000-0000-0000-0000000000AA', '00000000-0000-0000-0000-000000000001', 'VND', 'VN', 'ACTIVE')
ON CONFLICT DO NOTHING;

INSERT INTO cur.dim_merchant (merchant_name_norm, merchant_category, country)
VALUES (LOWER('Starbucks Phu Nhuan'), 'coffee', 'VN')
ON CONFLICT DO NOTHING;

-- Grab keys
WITH c AS (
  SELECT customer_sk FROM cur.dim_customer WHERE customer_bk='00000000-0000-0000-0000-000000000001' AND is_current
), a AS (
  SELECT account_sk FROM cur.dim_account WHERE account_bk='00000000-0000-0000-0000-0000000000AA' AND is_current
), m AS (
  SELECT merchant_sk FROM cur.dim_merchant WHERE merchant_name_norm=LOWER('Starbucks Phu Nhuan') AND is_current
)
INSERT INTO cur.fact_transactions (
  transaction_bk, customer_sk, account_sk, merchant_sk,
  type, status, amount, currency, country, created_at, created_date, settled_at
)
SELECT
  '00000000-0000-0000-0000-000000000011'::uuid, 
  c.customer_sk, 
  a.account_sk, 
  m.merchant_sk,
  'PAYMENT','PENDING', 250000.00, 'VND', 'VN',
  NOW() AT TIME ZONE 'UTC',
  (NOW() AT TIME ZONE 'UTC')::date,
  NULL
FROM c,a,m
ON CONFLICT DO NOTHING;
