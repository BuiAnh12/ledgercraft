-- ========== SCD2 helper functions in cur schema ==========

-- DIM CUSTOMER (BK = customer_bk)
CREATE OR REPLACE FUNCTION cur.upsert_dim_customer(
  p_customer_bk uuid,
  p_email citext,
  p_country char(2),
  p_kyc_level smallint,
  p_risk_band text,
  p_as_of timestamptz
) RETURNS bigint AS $$
DECLARE cur_row cur.dim_customer%ROWTYPE;
BEGIN
  SELECT * INTO cur_row
  FROM cur.dim_customer
  WHERE customer_bk = p_customer_bk AND is_current
  ORDER BY valid_from DESC
  LIMIT 1;

  IF cur_row.customer_sk IS NULL THEN
    INSERT INTO cur.dim_customer (customer_bk,email,country,kyc_level,risk_band,is_current,valid_from,version)
    VALUES (p_customer_bk,p_email,p_country,p_kyc_level,p_risk_band,TRUE,COALESCE(p_as_of,NOW()),1)
    RETURNING customer_sk INTO cur_row.customer_sk;
    RETURN cur_row.customer_sk;
  END IF;

  IF cur_row.email IS DISTINCT FROM p_email
     OR cur_row.country IS DISTINCT FROM p_country
     OR cur_row.kyc_level IS DISTINCT FROM p_kyc_level
     OR cur_row.risk_band IS DISTINCT FROM p_risk_band THEN
    UPDATE cur.dim_customer
      SET is_current=FALSE, valid_to=COALESCE(p_as_of,NOW())
      WHERE customer_sk = cur_row.customer_sk;

    INSERT INTO cur.dim_customer (customer_bk,email,country,kyc_level,risk_band,is_current,valid_from,version)
    VALUES (p_customer_bk,p_email,p_country,p_kyc_level,p_risk_band,TRUE,COALESCE(p_as_of,NOW()),cur_row.version+1)
    RETURNING customer_sk INTO cur_row.customer_sk;
  END IF;

  RETURN cur_row.customer_sk;
END$$ LANGUAGE plpgsql;

-- DIM ACCOUNT (BK = account_bk)
CREATE OR REPLACE FUNCTION cur.upsert_dim_account(
  p_account_bk uuid,
  p_customer_bk uuid,
  p_currency char(3),
  p_country char(2),
  p_status text,
  p_as_of timestamptz
) RETURNS bigint AS $$
DECLARE cur_row cur.dim_account%ROWTYPE;
BEGIN
  SELECT * INTO cur_row
  FROM cur.dim_account
  WHERE account_bk = p_account_bk AND is_current
  ORDER BY valid_from DESC
  LIMIT 1;

  IF cur_row.account_sk IS NULL THEN
    INSERT INTO cur.dim_account (account_bk,customer_bk,currency,country,status,is_current,valid_from,version)
    VALUES (p_account_bk,p_customer_bk,p_currency,p_country,p_status,TRUE,COALESCE(p_as_of,NOW()),1)
    RETURNING account_sk INTO cur_row.account_sk;
    RETURN cur_row.account_sk;
  END IF;

  IF cur_row.customer_bk IS DISTINCT FROM p_customer_bk
     OR cur_row.currency   IS DISTINCT FROM p_currency
     OR cur_row.country    IS DISTINCT FROM p_country
     OR cur_row.status     IS DISTINCT FROM p_status THEN
    UPDATE cur.dim_account
      SET is_current=FALSE, valid_to=COALESCE(p_as_of,NOW())
      WHERE account_sk = cur_row.account_sk;

    INSERT INTO cur.dim_account (account_bk,customer_bk,currency,country,status,is_current,valid_from,version)
    VALUES (p_account_bk,p_customer_bk,p_currency,p_country,p_status,TRUE,COALESCE(p_as_of,NOW()),cur_row.version+1)
    RETURNING account_sk INTO cur_row.account_sk;
  END IF;

  RETURN cur_row.account_sk;
END$$ LANGUAGE plpgsql;

-- DIM MERCHANT (BK = merchant_name_norm; category/country are versioned attrs)
CREATE OR REPLACE FUNCTION cur.upsert_dim_merchant(
  p_name text,
  p_category text,
  p_country char(2),
  p_as_of timestamptz
) RETURNS bigint AS $$
DECLARE cur_row cur.dim_merchant%ROWTYPE;
DECLARE p_norm text := lower(btrim(p_name));
BEGIN
  SELECT * INTO cur_row
  FROM cur.dim_merchant
  WHERE merchant_name_norm = p_norm AND is_current
  ORDER BY valid_from DESC
  LIMIT 1;

  IF cur_row.merchant_sk IS NULL THEN
    INSERT INTO cur.dim_merchant (merchant_name_norm, merchant_category, country, is_current, valid_from, version)
    VALUES (p_norm, p_category, p_country, TRUE, COALESCE(p_as_of,NOW()), 1)
    RETURNING merchant_sk INTO cur_row.merchant_sk;
    RETURN cur_row.merchant_sk;
  END IF;

  IF COALESCE(cur_row.merchant_category,'') IS DISTINCT FROM COALESCE(p_category,'')
     OR COALESCE(cur_row.country,'') IS DISTINCT FROM COALESCE(p_country,'') THEN
    UPDATE cur.dim_merchant
      SET is_current=FALSE, valid_to=COALESCE(p_as_of,NOW())
      WHERE merchant_sk = cur_row.merchant_sk;

    INSERT INTO cur.dim_merchant (merchant_name_norm, merchant_category, country, is_current, valid_from, version)
    VALUES (p_norm, p_category, p_country, TRUE, COALESCE(p_as_of,NOW()), cur_row.version+1)
    RETURNING merchant_sk INTO cur_row.merchant_sk;
  END IF;

  RETURN cur_row.merchant_sk;
END$$ LANGUAGE plpgsql;
