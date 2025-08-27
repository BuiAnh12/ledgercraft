from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "curated_load"

TABLE_STATES = {
    "dim_customer": "cur.dim_customer",
    "dim_account": "cur.dim_account",
    "dim_merchant": "cur.dim_merchant",
    "fact_transactions": "cur.fact_transactions",
}

def ensure_funcs_and_partitions(**_):
    dwh = PostgresHook(postgres_conn_id="dwh_postgres")
    sql = open("/opt/airflow/sql/dwh_curated_functions.sql", "r", encoding="utf-8").read()
    dwh.run(sql)
    # ensure current month partitions exist (safety)
    dwh.run("SELECT cur.ensure_fact_partition(CURRENT_DATE);")

# ----- watermark helpers shared with staging -----
def _get_last_ts(dwh, table: str):
    rows = dwh.get_records(
        "SELECT last_loaded_at FROM stg.etl_watermarks WHERE table_name = %s", parameters=(table,)
    )
    return rows[0][0] if rows and rows[0][0] else pendulum.datetime(1970,1,1,tz="UTC")

def _set_last_ts(dwh, table: str, ts):
    dwh.run("""
        INSERT INTO stg.etl_watermarks(table_name, last_loaded_at)
        VALUES (%s, %s)
        ON CONFLICT (table_name) DO UPDATE SET last_loaded_at = EXCLUDED.last_loaded_at
    """, parameters=(table, ts))

# ----- upsert dimensions from staging -----
def upsert_dim_customer(**_):
    dwh = PostgresHook(postgres_conn_id="dwh_postgres")
    last_ts = _get_last_ts(dwh, "cur.dim_customer")
    # Use the latest of created/updated as the "as_of" time
    sql = """
    WITH s AS (
      SELECT id, email, country, kyc_level, risk_band, GREATEST(created_at, updated_at) AS as_of
      FROM stg.customers
      WHERE GREATEST(created_at, updated_at) > %s
      ORDER BY 5
    )
    SELECT cur.upsert_dim_customer(id, email, country, kyc_level, risk_band, as_of) FROM s;
    SELECT COALESCE(MAX(GREATEST(created_at, updated_at)), %s) FROM stg.customers;
    """
    # run and capture the max ts
    with dwh.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (last_ts, last_ts))
            # the second SELECT returns one row
            for _ in range(0): pass  # consume first SELECT's result set (ignored)
            conn.commit()
    # Fetch max ts separately (simplify)
    max_ts = dwh.get_first("SELECT COALESCE(MAX(GREATEST(created_at, updated_at)), %s) FROM stg.customers", parameters=(last_ts,))[0]
    if max_ts:
        _set_last_ts(dwh, "cur.dim_customer", max_ts)

def upsert_dim_account(**_):
    dwh = PostgresHook(postgres_conn_id="dwh_postgres")
    last_ts = _get_last_ts(dwh, "cur.dim_account")
    sql = """
    WITH s AS (
      SELECT id, customer_id, currency, country, status, GREATEST(created_at, updated_at) AS as_of
      FROM stg.accounts
      WHERE GREATEST(created_at, updated_at) > %s
      ORDER BY 6
    )
    SELECT cur.upsert_dim_account(id, customer_id, currency, country, status, as_of) FROM s;
    """
    dwh.run(sql, parameters=(last_ts,))
    max_ts = dwh.get_first("SELECT COALESCE(MAX(GREATEST(created_at, updated_at)), %s) FROM stg.accounts", parameters=(last_ts,))[0]
    if max_ts:
        _set_last_ts(dwh, "cur.dim_account", max_ts)

def upsert_dim_merchant(**_):
    dwh = PostgresHook(postgres_conn_id="dwh_postgres")
    last_ts = _get_last_ts(dwh, "cur.dim_merchant")
    # derive distinct (name, category, country) from new transactions
    sql = """
    WITH src AS (
      SELECT merchant_name, merchant_category, country, MIN(created_at) AS as_of
      FROM stg.transactions
      WHERE created_at > %s
      GROUP BY merchant_name, merchant_category, country
    )
    SELECT cur.upsert_dim_merchant(merchant_name, merchant_category, country, as_of) FROM src;
    """
    dwh.run(sql, parameters=(last_ts,))
    max_ts = dwh.get_first("SELECT COALESCE(MAX(created_at), %s) FROM stg.transactions", parameters=(last_ts,))[0]
    if max_ts:
        _set_last_ts(dwh, "cur.dim_merchant", max_ts)

# ----- load fact (incremental, idempotent) -----
def load_fact_transactions(**_):
    dwh = PostgresHook(postgres_conn_id="dwh_postgres")
    last_ts = _get_last_ts(dwh, "cur.fact_transactions")

    # Ensure needed monthly partitions exist for the incoming dates
    dwh.run("""
    DO $$
    DECLARE d date;
    BEGIN
      FOR d IN
        SELECT DISTINCT (created_at AT TIME ZONE 'UTC')::date
        FROM stg.transactions
        WHERE created_at > %s
      LOOP
        PERFORM cur.ensure_fact_partition(d);
      END LOOP;
    END$$;
    """, parameters=(last_ts,))

    # Insert facts with SCD2-as-of joins, ON CONFLICT to be idempotent
    dwh.run("""
    INSERT INTO cur.fact_transactions
      (transaction_bk, customer_sk, account_sk, merchant_sk,
       type, status, amount, currency, country, created_at, settled_at, created_date)
    SELECT
      t.id AS transaction_bk,
      -- account_sk as of created_at
      (
        SELECT account_sk FROM cur.dim_account a
        WHERE a.account_bk = t.account_id
          AND a.valid_from <= t.created_at
          AND (a.valid_to IS NULL OR t.created_at < a.valid_to)
        ORDER BY a.valid_from DESC
        LIMIT 1
      ) AS account_sk,
      -- customer_sk as of created_at (via account's customer_bk)
      (
        SELECT c.customer_sk FROM cur.dim_customer c
        WHERE c.customer_bk = (
          SELECT a.customer_bk FROM cur.dim_account a
          WHERE a.account_bk = t.account_id
            AND a.valid_from <= t.created_at
            AND (a.valid_to IS NULL OR t.created_at < a.valid_to)
          ORDER BY a.valid_from DESC
          LIMIT 1
        )
          AND c.valid_from <= t.created_at
          AND (c.valid_to IS NULL OR t.created_at < c.valid_to)
        ORDER BY c.valid_from DESC
        LIMIT 1
      ) AS customer_sk,
      -- merchant_sk as of created_at
      (
        SELECT m.merchant_sk FROM cur.dim_merchant m
        WHERE m.merchant_name_norm = lower(btrim(t.merchant_name))
          AND m.valid_from <= t.created_at
          AND (m.valid_to IS NULL OR t.created_at < m.valid_to)
        ORDER BY m.valid_from DESC
        LIMIT 1
      ) AS merchant_sk,
      t.type, t.status, t.amount, t.currency, t.country, t.created_at, t.settled_at,
      (t.created_at AT TIME ZONE 'UTC')::date AS created_date
    FROM stg.transactions t
    WHERE t.created_at > %s
    -- only rows that resolved all keys:
    AND EXISTS (
      SELECT 1 FROM cur.dim_account a
      WHERE a.account_bk = t.account_id
        AND a.valid_from <= t.created_at
        AND (a.valid_to IS NULL OR t.created_at < a.valid_to)
    )
    AND EXISTS (
      SELECT 1 FROM cur.dim_merchant m
      WHERE m.merchant_name_norm = lower(btrim(t.merchant_name))
        AND m.valid_from <= t.created_at
        AND (m.valid_to IS NULL OR t.created_at < m.valid_to)
    )
    ON CONFLICT (transaction_bk, created_date) DO NOTHING;
    """, parameters=(last_ts,))

    max_ts = dwh.get_first("SELECT COALESCE(MAX(created_at), %s) FROM stg.transactions", parameters=(last_ts,))[0]
    if max_ts:
        _set_last_ts(dwh, "cur.fact_transactions", max_ts)

with DAG(
    dag_id=DAG_ID,
    schedule="@hourly",
    start_date=pendulum.now("UTC").subtract(days=1),
    catchup=False,
    default_args={"owner": "data", "retries": 0},
    tags=["curated","dwh","scd2"],
) as dag:

    t0 = PythonOperator(task_id="ensure_funcs_and_partitions", python_callable=ensure_funcs_and_partitions)
    d1 = PythonOperator(task_id="upsert_dim_customer", python_callable=upsert_dim_customer)
    d2 = PythonOperator(task_id="upsert_dim_account",  python_callable=upsert_dim_account)
    d3 = PythonOperator(task_id="upsert_dim_merchant", python_callable=upsert_dim_merchant)
    f1 = PythonOperator(task_id="load_fact_transactions", python_callable=load_fact_transactions)

    t0 >> [d1, d2, d3] >> f1
