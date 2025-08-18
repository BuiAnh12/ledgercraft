from datetime import datetime, timedelta
from typing import Optional, Iterable
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values, Json

DEFAULT_ARGS = {"owner": "data", "retries": 0}
TABLES = [
    # table_name, pk, created_col
    ("customers",    "id", "created_at"),
    ("accounts",     "id", "created_at"),
    ("transactions", "id", "created_at"),
    ("outbox",       "id", "created_at"),
]
BATCH_SIZE = 5000

def get_hooks():
    src = PostgresHook.get_hook("oltp_postgres")   # OLTP
    dst = PostgresHook.get_hook("dwh_postgres")    # DWH
    return src, dst

def ensure_watermark(dst: PostgresHook, table: str):
    sql = """
    INSERT INTO stg.etl_watermarks(table_name, last_loaded_at)
    VALUES (%s, TIMESTAMPTZ 'epoch')
    ON CONFLICT (table_name) DO NOTHING
    """
    dst.run(sql, parameters=(table,))

def get_watermark(dst: PostgresHook, table: str) -> datetime:
    return dst.get_first(
        "SELECT last_loaded_at FROM stg.etl_watermarks WHERE table_name=%s",
        parameters=(table,)
    )[0]

def set_watermark(dst: PostgresHook, table: str, value: datetime):
    dst.run(
        "UPDATE stg.etl_watermarks SET last_loaded_at=%s WHERE table_name=%s",
        parameters=(value, table)
    )

def copy_batch(src: PostgresHook, dst: PostgresHook, table: str, pk: str, created_col: str, since: datetime) -> Optional[datetime]:
    conn = src.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT * FROM {table}
        WHERE {created_col} > %s
        ORDER BY {created_col} ASC
        LIMIT {BATCH_SIZE}
        """,
        (since,)
    )
    rows = cursor.fetchall()
    if not rows:
        cursor.close()
        return None

    cols = [desc[0] for desc in cursor.description]
    cursor.close()

    # transform dict → Json() for psycopg2
    def adapt_row(row):
        return tuple(Json(v) if isinstance(v, dict) else v for v in row)

    adapted_rows = [adapt_row(r) for r in rows]

    collist = ",".join(cols)
    insert_sql = f"""
        INSERT INTO stg.{table} ({collist})
        VALUES %s
        ON CONFLICT ({pk}) DO NOTHING
    """

    dst_conn = dst.get_conn()
    with dst_conn.cursor() as cur:
        execute_values(cur, insert_sql, adapted_rows, page_size=1000)
    dst_conn.commit()

    # Return new watermark
    created_idx = cols.index(created_col)
    max_created = max(r[created_idx] for r in rows)
    return max_created
with DAG(
    dag_id="oltp_to_stg",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["staging","postgres"],
) as dag:

    @task
    def ensure_staging_objects():
        # This assumes you already ran migrate-dwh-stg from Makefile.
        # If you want, you can run CREATEs here, but keeping DDL separate is cleaner.
        return "ok"

    @task
    def load_table(table: str, pk: str, created_col: str) -> str:
        src, dst = get_hooks()
        ensure_watermark(dst, table)
        since = get_watermark(dst, table)

        moved = 0
        last = since
        while True:
            new_last = copy_batch(src, dst, table, pk, created_col, last)
            if not new_last:
                break
            moved += 1
            last = new_last
            set_watermark(dst, table, last)

        return f"{table}: batches={moved}, new_watermark={last.isoformat()}"

    ensure = ensure_staging_objects()
    # chain loads (customers -> accounts -> transactions -> outbox)
    logs = []
    for name, pk, created_col in TABLES:
        t = load_table.override(task_id=f"load_{name}")(name, pk, created_col)
        ensure >> t
        logs.append(t)
