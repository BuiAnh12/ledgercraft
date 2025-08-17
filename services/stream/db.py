import psycopg
from typing import Any
from config import DATABASE_URL

_conn = None

def conn():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg.connect(DATABASE_URL, autocommit=True)
    return _conn

def upsert_risk_flag(row: dict[str, Any]) -> None:
    """
    INSERT risk flag; use ON CONFLICT DO NOTHING for idempotency.
    row keys: event_id, account_id, window_start, window_end, count_5m, sum_5m, reason
    """
    with conn().cursor() as cur:
        cur.execute(
            """
            INSERT INTO risk_flags (event_id, account_id, window_start, window_end, count_5m, sum_5m, reason)
            VALUES (%(event_id)s, %(account_id)s, %(window_start)s, %(window_end)s, %(count_5m)s, %(sum_5m)s, %(reason)s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            row
        )
