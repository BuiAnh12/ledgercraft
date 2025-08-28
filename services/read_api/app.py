from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import os
import psycopg
from datetime import datetime, timedelta

# --- Env ---
DWH_DSN = os.getenv("DWH_DSN", "postgresql://dwh:dwh_password@dwh-postgres:5432/ledgercraft_dwh")
OLTP_DSN = os.getenv("OLTP_DSN", "postgresql://app:app_password@oltp-postgres:5432/ledgercraft_oltp")
API_PORT = int(os.getenv("READ_API_PORT", "8002"))
ALLOWED_ORIGINS = os.getenv("READ_API_CORS", "http://localhost:3000").split(",")

app = FastAPI(title="LedgerCraft Read API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in ALLOWED_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Models (response shapes) ---
class GMVPoint(BaseModel):
    day_utc: str         # YYYY-MM-DD
    currency: str
    tx_count: int
    total_amount: str    # keep as string to avoid float issues

class MerchantRow(BaseModel):
    merchant_name_norm: str
    tx_count: int
    total_amount: str

class RiskFlag(BaseModel):
    event_id: str
    account_id: str
    window_start: datetime
    window_end: datetime
    count_5m: int
    sum_5m: str
    reason: str
    created_at: datetime

# --- DB helpers (simple pooled connections via context managers) ---
def dwh_conn():
    return psycopg.connect(DWH_DSN)

def oltp_conn():
    return psycopg.connect(OLTP_DSN)

# --- Endpoints ---

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/gmv/daily", response_model=List[GMVPoint])
def gmv_daily(currency: str = Query("VND", min_length=3, max_length=3), days: int = Query(30, ge=1, le=365)):
    """
    Returns last N days GMV from materialized view cur.mv_gmv_daily_currency for a given currency.
    """
    with dwh_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT day_utc::text, currency, tx_count, total_amount::text
            FROM cur.mv_gmv_daily_currency
            WHERE currency = %s
              AND day_utc >= (CURRENT_DATE - %s::int)
            ORDER BY day_utc ASC
            """,
            (currency, days,)
        )
        rows = cur.fetchall()
        return [GMVPoint(day_utc=r[0], currency=r[1], tx_count=r[2], total_amount=r[3]) for r in rows]

@app.get("/merchants/top", response_model=List[MerchantRow])
def merchants_top(limit: int = Query(10, ge=1, le=100)):
    """
    Top merchants by total amount (7d) from cur.mv_top_merchants_7d.
    """
    try:
        with dwh_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT merchant_name_norm, tx_count, total_amount::text
                FROM cur.mv_top_merchants_7d
                ORDER BY total_amount DESC
                LIMIT %s
                """,
                (limit,)
            )
            rows = cur.fetchall()
            return [
                MerchantRow(merchant_name_norm=r[0], tx_count=r[1], total_amount=r[2])
                for r in rows
            ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"/merchants/top failed: {e}")

@app.get("/risk/flags", response_model=List[RiskFlag])
def risk_flags(hours: int = Query(1, ge=1, le=24)):
    """
    Recent risk flags from OLTP (near real-time). Default: last 1 hour.
    """
    with oltp_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_id::text, account_id::text, window_start, window_end,
                   count_5m, sum_5m::text, reason, created_at
            FROM risk_flags
            WHERE created_at >= (NOW() - (%s || ' hour')::interval)
            ORDER BY created_at DESC
            """,
            (hours,)
        )
        rows = cur.fetchall()
        return [
            RiskFlag(
                event_id=r[0], account_id=r[1], window_start=r[2], window_end=r[3],
                count_5m=r[4], sum_5m=r[5], reason=r[6], created_at=r[7]
            ) for r in rows
        ]

# Note: run with: uvicorn app:app --host 0.0.0.0 --port $READ_API_PORT
