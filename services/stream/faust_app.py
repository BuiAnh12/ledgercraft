import faust
import uuid
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque
from typing import Deque, Tuple
import json
import logging
import asyncio

from config import (
    KAFKA_BROKER_URL, WINDOW_SECONDS, COUNT_THRESHOLD,
    SUM_THRESHOLD_VND, SUM_THRESHOLD_USD
)
from db import upsert_risk_flag

import sys

logging.basicConfig(
    level=logging.INFO,  # 👈 only INFO, WARNING, ERROR, CRITICAL
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)

# ---------- Helpers ----------
def parse_dt(s: str | None) -> datetime:
    if not s:
        return datetime.now(timezone.utc)
    # Debezium uses ISO with Z; Python expects +00:00
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

def loads(value_bytes):
    try:
        return json.loads(value_bytes)
    except Exception:
        # In case value is bytes-like already convertible
        return None

def get_header(event, name: str) -> str | None:
    # Debezium headers are on the underlying Kafka message
    msg = getattr(event, "message", None)
    headers = getattr(msg, "headers", None)
    if not headers:
        return None
    lname = name.lower()
    for k, v in headers:
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else k
        if kk.lower() == lname:
            return v.decode() if isinstance(v, (bytes, bytearray)) else v
    return None

def payload_of(obj):
    """Debezium with JsonConverter(schemas=true) produces {'schema':..., 'payload': {...}} after unwrap.
    If schemas=false, value is already the row dict. Support both."""
    if isinstance(obj, dict) and "payload" in obj and isinstance(obj["payload"], dict):
        return obj["payload"]
    return obj

def floor_window(ts: datetime, seconds: int) -> tuple[datetime, datetime]:
    start = ts - timedelta(seconds=(ts.timestamp() % seconds))
    start = start.replace(microsecond=0)
    return start, start + timedelta(seconds=seconds)

# ---------- App & Topics ----------
app = faust.App(
    "lc-stream",
    broker=KAFKA_BROKER_URL,
    value_serializer="raw",
)

app.conf.consumer_auto_offset_reset = "earliest"

topic_txns = app.topic("txn.public.transactions")
topic_flags = app.topic("risk.flags", value_serializer="json")

log = logging.getLogger("lc-stream")

# ---------- In-memory sliding windows per account ----------
# For each account_id, keep a deque of (event_time, amount_decimal)
Window = Deque[Tuple[datetime, Decimal]]
windows: dict[str, Window] = defaultdict(deque)

NAMESPACE = uuid.UUID("00000000-0000-0000-0000-000000000001")  # deterministic namespace for uuid5

def thresholds_for_currency(cur: str) -> Decimal:
    return SUM_THRESHOLD_VND if cur == "VND" else SUM_THRESHOLD_USD

# ---------- Agent ----------
@app.agent(topic_txns)
async def process_transactions(stream):
    async for event in stream.events():
        raw = event.value
        if raw is None:
            log.info("event with None value, skipping")
            continue

        v = loads(raw)
        if not isinstance(v, dict):
            log.info(f"non-dict value {raw!r}, skipping")
            continue
        v = payload_of(v)

        op = get_header(event, "op")
        log.info(f"txn received op={op}, payload={v}")

        if op and op != "c":
            log.info("op not 'c', skipping")
            continue

        account_id = v.get("account_id")
        tx_id = v.get("id")
        amount = v.get("amount")
        currency = v.get("currency", "VND")
        created_at = parse_dt(v.get("created_at"))

        log.info(f"parsed tx id={tx_id} acct={account_id} amt={amount} cur={currency} ts={created_at}")

        if not account_id or not amount:
            log.info("missing account_id/amount; skipping")
            continue

        try:
            amt = Decimal(str(amount))
        except Exception as e:
            log.info(f"bad amount {amount}: {e}")
            continue

        key = str(account_id)
        dq: Window = windows[key]
        dq.append((created_at, amt))
        cutoff = created_at - timedelta(seconds=WINDOW_SECONDS)

        # Expire old
        while dq and dq[0][0] < cutoff:
            dropped = dq.popleft()
            log.info(f"dropped expired tx {dropped} for acct={account_id}")

        total = sum(a for ts, a in dq if ts >= cutoff)
        count = sum(1 for ts, _ in dq if ts >= cutoff)
        log.info(f"acct={account_id} window count={count} sum={total} cutoff={cutoff.isoformat()} dq={list(dq)}")

        sum_threshold = thresholds_for_currency(currency)
        reason = None
        if count >= COUNT_THRESHOLD:
            reason = "VELOCITY_COUNT"
        if total >= sum_threshold:
            reason = reason or "VELOCITY_SUM"

        if reason:
            w_start, w_end = floor_window(created_at, WINDOW_SECONDS)
            eid = uuid.uuid5(NAMESPACE, f"{account_id}:{w_end.isoformat()}:{reason}")

            row = {
                "event_id": eid,
                "account_id": account_id,
                "window_start": w_start,
                "window_end": w_end,
                "count_5m": count,
                "sum_5m": total,
                "reason": reason,
            }
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, upsert_risk_flag, row)

            flag_msg = {
                "event_id": str(eid),
                "account_id": str(account_id),
                "window": {"start": w_start.isoformat(), "end": w_end.isoformat()},
                "count_5m": count,
                "sum_5m": str(total),
                "reason": reason,
                "tx_example": str(tx_id),
            }
            await topic_flags.send(key=str(account_id), value=flag_msg)

            log.info(f"FLAG {reason} acct={account_id} count={count} sum={total} window_end={w_end.isoformat()} (op={op})")
        else:
            log.info(f"no flag acct={account_id} count={count} sum={total} (op={op})")
