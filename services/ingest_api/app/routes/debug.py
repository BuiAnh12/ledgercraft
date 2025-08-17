from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..core.db import get_session

router = APIRouter(prefix="/v1/debug", tags=["debug"])

@router.get("/outbox")
def list_outbox(limit: int = Query(default=10, ge=1, le=100), session: Session = Depends(get_session)):
    rows = session.execute(
        text("""
            SELECT id::text, aggregate_type, aggregate_id::text, event_type,
                   payload_json, created_at
            FROM outbox
            ORDER BY created_at DESC
            LIMIT :limit
        """),
        {"limit": limit}
    ).mappings().all()
    return {"items": [dict(r) for r in rows]}
