from fastapi import APIRouter, Depends, HTTPException, status, Header
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from ..core.db import get_session
from ..core import schemas
from ..core.models import Transaction, Account, TransactionStatus, Outbox, Idempotency
from ..core.errors import handle_integrity_error
from ..core.idem import canonical_request_hash

router = APIRouter(prefix="/v1/transactions", tags=["transactions"])

ENDPOINT_NAME = "POST /v1/transactions"

@router.post("", response_model=schemas.TransactionOut, status_code=201)
def create_transaction(
    payload: schemas.TransactionCreate,
    session: Session = Depends(get_session),
    idem_key: str | None = Header(default=None, alias="Idempotency-Key")
):
    print("[TRANSACTION] _ POST PAYLOAD:" , payload)
    # --- Idempotency: require a key for write operations ---
    if not idem_key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Idempotency-Key header is required")

    req_hash = canonical_request_hash(ENDPOINT_NAME, payload.model_dump(mode="json"))

    # Try to reserve the key by inserting a row.
    # If it already exists, check hash and potentially return stored response.
    existing = session.execute(
        select(Idempotency).where(
            (Idempotency.endpoint == ENDPOINT_NAME) & (Idempotency.idem_key == idem_key)
        )
    ).scalar_one_or_none()

    if existing:
        # Key already used
        if existing.request_hash != req_hash:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key reused with different request payload"
            )
        if existing.response_code is not None and existing.response_body is not None:
            # Return the previously stored response
            print("[TRANSACTION] _ Return previous")
            return JSONResponse(content=existing.response_body, status_code=existing.response_code)
        # else: another worker might be processing it; for simplicity, tell client to retry later
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Request with this Idempotency-Key is in progress")

    # Reserve the idempotency key now
    reserve = Idempotency(endpoint=ENDPOINT_NAME, idem_key=idem_key, request_hash=req_hash)
    session.add(reserve)
    try:
        session.flush()  # persists reservation inside the same transaction
    except IntegrityError as e:
        # A race could cause another process to insert first: reload and handle as above
        existing = session.execute(
            select(Idempotency).where(
                (Idempotency.endpoint == ENDPOINT_NAME) & (Idempotency.idem_key == idem_key)
            )
        ).scalar_one_or_none()
        if existing and existing.request_hash == req_hash and existing.response_code is not None:
            return JSONResponse(content=existing.response_body, status_code=existing.response_code)
        if existing and existing.request_hash != req_hash:
            raise HTTPException(status_code=409, detail="Idempotency key reused with different request payload")
        # Otherwise fall through and try again (rare path)
        raise

    # --- Domain validations ---
    acc = session.get(Account, payload.account_id)
    if not acc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="account_id not found")
    if payload.currency != acc.currency:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="currency mismatch with account")

    # --- Create Transaction ---
    txn = Transaction(
        account_id=payload.account_id,
        type=payload.type,
        status=TransactionStatus.PENDING,
        amount=payload.amount,
        currency=payload.currency,
        merchant_name=payload.merchant_name,
        merchant_category=payload.merchant_category,
        description=payload.description,
        country=payload.country,
        metadata={}
    )
    session.add(txn)
    try:
        session.flush()  # get txn.id
    except IntegrityError as e:
        # Roll back to a clean error
        handle_integrity_error(e)

    # --- Write Outbox event (same DB transaction) ---
    evt = Outbox(
        aggregate_type="transaction",
        aggregate_id=txn.id,
        event_type="TransactionCreated",
        payload_json={
            "id": str(txn.id),
            "account_id": str(txn.account_id),
            "type": txn.type.value,
            "status": txn.status.value,
            "amount": str(txn.amount),
            "currency": txn.currency,
            "merchant_name": txn.merchant_name,
            "merchant_category": txn.merchant_category,
            "country": txn.country,
            "created_at": txn.created_at.isoformat()
        }
    )
    session.add(evt)
    session.flush()

    # --- Build response and store it in idempotency record ---
    response_body = {"id": str(txn.id), "status": txn.status.value}
    reserve.response_code = 201
    reserve.response_body = response_body
    session.flush()  # ensure the idempotency row is updated before commit

    # The session context manager will commit here
    return response_body

