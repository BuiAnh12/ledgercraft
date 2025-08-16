from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from ..core.db import get_session
from ..core import schemas
from ..core.models import Transaction, Account, TransactionStatus, TransactionType
from ..core.errors import handle_integrity_error
from sqlalchemy.exc import IntegrityError

router = APIRouter(prefix="/v1/transactions", tags=["transactions"])

@router.post("", response_model=schemas.TransactionOut, status_code=201)
def create_transaction(payload: schemas.TransactionCreate, session: Session = Depends(get_session)):
    # Validate account exists
    acc = session.get(Account, payload.account_id)
    if not acc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="account_id not found")

    # Enforce currency match (simple rule for now)
    if payload.currency != acc.currency:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="currency mismatch with account")

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
        session.flush()  # gets generated id
    except IntegrityError as e:
        handle_integrity_error(e)

    return {"id": txn.id, "status": txn.status.value}
