from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from ..core.db import get_session
from ..core import schemas
from ..core.models import Account, Customer, AccountStatus
from ..core.errors import handle_integrity_error
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException, status

router = APIRouter(prefix="/v1/accounts", tags=["accounts"])

@router.post("", response_model=schemas.AccountOut, status_code=201)
def create_account(payload: schemas.AccountCreate, session: Session = Depends(get_session)):
    # ensure customer exists
    cust = session.get(Customer, payload.customer_id)
    if not cust:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="customer_id not found")

    acc = Account(customer_id=payload.customer_id, currency=payload.currency, country=payload.country, status=AccountStatus.ACTIVE)
    session.add(acc)
    try:
        session.flush()
    except IntegrityError as e:
        # covers unique open-account-per-currency check
        handle_integrity_error(e)

    return schemas.AccountOut(id=acc.id, customer_id=acc.customer_id, currency=acc.currency, country=acc.country, status=acc.status)
