from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from ..core.db import get_session
from ..core import schemas
from ..core.models import Customer
from ..core.errors import handle_integrity_error
from sqlalchemy.exc import IntegrityError

router = APIRouter(prefix="/v1/customers", tags=["customers"])

@router.post("", response_model=schemas.CustomerOut, status_code=201)
def create_customer(payload: schemas.CustomerCreate, session: Session = Depends(get_session)):
    c = Customer(email=payload.email, country=payload.country, kyc_level=payload.kyc_level, risk_band=payload.risk_band)
    session.add(c)
    try:
        session.flush()
    except IntegrityError as e:
        handle_integrity_error(e)
    return schemas.CustomerOut(id=c.id, email=c.email, country=c.country, kyc_level=c.kyc_level, risk_band=c.risk_band)
