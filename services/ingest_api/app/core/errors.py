from fastapi import HTTPException, status
from sqlalchemy.exc import IntegrityError
from psycopg.errors import UniqueViolation, ForeignKeyViolation, CheckViolation

def handle_integrity_error(e: IntegrityError):
    orig = getattr(e, "orig", None)
    if isinstance(orig, UniqueViolation):
        # e.g., duplicate email; or unique index on open account+currency
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="conflict: unique constraint violated")
    if isinstance(orig, ForeignKeyViolation):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="bad reference: foreign key violated")
    if isinstance(orig, CheckViolation):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="check constraint violated")
    # Fallback
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="integrity error")
