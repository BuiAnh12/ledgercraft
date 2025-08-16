from pydantic import BaseModel, Field, EmailStr, field_validator
from decimal import Decimal
from typing import Optional
from uuid import UUID
from enum import Enum
import re

CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
COUNTRY_RE = re.compile(r"^[A-Z]{2}$")

class AccountStatus(str, Enum):
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    CLOSED = "CLOSED"

class TransactionType(str, Enum):
    PAYMENT = "PAYMENT"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"
    REFUND = "REFUND"
    FEE = "FEE"

class CustomerCreate(BaseModel):
    email: EmailStr
    country: str = Field(..., description="ISO 3166-1 alpha-2 e.g., 'VN'")
    kyc_level: int = 0
    risk_band: str = "LOW"

    @field_validator("country")
    @classmethod
    def valid_country(cls, v: str) -> str:
        if not COUNTRY_RE.match(v):
            raise ValueError("country must be 2 uppercase letters (ISO alpha-2)")
        return v

class CustomerOut(BaseModel):
    id: UUID
    email: EmailStr
    country: str
    kyc_level: int
    risk_band: str

class AccountCreate(BaseModel):
    customer_id: UUID
    currency: str
    country: str = "VN"

    @field_validator("currency")
    @classmethod
    def valid_currency(cls, v: str) -> str:
        if not CURRENCY_RE.match(v):
            raise ValueError("currency must be 3 uppercase letters")
        return v

    @field_validator("country")
    @classmethod
    def valid_country(cls, v: str) -> str:
        if not COUNTRY_RE.match(v):
            raise ValueError("country must be 2 uppercase letters")
        return v

class AccountOut(BaseModel):
    id: UUID
    customer_id: UUID
    currency: str
    country: str
    status: AccountStatus

class TransactionCreate(BaseModel):
    account_id: UUID
    type: TransactionType
    amount: Decimal = Field(..., gt=0)
    currency: str
    merchant_name: str
    merchant_category: Optional[str] = None
    description: Optional[str] = None
    country: str

    @field_validator("currency")
    @classmethod
    def valid_currency(cls, v: str) -> str:
        if not CURRENCY_RE.match(v):
            raise ValueError("currency must be 3 uppercase letters")
        return v

    @field_validator("country")
    @classmethod
    def valid_country(cls, v: str) -> str:
        if not COUNTRY_RE.match(v):
            raise ValueError("country must be 2 uppercase letters")
        return v

class TransactionOut(BaseModel):
    id: UUID
    status: str
