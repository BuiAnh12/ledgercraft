import enum
import uuid
from datetime import datetime
from decimal import Decimal
from sqlalchemy import (
    Column, String, Enum, ForeignKey, CheckConstraint,
    Numeric, Text, JSON, TIMESTAMP, text, CHAR
)
from sqlalchemy.dialects.postgresql import CITEXT, UUID as PG_UUID
from sqlalchemy.orm import declarative_base, relationship, Mapped, mapped_column

Base = declarative_base()

# Match DB enums by name; do not try to create them again
class AccountStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    CLOSED = "CLOSED"

class TransactionStatus(str, enum.Enum):
    PENDING = "PENDING"
    SETTLED = "SETTLED"
    DECLINED = "DECLINED"
    REVERSED = "REVERSED"
    CHARGEBACK = "CHARGEBACK"

class TransactionType(str, enum.Enum):
    PAYMENT = "PAYMENT"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"
    REFUND = "REFUND"
    FEE = "FEE"

class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    email: Mapped[str] = mapped_column(CITEXT, unique=True, nullable=False)
    country: Mapped[str] = mapped_column(CHAR(2), nullable=False)
    kyc_level: Mapped[int] = mapped_column(nullable=False, server_default=text("0"))
    risk_band: Mapped[str] = mapped_column(String, nullable=False, server_default=text("'LOW'"))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=text("NOW()"))

    accounts: Mapped[list["Account"]] = relationship(back_populates="customer", cascade="all,delete")

class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    customer_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("customers.id", ondelete="CASCADE"), nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    country: Mapped[str] = mapped_column(CHAR(2), nullable=False)
    status: Mapped[AccountStatus] = mapped_column(Enum(AccountStatus, name="account_status", create_type=False), nullable=False, server_default=text("'ACTIVE'"))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=text("NOW()"))

    customer: Mapped[Customer] = relationship(back_populates="accounts")
    transactions: Mapped[list["Transaction"]] = relationship(back_populates="account", cascade="all,delete")

class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    account_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    type: Mapped[TransactionType] = mapped_column(Enum(TransactionType, name="transaction_type", create_type=False), nullable=False)
    status: Mapped[TransactionStatus] = mapped_column(Enum(TransactionStatus, name="transaction_status", create_type=False), nullable=False, server_default=text("'PENDING'"))
    amount: Mapped[Decimal] = mapped_column(Numeric(20,6), nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    merchant_name: Mapped[str] = mapped_column(Text, nullable=False)
    merchant_category: Mapped[str | None] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)
    country: Mapped[str] = mapped_column(CHAR(2), nullable=False)
    extra_metadata: Mapped[dict] = mapped_column(
        "metadata", JSON, nullable=False, server_default=text("'{}'::jsonb")
    ) # Reason to name it extra_metadata is due to a bug 'sqlalchemy.exc.InvalidRequestError: Attribute name 'metadata' is reserved when using the Declarative API.'

    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=text("NOW()"))
    settled_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))

    account: Mapped[Account] = relationship(back_populates="transactions")
