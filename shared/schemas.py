from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field


class TokenRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class AccountCreate(BaseModel):
    owner_id: str
    currency: str = "USD"
    balance: Decimal = Field(gt=0)


class AccountResponse(BaseModel):
    id: str
    owner_id: str
    currency: str
    balance: Decimal
    reserved_balance: Decimal
    version: int


class PaymentCreate(BaseModel):
    source_account_id: str
    destination_account_id: str
    amount: Decimal = Field(gt=0)
    currency: str = "USD"
    metadata: dict[str, Any] = Field(default_factory=dict)


class PaymentResponse(BaseModel):
    id: str
    state: str
    amount: Decimal
    currency: str
    source_account_id: str
    destination_account_id: str
    idempotency_key: str
    version: int
    failure_reason: str | None = None
    correlation_id: str
    metadata: dict[str, Any]


class TransferCreate(PaymentCreate):
    pass
