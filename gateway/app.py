from __future__ import annotations

from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import PlainTextResponse
from sqlalchemy import select

from shared.db import init_db, session_scope
from shared.domain import create_account, create_payment, get_account, serialize_payment
from shared.logging import configure_logging, correlation_id_var
from shared.metrics import increment, render_metrics
from shared.models import AuditLog, Payment
from shared.rate_limit import enforce_rate_limit
from shared.schemas import AccountCreate, AccountResponse, PaymentCreate, PaymentResponse, TokenRequest, TokenResponse, TransferCreate
from shared.security import issue_token, require_auth

@asynccontextmanager
async def lifespan(_app: FastAPI):
    configure_logging("gateway")
    await init_db()
    yield


app = FastAPI(title="Payment Platform Gateway", lifespan=lifespan)


@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid4()))
    correlation_id_var.set(correlation_id)
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    return response


@app.post("/auth/token", response_model=TokenResponse)
async def create_token(request: TokenRequest) -> TokenResponse:
    increment("token_requests_total")
    return TokenResponse(access_token=issue_token(request.username, request.password))


@app.post("/accounts", response_model=AccountResponse)
async def create_account_endpoint(payload: AccountCreate, user=Depends(require_auth)) -> AccountResponse:
    await enforce_rate_limit(user["sub"])
    async with session_scope() as session:
        account = await create_account(session, payload)
        increment("accounts_created_total")
        return AccountResponse.model_validate(account, from_attributes=True)


@app.get("/accounts/{account_id}", response_model=AccountResponse)
async def get_account_endpoint(account_id: str, user=Depends(require_auth)) -> AccountResponse:
    await enforce_rate_limit(user["sub"])
    async with session_scope() as session:
        account = await get_account(session, account_id)
        if account is None:
            raise HTTPException(status_code=404, detail="account not found")
        increment("account_reads_total")
        return AccountResponse.model_validate(account, from_attributes=True)


@app.post("/payments", response_model=PaymentResponse, status_code=202)
async def create_payment_endpoint(payload: PaymentCreate, idempotency_key: str = Header(default="", alias="Idempotency-Key"), user=Depends(require_auth)) -> PaymentResponse:
    await enforce_rate_limit(user["sub"])
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")
    async with session_scope() as session:
        payment = await create_payment(session, payload, idempotency_key, correlation_id_var.get())
        increment("payments_created_total")
        return PaymentResponse(**serialize_payment(payment))


@app.post("/transfers", response_model=PaymentResponse, status_code=202)
async def create_transfer_endpoint(payload: TransferCreate, idempotency_key: str = Header(default="", alias="Idempotency-Key"), user=Depends(require_auth)) -> PaymentResponse:
    return await create_payment_endpoint(PaymentCreate(**payload.model_dump()), idempotency_key, user)


@app.get("/payments/{payment_id}", response_model=PaymentResponse)
async def get_payment_endpoint(payment_id: str, user=Depends(require_auth)) -> PaymentResponse:
    await enforce_rate_limit(user["sub"])
    async with session_scope() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            raise HTTPException(status_code=404, detail="payment not found")
        increment("payment_reads_total")
        return PaymentResponse(**serialize_payment(payment))


@app.get("/payments/{payment_id}/audit")
async def get_payment_audit(payment_id: str, user=Depends(require_auth)):
    await enforce_rate_limit(user["sub"])
    async with session_scope() as session:
        rows = list((await session.execute(select(AuditLog).where(AuditLog.aggregate_type == "payment", AuditLog.aggregate_id == payment_id).order_by(AuditLog.created_at))).scalars())
        return [{"action": row.action, "state_from": row.state_from, "state_to": row.state_to, "payload": row.payload} for row in rows]


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gateway"}


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics(user=Depends(require_auth)) -> str:
    return render_metrics()
