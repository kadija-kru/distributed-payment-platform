from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

from fastapi import HTTPException, status
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from shared.broker import already_processed, mark_processed, queue_outbox_event
from shared.cache import get_cache
from shared.config import get_settings
from shared.events import (
    EventEnvelope,
    FRAUD_APPROVED,
    FRAUD_REJECTED,
    FUNDS_RELEASED,
    FUNDS_RESERVED,
    LEDGER_POSTED,
    PAYMENT_COMPLETED,
    PAYMENT_CREATED,
    PAYMENT_FAILED,
    RELEASE_FUNDS_REQUESTED,
)
from shared.models import Account, AuditLog, LedgerEntry, Payment, PaymentState
from shared.schemas import AccountCreate, PaymentCreate


def serialize_payment(payment: Payment) -> dict:
    return {
        "id": payment.id,
        "state": payment.state.value,
        "amount": payment.amount,
        "currency": payment.currency,
        "source_account_id": payment.source_account_id,
        "destination_account_id": payment.destination_account_id,
        "idempotency_key": payment.idempotency_key,
        "version": payment.version,
        "failure_reason": payment.failure_reason,
        "correlation_id": payment.correlation_id,
        "metadata": payment.payment_metadata,
    }


async def add_audit(session: AsyncSession, aggregate_type: str, aggregate_id: str, action: str, correlation_id: str, state_from: str | None, state_to: str | None, payload: dict) -> None:
    session.add(AuditLog(aggregate_type=aggregate_type, aggregate_id=aggregate_id, action=action, correlation_id=correlation_id, state_from=state_from, state_to=state_to, payload=payload))


async def create_account(session: AsyncSession, request: AccountCreate) -> Account:
    account = Account(id=str(uuid4()), owner_id=request.owner_id, currency=request.currency, balance=request.balance, reserved_balance=Decimal("0.00"), version=1)
    session.add(account)
    await session.flush()
    await add_audit(session, "account", account.id, "account.created", "system", None, None, {"owner_id": request.owner_id})
    return account


async def get_account(session: AsyncSession, account_id: str) -> Account | None:
    cache = await get_cache()
    cached = await cache.get_json(f"account:{account_id}")
    if cached:
        return Account(
            id=cached["id"], owner_id=cached["owner_id"], currency=cached["currency"], balance=Decimal(str(cached["balance"])), reserved_balance=Decimal(str(cached["reserved_balance"])), version=cached["version"]
        )
    account = await session.get(Account, account_id)
    if account:
        await cache.set_json(
            f"account:{account_id}",
            {
                "id": account.id,
                "owner_id": account.owner_id,
                "currency": account.currency,
                "balance": float(account.balance),
                "reserved_balance": float(account.reserved_balance),
                "version": account.version,
            },
            get_settings().cache_ttl_seconds,
        )
    return account


async def invalidate_account_cache(account_id: str) -> None:
    cache = await get_cache()
    await cache.delete(f"account:{account_id}")


async def create_payment(session: AsyncSession, request: PaymentCreate, idempotency_key: str, correlation_id: str) -> Payment:
    existing = (await session.execute(select(Payment).where(Payment.idempotency_key == idempotency_key))).scalar_one_or_none()
    if existing:
        return existing
    if request.source_account_id == request.destination_account_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source and destination must differ")
    source = await session.get(Account, request.source_account_id)
    destination = await session.get(Account, request.destination_account_id)
    if not source or not destination:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="account not found")
    payment = Payment(
        id=str(uuid4()),
        source_account_id=request.source_account_id,
        destination_account_id=request.destination_account_id,
        amount=request.amount,
        currency=request.currency,
        state=PaymentState.CREATED,
        idempotency_key=idempotency_key,
        correlation_id=correlation_id,
        payment_metadata=request.metadata,
        version=1,
    )
    session.add(payment)
    await session.flush()
    await add_audit(session, "payment", payment.id, PAYMENT_CREATED, correlation_id, None, PaymentState.CREATED.value, request.metadata)
    event = EventEnvelope(PAYMENT_CREATED, payment.id, correlation_id, {**request.model_dump(mode="json"), "payment_id": payment.id})
    queue_outbox_event(session, get_settings().kafka_topic, event.as_dict(), payment.id)
    return payment


async def transition_payment(session: AsyncSession, payment_id: str, expected_state: PaymentState, new_state: PaymentState, correlation_id: str, payload: dict, failure_reason: str | None = None) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise ValueError(f"missing payment {payment_id}")
    if payment.state == new_state:
        return payment
    if payment.state != expected_state:
        raise ValueError(f"invalid transition {payment.state.value} -> {new_state.value}")
    current_version = payment.version
    result = await session.execute(
        update(Payment).where(Payment.id == payment_id, Payment.version == current_version).values(state=new_state, version=current_version + 1, failure_reason=failure_reason)
    )
    if result.rowcount != 1:
        raise ValueError("optimistic concurrency conflict")
    await session.flush()
    payment = await session.get(Payment, payment_id)
    await session.refresh(payment)
    await add_audit(session, "payment", payment_id, f"state.changed.{new_state.value.lower()}", correlation_id, expected_state.value, new_state.value, payload)
    return payment


async def process_accounts_event(session: AsyncSession, event: dict) -> None:
    if await already_processed(session, "accounts", event):
        return
    payment = await session.get(Payment, event["payment_id"])
    if payment is None:
        return
    if event["event_type"] == PAYMENT_CREATED:
        source = await session.get(Account, payment.source_account_id)
        if source.balance - source.reserved_balance < payment.amount:
            raise ValueError("insufficient funds")
        source.reserved_balance += payment.amount
        source.version += 1
        await invalidate_account_cache(source.id)
        await add_audit(session, "account", source.id, "funds.reserved", payment.correlation_id, None, None, {"payment_id": payment.id, "amount": str(payment.amount)})
        next_event = EventEnvelope(FUNDS_RESERVED, payment.id, payment.correlation_id, {"payment_id": payment.id})
        queue_outbox_event(session, get_settings().kafka_topic, next_event.as_dict(), payment.id)
    elif event["event_type"] == RELEASE_FUNDS_REQUESTED:
        source = await session.get(Account, payment.source_account_id)
        source.reserved_balance = max(Decimal("0.00"), source.reserved_balance - payment.amount)
        source.version += 1
        await invalidate_account_cache(source.id)
        await add_audit(session, "account", source.id, "funds.released", payment.correlation_id, None, None, {"payment_id": payment.id, "amount": str(payment.amount)})
        next_event = EventEnvelope(FUNDS_RELEASED, payment.id, payment.correlation_id, {"payment_id": payment.id})
        queue_outbox_event(session, get_settings().kafka_topic, next_event.as_dict(), payment.id)
    await mark_processed(session, "accounts", event)


async def process_fraud_event(session: AsyncSession, event: dict) -> None:
    if await already_processed(session, "fraud", event) or event["event_type"] != FUNDS_RESERVED:
        return
    payment = await session.get(Payment, event["payment_id"])
    if payment is None:
        return
    if payment.payment_metadata.get("force_fraud_reject") or payment.amount >= Decimal(str(get_settings().fraud_reject_above)):
        next_type = FRAUD_REJECTED
    else:
        next_type = FRAUD_APPROVED
    next_event = EventEnvelope(next_type, payment.id, payment.correlation_id, {"payment_id": payment.id})
    queue_outbox_event(session, get_settings().kafka_topic, next_event.as_dict(), payment.id)
    await mark_processed(session, "fraud", event)


async def process_ledger_event(session: AsyncSession, event: dict) -> None:
    if await already_processed(session, "ledger", event) or event["event_type"] != FRAUD_APPROVED:
        return
    payment = await session.get(Payment, event["payment_id"])
    if payment is None:
        return
    if payment.payment_metadata.get("force_ledger_failure"):
        raise RuntimeError("simulated ledger failure")
    source = await session.get(Account, payment.source_account_id)
    destination = await session.get(Account, payment.destination_account_id)
    source.balance -= payment.amount
    source.reserved_balance = max(Decimal("0.00"), source.reserved_balance - payment.amount)
    source.version += 1
    destination.balance += payment.amount
    destination.version += 1
    session.add_all([
        LedgerEntry(payment_id=payment.id, account_id=source.id, entry_type="DEBIT", amount=payment.amount),
        LedgerEntry(payment_id=payment.id, account_id=destination.id, entry_type="CREDIT", amount=payment.amount),
    ])
    await invalidate_account_cache(source.id)
    await invalidate_account_cache(destination.id)
    next_event = EventEnvelope(LEDGER_POSTED, payment.id, payment.correlation_id, {"payment_id": payment.id})
    queue_outbox_event(session, get_settings().kafka_topic, next_event.as_dict(), payment.id)
    await mark_processed(session, "ledger", event)


async def process_payment_event(session: AsyncSession, event: dict) -> None:
    if await already_processed(session, "payments", event):
        return
    payment = await session.get(Payment, event["payment_id"])
    if payment is None:
        return
    if event["event_type"] == FUNDS_RESERVED:
        await transition_payment(session, payment.id, PaymentState.CREATED, PaymentState.FUNDS_RESERVED, payment.correlation_id, event)
    elif event["event_type"] == FRAUD_APPROVED:
        await transition_payment(session, payment.id, PaymentState.FUNDS_RESERVED, PaymentState.FRAUD_APPROVED, payment.correlation_id, event)
    elif event["event_type"] == FRAUD_REJECTED:
        await transition_payment(session, payment.id, PaymentState.FUNDS_RESERVED, PaymentState.FRAUD_REJECTED, payment.correlation_id, event, failure_reason="fraud rejected")
        release_event = EventEnvelope(RELEASE_FUNDS_REQUESTED, payment.id, payment.correlation_id, {"payment_id": payment.id})
        queue_outbox_event(session, get_settings().kafka_topic, release_event.as_dict(), payment.id)
    elif event["event_type"] == LEDGER_POSTED:
        await transition_payment(session, payment.id, PaymentState.FRAUD_APPROVED, PaymentState.LEDGER_POSTED, payment.correlation_id, event)
        await transition_payment(session, payment.id, PaymentState.LEDGER_POSTED, PaymentState.COMPLETED, payment.correlation_id, event)
        completed = EventEnvelope(PAYMENT_COMPLETED, payment.id, payment.correlation_id, {"payment_id": payment.id})
        queue_outbox_event(session, get_settings().kafka_topic, completed.as_dict(), payment.id)
    elif event["event_type"] == FUNDS_RELEASED:
        await transition_payment(session, payment.id, PaymentState.FRAUD_REJECTED, PaymentState.FUNDS_RELEASED, payment.correlation_id, event, failure_reason="fraud rejected")
        await transition_payment(session, payment.id, PaymentState.FUNDS_RELEASED, PaymentState.FAILED, payment.correlation_id, event, failure_reason="fraud rejected")
        failed = EventEnvelope(PAYMENT_FAILED, payment.id, payment.correlation_id, {"payment_id": payment.id})
        queue_outbox_event(session, get_settings().kafka_topic, failed.as_dict(), payment.id)
    await mark_processed(session, "payments", event)


async def process_notification_event(session: AsyncSession, event: dict) -> None:
    if await already_processed(session, "notifications", event):
        return
    if event["event_type"] in {PAYMENT_COMPLETED, PAYMENT_FAILED}:
        await add_audit(session, "notification", event["payment_id"], event["event_type"], event["correlation_id"], None, None, event)
    await mark_processed(session, "notifications", event)
