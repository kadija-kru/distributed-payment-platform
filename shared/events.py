from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

PAYMENT_CREATED = "payment.created"
FUNDS_RESERVED = "payment.funds_reserved"
FRAUD_APPROVED = "payment.fraud_approved"
FRAUD_REJECTED = "payment.fraud_rejected"
RELEASE_FUNDS_REQUESTED = "payment.release_funds_requested"
FUNDS_RELEASED = "payment.funds_released"
LEDGER_POSTED = "payment.ledger_posted"
PAYMENT_COMPLETED = "payment.completed"
PAYMENT_FAILED = "payment.failed"


@dataclass(slots=True)
class EventEnvelope:
    event_type: str
    payment_id: str
    correlation_id: str
    payload: dict[str, Any]
    event_id: str = field(default_factory=lambda: str(uuid4()))
    attempt: int = 0
    occurred_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "payment_id": self.payment_id,
            "correlation_id": self.correlation_id,
            "attempt": self.attempt,
            "occurred_at": self.occurred_at,
            "payload": self.payload,
        }
