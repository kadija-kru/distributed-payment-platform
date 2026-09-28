# Event Flow

1. Gateway stores a new `payment.created` event in the outbox when it persists a payment in `CREATED`.
2. The payments service publishes the outbox row to Kafka.
3. Accounts consumes `payment.created`, reserves funds, and emits `payment.funds_reserved`.
4. Payments updates the payment to `FUNDS_RESERVED`.
5. Payments first emits `payment.fraud_check_requested` after recording `FUNDS_RESERVED`; fraud then emits either `payment.fraud_approved` or `payment.fraud_rejected`.
6. Payments emits `payment.ledger_post_requested` after recording `FRAUD_APPROVED`; ledger then emits `payment.ledger_posted` after moving money.
7. Payments finalizes the lifecycle to `COMPLETED`, or issues compensation (`payment.release_funds_requested`) after a fraud rejection.
8. Notifications records terminal events for auditability.
