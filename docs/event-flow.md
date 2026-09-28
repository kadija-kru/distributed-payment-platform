# Event Flow

1. Gateway stores a new `payment.created` event in the outbox when it persists a payment in `CREATED`.
2. The payments service publishes the outbox row to Kafka.
3. Accounts consumes `payment.created`, reserves funds, and emits `payment.funds_reserved`.
4. Payments updates the payment to `FUNDS_RESERVED`.
5. Fraud consumes `payment.funds_reserved` and emits either `payment.fraud_approved` or `payment.fraud_rejected`.
6. Ledger consumes approvals and emits `payment.ledger_posted` after moving money.
7. Payments finalizes the lifecycle to `COMPLETED`, or issues compensation (`payment.release_funds_requested`) after a fraud rejection.
8. Notifications records terminal events for auditability.
