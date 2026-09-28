# Failure Scenarios

- `metadata.force_fraud_reject=true`: drives the compensation path through `FRAUD_REJECTED`, `FUNDS_RELEASED`, and `FAILED`
- `metadata.force_ledger_failure=true`: forces ledger retries and ultimately routes the event to the DLQ
- duplicate delivery of the same Kafka event ID: ignored after the first successful processing per service
- repeated payment POST with the same `Idempotency-Key`: returns the original payment instead of creating a second one
