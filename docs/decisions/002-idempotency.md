# 002 - Idempotency

Payment creation uses a durable `idempotency_key` unique constraint. Consumer-side deduplication stores processed event IDs per service, which is necessary because Kafka delivery is treated as at-least-once.
