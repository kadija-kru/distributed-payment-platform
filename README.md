# distributed-payment-platform

A production-style distributed payment platform scaffold built with **FastAPI**, **PostgreSQL**, **Redis**, **Kafka**, and **Docker Compose**. The project demonstrates backend engineering topics that go beyond CRUD: saga orchestration, idempotent payment processing, transactional outbox publishing, at-least-once event handling, retry and dead-letter behavior, optimistic concurrency, auditability, and failure recovery.

## Engineering story

- **Event-driven microservices**: gateway, payments, accounts, fraud, ledger, and notifications services
- **Saga orchestration**: the payment service advances the lifecycle and triggers compensation when fraud rejects a payment
- **Idempotent processing**: `Idempotency-Key` prevents duplicate payment creation
- **Transactional outbox**: state changes and emitted events are committed atomically
- **At-least-once Kafka delivery handling**: per-service duplicate-event protection through processed-event records
- **Distributed failure recovery**: retries, dead-letter queue handling, and compensating release of reserved funds
- **PostgreSQL persistence**: authoritative state for accounts, payments, ledger entries, audits, and event tracking
- **Redis caching**: cached account balance reads and rate-limiting counters
- **JWT authentication**: demo bearer tokens for local development
- **Observability**: structured logging, correlation IDs, health checks, metrics, and audit trail endpoints
- **Automated integration testing**: happy path, compensation path, duplicate-event protection, and DLQ simulation

## Architecture

```text
Client
  |
  v
API Gateway (FastAPI)
  |  writes payment + outbox in one DB transaction
  v
PostgreSQL <-------------------------------+
  |                                        |
  +--> Outbox Publisher --> Kafka topic ---+
                                 |     |     |     |
                                 v     v     v     v
                             Accounts Fraud Ledger Notifications
                                 |        |      |
                                 +--------+------+----> Payment Saga Orchestrator
                                                  |
                                                  v
                                                Redis
                                 (cache + rate limiting counters)
```

## Payment lifecycle

Happy path:

`CREATED -> FUNDS_RESERVED -> FRAUD_APPROVED -> LEDGER_POSTED -> COMPLETED`

Compensation path:

`CREATED -> FUNDS_RESERVED -> FRAUD_REJECTED -> FUNDS_RELEASED -> FAILED`

## Local run

```bash
cp .env.example .env
make up
```

Gateway: `http://localhost:8000`

Demo credentials:

- username: `demo`
- password: `demo-password`

Example flow:

```bash
TOKEN=$(curl -s http://localhost:8000/auth/token           -H 'content-type: application/json'           -d '{"username":"demo","password":"demo-password"}' | python -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
```

Create two accounts, then create a payment with `Idempotency-Key` and optional failure simulation flags such as `force_fraud_reject` or `force_ledger_failure` in the `metadata` field.

## Project layout

- `gateway/` public API service
- `services/` background worker services
- `shared/` database, security, logging, events, and domain logic
- `docs/` architecture, API, event flow, failure scenarios, and ADRs
- `infrastructure/` service-specific infrastructure notes
- `tests/` contract, integration, and failure-focused tests

## Tests

```bash
make install
make test
```

The test suite runs against SQLite and in-memory Redis/Kafka substitutes for speed, while Docker Compose wires PostgreSQL, Redis, and Kafka for local end-to-end execution.
