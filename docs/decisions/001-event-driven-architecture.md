# 001 - Event-driven architecture

We use Kafka-backed asynchronous events to decouple fund reservation, fraud checks, ledger posting, and notifications. This keeps the project focused on distributed systems concerns instead of synchronous CRUD request chaining.
