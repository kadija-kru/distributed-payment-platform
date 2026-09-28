# Architecture

The repository is structured as a pragmatic microservice-style monorepo. All services share the same domain package and persistence schema so the project stays runnable and easy to understand, while still demonstrating the operational concerns expected in a distributed payment workflow.

Core responsibilities:

- **Gateway**: authentication, rate limiting, public API, correlation IDs, metrics, health
- **Payments**: transactional outbox publisher and saga state transitions
- **Accounts**: reserve and release funds
- **Fraud**: approve or reject based on policy or simulation metadata
- **Ledger**: final double-entry posting for completed transfers
- **Notifications**: terminal-state event observation
