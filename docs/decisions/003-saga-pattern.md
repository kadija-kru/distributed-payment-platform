# 003 - Saga pattern

The payment lifecycle is orchestrated through state transitions and events rather than a single database transaction spanning multiple services. Fraud rejection triggers a compensating command that releases reserved funds before marking the payment as failed.
