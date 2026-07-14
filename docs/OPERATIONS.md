# Operations

## Failure behavior

| Failure | Expected behavior | Recovery bound |
| --- | --- | --- |
| API exits during enqueue | Transaction commits the full job or nothing | Client retry and API restart |
| Worker exits during handler | Lease expires, abandoned run closes, job becomes eligible | Lease duration |
| Scheduler exits | PostgreSQL releases its session lock; another replica takes leadership | Next one-second tick |
| Redis is unavailable | Live events and rate limiting degrade; durable queue remains intact | Redis recovery |
| PostgreSQL restarts | Committed state survives; clients reconnect; leases expire normally | Database restart plus lease |

## Production checklist

- Put the API behind authenticated TLS termination.
- Store credentials in a secret manager and rotate them.
- Restrict PostgreSQL and Redis to private networks.
- Back up PostgreSQL and regularly test restores.
- Alert on queue depth, oldest-ready age, dead-letter growth, error rate, and lease expiry.
- Use idempotent handlers for all external side effects.
- Tune pool size, worker concurrency, lease, and timeout against representative workloads.
