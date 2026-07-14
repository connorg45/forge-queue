# Architecture

## Correctness boundary

PostgreSQL stores jobs, attempts, schedules, dead-letter metadata, and tenant-scoped idempotency keys. Redis is intentionally outside the correctness boundary and supports live events, rate limiting, and recent statistics.

## Job state machine

`ready -> running -> succeeded`

`running -> ready` after a failed attempt or expired lease while retry budget remains.

`running -> dead` when the final attempt fails. Operators may requeue a dead job; its lifetime attempt count is preserved and retry budget is extended by one.

Workers select eligible rows using `FOR UPDATE SKIP LOCKED`. Each claim records `locked_by`, `locked_until`, and an immutable attempt row. Heartbeats extend the timestamp lease. A later dequeue closes the abandoned run record before reclaiming an expired lease.

## Scheduler leadership

Each scheduler replica acquires a PostgreSQL session-level advisory lock on one pinned pool connection. Lock and unlock must use the same session. If unlock fails, Forge closes that connection so a stale leadership lock cannot return to the pool.

Each schedule occurrence receives an idempotency key derived from schedule ID and run timestamp. Multiple scheduler replicas can therefore recover safely without duplicate rows for the same occurrence.

## Delivery semantics

Forge provides at-least-once execution. A crash can happen after an external side effect and before acknowledgement. Production handlers should use an idempotency key, an outbox, or a transaction shared with the side-effect record.

## Scaling

API and worker processes are stateless and scale horizontally. Queue contention is reduced through `SKIP LOCKED`, targeted partial indexes, and short claim transactions. PostgreSQL capacity is the practical ceiling. Kafka or a log-oriented broker is a better fit when workloads need long retention, partition ordering, many replaying consumers, or sustained throughput far beyond a relational queue.
