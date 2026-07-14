# REST API

The API defaults to `http://localhost:8080`. JSON request bodies are limited to 1 MiB and reject unknown fields.

## Health

- `GET /healthz`: process liveness.
- `GET /readyz`: PostgreSQL readiness.
- `GET /metrics`: Prometheus metrics.

## Jobs

- `POST /v1/jobs`: submit a job. Fields include `tenant_id`, `queue`, `handler`, `payload`, `priority`, `max_attempts`, and `idempotency_key`.
- `GET /v1/jobs?limit=50`: recent jobs.
- `GET /v1/jobs/{id}`: one job.
- `POST /v1/jobs/{id}/cancel`: cancel ready work.
- `GET /v1/queues/{name}/stats`: queue statistics.

## Dead-letter queue

- `GET /v1/dlq?limit=100`: dead jobs with failure reason.
- `POST /v1/dlq/{id}/requeue`: return dead work to the ready queue while preserving lifetime attempts.

## Schedules

- `GET /v1/schedules`: list schedules.
- `POST /v1/schedules`: create a schedule with `name`, `cron_expr`, `handler`, `queue`, and `payload`.
- `POST /v1/schedules/{id}/pause`: disable future occurrences.
- `POST /v1/schedules/{id}/resume`: enable future occurrences.
- `DELETE /v1/schedules/{id}`: delete a schedule.

## Events

`GET /v1/events` opens a server-sent-event stream containing job lifecycle events and periodic statistics snapshots.
