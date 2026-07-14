# Forge

[![CI](https://github.com/connorg45/forge-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/connorg45/forge-queue/actions/workflows/ci.yml)
[![CodeQL](https://github.com/connorg45/forge-queue/actions/workflows/codeql.yml/badge.svg)](https://github.com/connorg45/forge-queue/actions/workflows/codeql.yml)
[![Go](https://img.shields.io/badge/Go-1.26-00ADD8)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Forge is a self-hostable distributed job queue and cron scheduler built with Go, PostgreSQL, Redis, gRPC, React, and TypeScript. It demonstrates the systems work behind reliable background execution: transactional enqueue, concurrent leasing, retries with jitter, dead-letter recovery, recurring schedules, observability, and tested crash behavior.

Run the Docker stack for the API, workers, scheduler, PostgreSQL, Redis, Prometheus, Grafana, OpenTelemetry pipeline, and live control plane.

## Engineering highlights

- Durable PostgreSQL queue with `SELECT ... FOR UPDATE SKIP LOCKED` and timestamp leases.
- At-least-once delivery with tenant-scoped idempotency keys and idempotent handler guidance.
- Session-pinned PostgreSQL advisory locking for safe scheduler leadership.
- Lease expiry recovery that closes abandoned run history before retrying work.
- Capped exponential backoff with jitter, dead-letter inspection, and requeue without erasing attempt history.
- REST, gRPC, CLI, and server-sent-event interfaces plus a responsive React operations dashboard.
- Prometheus metrics, Grafana dashboards, structured logs, Redis-backed live stats, and OpenTelemetry traces.
- Race-enabled Go tests, TypeScript production builds, Docker builds, CodeQL, Dependabot, and migration drift checks in CI.

## Architecture

```mermaid
flowchart LR
  Clients[REST, gRPC, CLI] --> API[Go API]
  Dashboard[React control plane] -->|SSE and REST| API
  API --> Postgres[(PostgreSQL 16)]
  Scheduler[Scheduler replicas] -->|session advisory lock| Postgres
  Workers[Worker pool] -->|SKIP LOCKED leases| Postgres
  API <--> Redis[(Redis 7)]
  Workers <--> Redis
  API --> Prometheus[Prometheus]
  Prometheus --> Grafana[Grafana]
  API --> Tempo[OpenTelemetry / Tempo]
```

PostgreSQL is the correctness boundary. Redis accelerates pub/sub and recent metrics, but losing Redis cannot lose an acknowledged job. See [Architecture](docs/ARCHITECTURE.md) for state transitions and design tradeoffs.

## Quickstart

Requirements: Docker with Compose.

```bash
git clone https://github.com/connorg45/forge-queue.git
cd forge-queue
make demo
```

Open the dashboard at `http://localhost:5173`, the API at `http://localhost:8080`, and Grafana at `http://localhost:3000`.

```bash
make seed
curl http://localhost:8080/readyz
curl http://localhost:8080/v1/jobs?limit=10
```

## Interfaces

```bash
# Submit a job
go run ./cmd/forge-cli submit --handler echo --payload '{"message":"hello"}'

# Create and operate a recurring schedule
go run ./cmd/forge-cli schedule create --name heartbeat --cron '*/5 * * * *' --handler echo
go run ./cmd/forge-cli schedules
go run ./cmd/forge-cli schedule pause <schedule-id>

# Inspect and recover dead work
go run ./cmd/forge-cli dlq
go run ./cmd/forge-cli dlq requeue <job-id>
```

The full REST contract and examples are in [API](docs/API.md).

## Benchmark and failure testing

The recorded Apple M4, 16 GB Docker Desktop run sustained **2,091.20 enqueues/sec** across two workers with **45.34 ms p99 enqueue latency**. A separate chaos run completed **10,000 of 10,000 jobs** with **zero dead-letter rows** while workers were killed every five seconds for one minute.

These are reproducible development-machine measurements, not universal capacity claims. Workload, PostgreSQL configuration, network latency, and payload size materially affect results.

```bash
make bench
make chaos
make verify
```

See [Benchmarks](docs/BENCHMARKS.md) for methodology and [Operations](docs/OPERATIONS.md) for recovery expectations.

## Correctness model

Forge provides at-least-once execution. Enqueue idempotency is enforced by a unique `(tenant_id, idempotency_key)` constraint. A worker can perform a side effect and crash before acknowledgement, so handlers must make external effects idempotent. This is the honest boundary: no queue can prove exactly one external effect across an arbitrary crash without an idempotent record or transaction shared with that effect.

## Repository map

- `cmd/`: API, worker, scheduler, and CLI entry points.
- `internal/queue/`: transactional queue and state transitions.
- `internal/scheduler/`: cron evaluation, leadership, and schedule storage.
- `internal/worker/`: concurrency, heartbeat, shutdown, and handler registry.
- `internal/api/`, `internal/grpcsvc/`: public interfaces and live events.
- `web/`: React and TypeScript operations dashboard.
- `deploy/`: Docker Compose, images, Prometheus, Grafana, and Tempo.
- `test/integration/`: queue, worker, and chaos tests with Testcontainers.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [API](docs/API.md)
- [Operations](docs/OPERATIONS.md)
- [Benchmarks](docs/BENCHMARKS.md)
- [Contributing](CONTRIBUTING.md)
- [Security](SECURITY.md)

Licensed under the [MIT License](LICENSE).
