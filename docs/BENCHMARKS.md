# Benchmarks

## Recorded run

| Workers | Enqueue throughput | p50 | p95 | p99 | Environment |
| ---: | ---: | ---: | ---: | ---: | --- |
| 2 | 2,091.20 jobs/sec | 13.49 ms | 29.02 ms | 45.34 ms | Apple M4, 16 GB, Docker Desktop |

The k6 workload uses 32 virtual users for 20 seconds against `POST /v1/jobs`. The chaos workload enqueues 10,000 idempotent jobs and kills workers every five seconds for 60 seconds. The recorded chaos run completed all 10,000 with zero dead-letter rows.

Run `make bench` and `make chaos` against a clean local stack. Record hardware, Docker resources, PostgreSQL settings, payload size, worker count, and commit SHA with every published result. Do not compare results across different environments without those controls.
