#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"

for _ in $(seq 1 60); do
  if curl -fsS "$API_URL/readyz" >/dev/null; then
    break
  fi
  sleep 1
done
curl -fsS "$API_URL/readyz" >/dev/null

curl -fsS -X POST "$API_URL/v1/jobs" -H 'content-type: application/json' \
  -d '{"queue":"default","handler":"echo","payload":{"msg":"hello from seed"},"idempotency_key":"seed:echo"}'
curl -fsS -X POST "$API_URL/v1/jobs" -H 'content-type: application/json' \
  -d '{"queue":"default","handler":"sleep","payload":{"duration":"500ms"},"idempotency_key":"seed:sleep"}'
curl -fsS -X POST "$API_URL/v1/jobs" -H 'content-type: application/json' \
  -d '{"queue":"default","handler":"fail","payload":{"message":"seeded failure"},"max_attempts":1,"idempotency_key":"seed:failure"}'

if ! curl -fsS "$API_URL/v1/schedules" | grep -q 'seed-heartbeat'; then
  curl -fsS -X POST "$API_URL/v1/schedules" -H 'content-type: application/json' \
    -d '{"name":"seed-heartbeat","cron_expr":"*/5 * * * *","queue":"default","handler":"echo","payload":{"source":"schedule"}}'
fi
