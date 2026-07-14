#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f deploy/docker-compose.yml"
API_URL="${API_URL:-http://localhost:8080}"
TOTAL="${TOTAL:-10000}"
KILL_SECONDS="${KILL_SECONDS:-60}"

trap '$COMPOSE up -d --scale worker=2 worker >/dev/null 2>&1 || true' EXIT

$COMPOSE up -d --build postgres redis api
$COMPOSE stop worker >/dev/null 2>&1 || true

until curl -fsS "$API_URL/healthz" >/dev/null; do
  sleep 1
done

$COMPOSE exec -T postgres psql -U forge -d forge <<'SQL'
CREATE TABLE IF NOT EXISTS chaos_results (
  job_id uuid PRIMARY KEY,
  created_at timestamptz DEFAULT now()
);
TRUNCATE dead_letter, job_runs, jobs, chaos_results RESTART IDENTITY;
SQL

$COMPOSE up -d --scale worker=4 worker

kill_loop() {
  local end=$((SECONDS + KILL_SECONDS))
  while [ "$SECONDS" -lt "$end" ]; do
    workers="$($COMPOSE ps -q worker | sed '/^$/d')"
    count="$(printf '%s\n' "$workers" | sed '/^$/d' | wc -l | tr -d ' ')"
    if [ "${count:-0}" -gt 0 ]; then
      index=$((RANDOM % count + 1))
      victim="$(printf '%s\n' "$workers" | sed -n "${index}p")"
      docker kill --signal=KILL "$victim" >/dev/null 2>&1 || true
      $COMPOSE up -d --scale worker=4 worker >/dev/null 2>&1 || true
    fi
    sleep 5
  done
}

kill_loop &
killer=$!

seq 1 "$TOTAL" | xargs -P64 -I{} curl -fsS -o /dev/null -X POST "$API_URL/v1/jobs" \
  -H 'content-type: application/json' \
  -d "{\"queue\":\"default\",\"handler\":\"chaos\",\"payload\":{\"ms\":8},\"idempotency_key\":\"chaos-{}\"}"

wait "$killer"

deadline=$((SECONDS + 180))
while [ "$SECONDS" -lt "$deadline" ]; do
  line="$($COMPOSE exec -T postgres psql -U forge -d forge -Atc "SELECT (SELECT count(*) FROM jobs WHERE status='succeeded'), (SELECT count(*) FROM chaos_results), (SELECT count(*) FROM dead_letter)")"
  IFS='|' read -r succeeded results dead <<<"$line"
  if [ "${succeeded:-0}" = "$TOTAL" ] && [ "${results:-0}" = "$TOTAL" ] && [ "${dead:-0}" = "0" ]; then
    echo "chaos passed: submitted=$TOTAL succeeded=$succeeded side_effects=$results dead_letter=$dead"
    exit 0
  fi
  sleep 2
done

$COMPOSE exec -T postgres psql -U forge -d forge -c "
SELECT
  (SELECT count(*) FROM jobs WHERE status='succeeded') AS succeeded,
  (SELECT count(*) FROM chaos_results) AS side_effects,
  (SELECT count(*) FROM dead_letter) AS dead_letter,
  (SELECT count(*) FROM jobs WHERE status <> 'succeeded') AS unfinished;
"
echo "chaos failed"
exit 1
