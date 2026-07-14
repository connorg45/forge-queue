import http from 'k6/http';
import { check } from 'k6';

const apiURL = (__ENV.API_URL || 'http://localhost:8080').replace(/\/$/, '');
const vus = Number(__ENV.VUS || 32);
const duration = __ENV.DURATION || '20s';

export const options = {
  vus,
  duration,
  summaryTrendStats: ['avg', 'min', 'med', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    http_req_failed: ['rate<0.01']
  }
};

export default function () {
  const key = `${__VU}-${__ITER}-${Date.now()}`;
  const res = http.post(
    `${apiURL}/v1/jobs`,
    JSON.stringify({
      queue: 'default',
      handler: 'echo',
      payload: { source: 'bench' },
      idempotency_key: `bench-${key}`
    }),
    { headers: { 'content-type': 'application/json' } }
  );
  check(res, {
    'enqueue accepted': (r) => r.status === 200 || r.status === 201
  });
}

export function handleSummary(data) {
  const reqs = data.metrics.http_reqs?.values?.rate ?? 0;
  const latency = data.metrics.http_req_duration?.values ?? {};
  const p50 = latency['p(50)'] ?? latency.med ?? 0;
  const p95 = latency['p(95)'] ?? 0;
  const p99 = latency['p(99)'] ?? 0;
  return {
    stdout:
      `Forge enqueue benchmark\n` +
      `workers=${__ENV.WORKERS || '2'} vus=${vus} duration=${duration}\n` +
      `throughput=${reqs.toFixed(2)} jobs/sec\n` +
      `p50_enqueue_latency=${p50.toFixed(2)} ms\n` +
      `p95_enqueue_latency=${p95.toFixed(2)} ms\n` +
      `p99_enqueue_latency=${p99.toFixed(2)} ms\n`
  };
}
