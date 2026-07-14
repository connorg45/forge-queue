import React, { lazy, Suspense, useEffect, useMemo, useState } from 'react';
import ReactDOM from 'react-dom/client';
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Clock3,
  Database,
  RefreshCcw,
  Server,
  Zap
} from 'lucide-react';
import type { Point } from './Chart';
import './styles.css';

const Chart = lazy(() => import('./Chart'));

const API_URL = (import.meta.env.VITE_API_URL || '').replace(/\/$/, '');

type Job = {
  id: string;
  tenant_id: string;
  queue: string;
  handler: string;
  status: string;
  attempts: number;
  max_attempts: number;
  run_at: string;
  updated_at: string;
  last_error?: string;
};

type DeadJob = {
  job: Job;
  dead_at: string;
  reason: string;
};

type Stats = {
  queue: string;
  depth: number;
  throughput_per_sec: number;
  total_processed: number;
  p50_enqueue_latency_ms: number;
  p95_enqueue_latency_ms: number;
  p99_enqueue_latency_ms: number;
  sampled_at: string;
};

type TickPayload = {
  stats: Stats;
  jobs: Job[];
  dlq: DeadJob[];
};

type Schedule = { id: string; name: string; cron_expr: string; queue: string; handler: string; next_run_at: string; enabled: boolean };

function App() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [points, setPoints] = useState<Point[]>([]);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [dlq, setDLQ] = useState<DeadJob[]>([]);
  const [filter, setFilter] = useState('');
  const [connected, setConnected] = useState(false);
  const [schedules, setSchedules] = useState<Schedule[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    Promise.all([
      fetch(`${API_URL}/v1/jobs?limit=50`).then((response) => response.ok ? response.json() : Promise.reject(new Error('jobs unavailable'))),
      fetch(`${API_URL}/v1/dlq`).then((response) => response.ok ? response.json() : Promise.reject(new Error('DLQ unavailable'))),
      fetch(`${API_URL}/v1/schedules`).then((response) => response.ok ? response.json() : Promise.reject(new Error('schedules unavailable')))
    ]).then(([initialJobs, initialDLQ, initialSchedules]) => {
      setJobs(initialJobs); setDLQ(initialDLQ); setSchedules(initialSchedules);
    }).catch((reason) => setError(reason instanceof Error ? reason.message : 'API unavailable'));
    const source = new EventSource(`${API_URL}/v1/events`);
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(false);
    source.onmessage = (message) => {
      let event;
      try { event = JSON.parse(message.data); } catch { return; }
      if (event.type !== 'stats.tick') return;
      const payload = event.payload as TickPayload;
      setStats(payload.stats);
      setJobs(payload.jobs ?? []);
      setDLQ(payload.dlq ?? []);
      const label = new Date(payload.stats.sampled_at).toLocaleTimeString([], {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
      });
      setPoints((current) =>
        [
          ...current,
          {
            time: label,
            depth: payload.stats.depth,
            p50: payload.stats.p50_enqueue_latency_ms,
            p95: payload.stats.p95_enqueue_latency_ms,
            p99: payload.stats.p99_enqueue_latency_ms,
            throughput: payload.stats.throughput_per_sec
          }
        ].slice(-300)
      );
    };
    return () => source.close();
  }, []);

  const filteredDLQ = useMemo(() => {
    const needle = filter.toLowerCase().trim();
    if (!needle) return dlq;
    return dlq.filter((item) =>
      [item.job.id, item.job.handler, item.reason, item.job.queue]
        .join(' ')
        .toLowerCase()
        .includes(needle)
    );
  }, [dlq, filter]);

  async function requeue(id: string) {
    const response = await fetch(`${API_URL}/v1/dlq/${id}/requeue`, { method: 'POST' });
    if (!response.ok) setError('Requeue failed');
    else setDLQ((current) => current.filter((item) => item.job.id !== id));
  }

  async function toggleSchedule(item: Schedule) {
    const action = item.enabled ? 'pause' : 'resume';
    const response = await fetch(`${API_URL}/v1/schedules/${item.id}/${action}`, { method: 'POST' });
    if (!response.ok) { setError(`Could not ${action} schedule`); return; }
    const updated = await response.json();
    setSchedules((current) => current.map((schedule) => schedule.id === item.id ? updated : schedule));
  }

  return (
    <main className="min-h-screen bg-[#101214] text-zinc-100">
      <header className="border-b border-white/10 bg-[#171a1d]">
        <div className="mx-auto flex max-w-7xl flex-col gap-5 px-5 py-5 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <div className="flex items-center gap-3">
              <div className="grid h-9 w-9 place-items-center rounded-md bg-emerald-400 text-zinc-950">
                <Zap size={19} />
              </div>
              <div>
                <h1 className="text-xl font-semibold tracking-normal">Forge</h1>
                <p className="text-sm text-zinc-400">Distributed queue control plane</p>
              </div>
            </div>
          </div>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <KPI icon={<Activity size={16} />} label="Throughput" value={`${(stats?.throughput_per_sec ?? 0).toFixed(1)}/s`} />
            <KPI icon={<CheckCircle2 size={16} />} label="Processed" value={format(stats?.total_processed ?? 0)} />
            <KPI icon={<Database size={16} />} label="Depth" value={format(stats?.depth ?? 0)} />
            <KPI icon={<Server size={16} />} label="SSE" value={connected ? 'live' : 'reconnect'} tone={connected ? 'good' : 'warn'} />
          </div>
        </div>
      </header>

      <section className="mx-auto grid max-w-7xl gap-5 px-5 py-6 xl:grid-cols-[1.2fr_0.8fr]">
        {error && <div className="xl:col-span-2 rounded-md border border-rose-400/30 bg-rose-400/10 px-4 py-3 text-sm text-rose-200">{error}</div>}
        <div className="grid gap-5">
          <Panel title="Queue depth" subtitle="Ready jobs, sampled from Postgres every second">
            <Suspense fallback={<ChartFallback />}><Chart data={points} lines={[{ key: 'depth', color: '#34d399', name: 'depth' }]} /></Suspense>
          </Panel>
          <Panel title="Enqueue latency" subtitle="p50, p95, and p99 from recent enqueue writes">
            <Suspense fallback={<ChartFallback />}><Chart
              data={points}
              lines={[
                { key: 'p50', color: '#60a5fa', name: 'p50' },
                { key: 'p95', color: '#fbbf24', name: 'p95' },
                { key: 'p99', color: '#fb7185', name: 'p99' }
              ]}
            /></Suspense>
          </Panel>
        </div>

        <div className="grid gap-5">
          <Panel title="Active jobs" subtitle="Last 50 jobs by update time">
            <div className="max-h-[390px] overflow-auto">
              <table className="w-full min-w-[560px] text-left text-sm">
                <thead className="sticky top-0 bg-[#171a1d] text-xs uppercase text-zinc-500">
                  <tr>
                    <th className="py-2 pr-3">Job</th>
                    <th className="py-2 pr-3">Handler</th>
                    <th className="py-2 pr-3">Attempts</th>
                    <th className="py-2">Status</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-white/5">
                  {jobs.map((job) => (
                    <tr key={job.id} className="text-zinc-300">
                      <td className="py-2 pr-3 font-mono text-xs text-zinc-400">{short(job.id)}</td>
                      <td className="py-2 pr-3">{job.handler}</td>
                      <td className="py-2 pr-3">{job.attempts}/{job.max_attempts}</td>
                      <td className="py-2"><Badge status={job.status} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Panel>

          <Panel title="Dead letter queue" subtitle="Inspect failed jobs and put them back on the ready queue">
            <div className="mb-3 flex items-center gap-2 rounded-md border border-white/10 bg-black/20 px-3 py-2">
              <AlertTriangle size={16} className="text-amber-300" />
              <input
                className="w-full bg-transparent text-sm text-zinc-100 outline-none placeholder:text-zinc-600"
                placeholder="Filter DLQ"
                value={filter}
                onChange={(event) => setFilter(event.target.value)}
              />
            </div>
            <div className="max-h-[330px] overflow-auto">
              <table className="w-full min-w-[600px] text-left text-sm">
                <thead className="sticky top-0 bg-[#171a1d] text-xs uppercase text-zinc-500">
                  <tr>
                    <th className="py-2 pr-3">Job</th>
                    <th className="py-2 pr-3">Reason</th>
                    <th className="py-2 pr-3">Dead at</th>
                    <th className="py-2"></th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-white/5">
                  {filteredDLQ.map((item) => (
                    <tr key={item.job.id} className="text-zinc-300">
                      <td className="py-2 pr-3 font-mono text-xs text-zinc-400">{short(item.job.id)}</td>
                      <td className="py-2 pr-3 text-rose-200">{item.reason}</td>
                      <td className="py-2 pr-3 text-zinc-500">{new Date(item.dead_at).toLocaleTimeString()}</td>
                      <td className="py-2 text-right">
                        <button
                          className="inline-flex h-8 items-center gap-2 rounded-md bg-emerald-400 px-3 text-xs font-medium text-zinc-950 transition hover:bg-emerald-300"
                          onClick={() => requeue(item.job.id)}
                        >
                          <RefreshCcw size={14} />
                          Requeue
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {filteredDLQ.length === 0 && (
                <div className="grid h-24 place-items-center text-sm text-zinc-500">No dead jobs</div>
              )}
            </div>
          </Panel>
          <Panel title="Schedules" subtitle="Pause or resume recurring jobs without a redeploy">
            <div className="grid gap-2">
              {schedules.map((item) => (
                <div key={item.id} className="flex items-center justify-between rounded-md border border-white/10 bg-black/20 px-3 py-2">
                  <div><div className="text-sm text-zinc-200">{item.name}</div><div className="font-mono text-xs text-zinc-500">{item.cron_expr} · {item.handler}</div></div>
                  <button className="rounded-md border border-white/10 px-3 py-1 text-xs hover:bg-white/5" onClick={() => toggleSchedule(item)}>{item.enabled ? 'Pause' : 'Resume'}</button>
                </div>
              ))}
              {schedules.length === 0 && <div className="py-6 text-center text-sm text-zinc-500">No schedules configured</div>}
            </div>
          </Panel>
        </div>
      </section>
    </main>
  );
}

function KPI({ icon, label, value, tone = 'neutral' }: { icon: React.ReactNode; label: string; value: string; tone?: 'neutral' | 'good' | 'warn' }) {
  const toneClass = tone === 'good' ? 'text-emerald-300' : tone === 'warn' ? 'text-amber-300' : 'text-zinc-100';
  return (
    <div className="min-w-[130px] rounded-md border border-white/10 bg-black/20 px-3 py-2">
      <div className="flex items-center gap-2 text-xs uppercase text-zinc-500">{icon}{label}</div>
      <div className={`mt-1 text-lg font-semibold ${toneClass}`}>{value}</div>
    </div>
  );
}

function Panel({ title, subtitle, children }: { title: string; subtitle: string; children: React.ReactNode }) {
  return (
    <section className="rounded-md border border-white/10 bg-[#171a1d] p-4">
      <div className="mb-4 flex items-start justify-between gap-4">
        <div>
          <h2 className="text-base font-semibold">{title}</h2>
          <p className="mt-1 text-sm text-zinc-500">{subtitle}</p>
        </div>
        <Clock3 size={16} className="mt-1 text-zinc-600" />
      </div>
      {children}
    </section>
  );
}

function ChartFallback() { return <div className="h-64 animate-pulse rounded-md bg-black/20" />; }

function Badge({ status }: { status: string }) {
  const classes: Record<string, string> = {
    ready: 'bg-sky-400/10 text-sky-300',
    running: 'bg-amber-400/10 text-amber-300',
    succeeded: 'bg-emerald-400/10 text-emerald-300',
    failed: 'bg-rose-400/10 text-rose-300',
    dead: 'bg-red-500/10 text-red-300'
  };
  return <span className={`rounded-md px-2 py-1 text-xs ${classes[status] ?? 'bg-zinc-400/10 text-zinc-300'}`}>{status}</span>;
}

function short(id: string) {
  return `${id.slice(0, 8)}...${id.slice(-4)}`;
}

function format(value: number) {
  return new Intl.NumberFormat().format(value);
}

ReactDOM.createRoot(document.getElementById('root')!).render(<App />);
