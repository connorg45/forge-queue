CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE tenants (
  id uuid PRIMARY KEY,
  name text UNIQUE,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE jobs (
  id uuid PRIMARY KEY,
  tenant_id uuid REFERENCES tenants,
  queue text NOT NULL,
  handler text NOT NULL,
  payload jsonb NOT NULL,
  status text CHECK (status in ('ready','running','succeeded','failed','dead')) NOT NULL DEFAULT 'ready',
  priority smallint NOT NULL DEFAULT 5,
  attempts int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL DEFAULT 5,
  run_at timestamptz NOT NULL DEFAULT now(),
  locked_by text,
  locked_until timestamptz,
  idempotency_key text,
  last_error text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE (tenant_id, idempotency_key)
);

CREATE TABLE job_runs (
  id uuid PRIMARY KEY,
  job_id uuid REFERENCES jobs ON DELETE CASCADE,
  attempt int,
  started_at timestamptz,
  finished_at timestamptz,
  status text,
  error text
);

CREATE TABLE schedules (
  id uuid PRIMARY KEY,
  tenant_id uuid REFERENCES tenants,
  name text,
  cron_expr text,
  payload jsonb,
  queue text,
  handler text,
  enabled bool DEFAULT true,
  last_run_at timestamptz,
  next_run_at timestamptz
);

CREATE TABLE dead_letter (
  job_id uuid PRIMARY KEY REFERENCES jobs ON DELETE CASCADE,
  dead_at timestamptz DEFAULT now(),
  reason text
);

CREATE INDEX jobs_ready_idx ON jobs (queue, priority DESC, run_at)
  WHERE status = 'ready';
CREATE INDEX jobs_locked_idx ON jobs (locked_until)
  WHERE status = 'running';
CREATE INDEX jobs_recent_idx ON jobs (updated_at DESC);
CREATE INDEX schedules_due_idx ON schedules (next_run_at)
  WHERE enabled = true;
CREATE INDEX job_runs_active_idx ON job_runs (job_id, started_at DESC)
  WHERE status = 'running';

ALTER TABLE jobs ADD CONSTRAINT jobs_attempts_nonnegative CHECK (attempts >= 0);
ALTER TABLE jobs ADD CONSTRAINT jobs_max_attempts_positive CHECK (max_attempts > 0);
ALTER TABLE jobs ADD CONSTRAINT jobs_priority_range CHECK (priority BETWEEN 0 AND 32767);
ALTER TABLE schedules ADD CONSTRAINT schedules_cron_nonempty CHECK (length(trim(cron_expr)) > 0);
ALTER TABLE schedules ADD CONSTRAINT schedules_handler_nonempty CHECK (length(trim(handler)) > 0);
