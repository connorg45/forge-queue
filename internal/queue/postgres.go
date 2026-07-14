package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	StatusReady     = "ready"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
	StatusDead      = "dead"
)

var ErrInvalidTransition = errors.New("invalid job state transition")

type Queue struct {
	db          *pgxpool.Pool
	baseBackoff time.Duration
	rngMu       sync.Mutex
	rng         *rand.Rand
}

type Job struct {
	ID             uuid.UUID       `json:"id"`
	TenantID       uuid.UUID       `json:"tenant_id"`
	Queue          string          `json:"queue"`
	Handler        string          `json:"handler"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Priority       int16           `json:"priority"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          time.Time       `json:"run_at"`
	LockedBy       *string         `json:"locked_by,omitempty"`
	LockedUntil    *time.Time      `json:"locked_until,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type DeadJob struct {
	Job    Job       `json:"job"`
	DeadAt time.Time `json:"dead_at"`
	Reason string    `json:"reason"`
}

type EnqueueParams struct {
	TenantID       uuid.UUID
	Queue          string
	Handler        string
	Payload        json.RawMessage
	Priority       int16
	MaxAttempts    int
	RunAt          time.Time
	IdempotencyKey string
}

type StateCounts struct {
	Ready     int64 `json:"ready"`
	Running   int64 `json:"running"`
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
	Dead      int64 `json:"dead"`
}

func New(db *pgxpool.Pool) *Queue {
	return &Queue{
		db:          db,
		baseBackoff: time.Second,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (q *Queue) DB() *pgxpool.Pool {
	return q.db
}

func (q *Queue) SetBaseBackoff(base time.Duration) {
	q.baseBackoff = base
}

func (q *Queue) Enqueue(ctx context.Context, params EnqueueParams) (Job, bool, time.Duration, error) {
	start := time.Now()
	params = normalizeEnqueueParams(params)
	if err := ensureTenant(ctx, q.db, params.TenantID); err != nil {
		return Job{}, false, 0, err
	}

	var row pgx.Row
	if params.IdempotencyKey == "" {
		row = q.db.QueryRow(ctx, `
			INSERT INTO jobs (id, tenant_id, queue, handler, payload, priority, max_attempts, run_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			RETURNING id, tenant_id, queue, handler, payload, status, priority, attempts, max_attempts, run_at,
				locked_by, locked_until, idempotency_key, last_error, created_at, updated_at
		`, uuid.New(), params.TenantID, params.Queue, params.Handler, params.Payload, params.Priority, params.MaxAttempts, params.RunAt)
		job, err := scanJob(row)
		return job, true, time.Since(start), err
	}

	var created bool
	row = q.db.QueryRow(ctx, `
		INSERT INTO jobs (id, tenant_id, queue, handler, payload, priority, max_attempts, run_at, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (tenant_id, idempotency_key) DO UPDATE
		SET idempotency_key = jobs.idempotency_key
		RETURNING id, tenant_id, queue, handler, payload, status, priority, attempts, max_attempts, run_at,
			locked_by, locked_until, idempotency_key, last_error, created_at, updated_at, (xmax = 0) AS created
	`, uuid.New(), params.TenantID, params.Queue, params.Handler, params.Payload, params.Priority, params.MaxAttempts, params.RunAt, params.IdempotencyKey)
	job, err := scanJobWithCreated(row, &created)
	return job, created, time.Since(start), err
}

func (q *Queue) Dequeue(ctx context.Context, queues []string, limit int, workerID string, lease time.Duration) ([]Job, error) {
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	if limit <= 0 {
		limit = 1
	}
	tx, err := q.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer rollback(ctx, tx)

	if _, err := tx.Exec(ctx, `
		UPDATE job_runs
		SET finished_at = now(),
		    status = 'failed',
		    error = 'worker lease expired before acknowledgement'
		WHERE status = 'running'
		  AND job_id IN (
		    SELECT id FROM jobs
		    WHERE status = 'running' AND locked_until < now()
		  )
	`); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status = 'ready',
		    locked_by = NULL,
		    locked_until = NULL,
		    last_error = 'worker lease expired before acknowledgement',
		    updated_at = now()
		WHERE status = 'running'
		  AND locked_until < now()
	`); err != nil {
		return nil, err
	}

	// SKIP LOCKED lets many workers compete on the same durable jobs table without
	// blocking behind the first transaction that touches a row; LISTEN/NOTIFY is
	// useful as a wakeup hint, but it cannot be the correctness mechanism because
	// notifications are lossy and carry no durable ordering. The lease is stored
	// as locked_until instead of being tied to a database session so a worker can
	// crash, lose its TCP connection, or be SIGKILLed and the job will become
	// eligible again when the timestamp expires. If a worker dies after this pick
	// and before Ack, the next Dequeue first reclaims the expired lease and the job
	// is retried; handlers must therefore be idempotent.
	rows, err := tx.Query(ctx, `
		WITH picked AS (
		  SELECT id FROM jobs
		  WHERE status = 'ready'
		    AND run_at <= now()
		    AND queue = ANY($1)
		  ORDER BY priority DESC, run_at ASC
		  FOR UPDATE SKIP LOCKED
		  LIMIT $2
		)
		UPDATE jobs j
		SET status      = 'running',
		    locked_by   = $3,
		    locked_until= now() + $4::interval,
		    attempts    = attempts + 1,
		    updated_at  = now()
		FROM picked
		WHERE j.id = picked.id
		RETURNING j.id, j.tenant_id, j.queue, j.handler, j.payload, j.status, j.priority, j.attempts, j.max_attempts, j.run_at,
			j.locked_by, j.locked_until, j.idempotency_key, j.last_error, j.created_at, j.updated_at
	`, queues, limit, workerID, intervalLiteral(lease))
	if err != nil {
		return nil, err
	}
	jobs, err := scanJobs(rows)
	if err != nil {
		return nil, err
	}
	for _, job := range jobs {
		_, err := tx.Exec(ctx, `
			INSERT INTO job_runs (id, job_id, attempt, started_at, status)
			VALUES ($1, $2, $3, now(), 'running')
		`, uuid.New(), job.ID, job.Attempts)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (q *Queue) Ack(ctx context.Context, jobID uuid.UUID, workerID string) (Job, error) {
	tx, err := q.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, err
	}
	defer rollback(ctx, tx)
	row := tx.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'succeeded',
		    locked_by = NULL,
		    locked_until = NULL,
		    updated_at = now()
		WHERE id = $1
		  AND status = 'running'
		  AND ($2 = '' OR locked_by = $2)
		RETURNING id, tenant_id, queue, handler, payload, status, priority, attempts, max_attempts, run_at,
			locked_by, locked_until, idempotency_key, last_error, created_at, updated_at
	`, jobID, workerID)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrInvalidTransition
		}
		return Job{}, err
	}
	if _, err := tx.Exec(ctx, finishRunSQL(), jobID, "succeeded", ""); err != nil {
		return Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return job, nil
}

func (q *Queue) Nack(ctx context.Context, jobID uuid.UUID, workerID string, cause error) (Job, bool, time.Duration, error) {
	tx, err := q.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, false, 0, err
	}
	defer rollback(ctx, tx)

	row := tx.QueryRow(ctx, `
		SELECT id, tenant_id, queue, handler, payload, status, priority, attempts, max_attempts, run_at,
			locked_by, locked_until, idempotency_key, last_error, created_at, updated_at
		FROM jobs
		WHERE id = $1
		  AND status = 'running'
		  AND ($2 = '' OR locked_by = $2)
		FOR UPDATE
	`, jobID, workerID)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, false, 0, ErrInvalidTransition
		}
		return Job{}, false, 0, err
	}
	message := errorString(cause)
	dead := job.Attempts >= job.MaxAttempts
	if dead {
		row = tx.QueryRow(ctx, `
			UPDATE jobs
			SET status = 'dead',
			    locked_by = NULL,
			    locked_until = NULL,
			    last_error = $2,
			    updated_at = now()
			WHERE id = $1
			RETURNING id, tenant_id, queue, handler, payload, status, priority, attempts, max_attempts, run_at,
				locked_by, locked_until, idempotency_key, last_error, created_at, updated_at
		`, jobID, message)
		job, err = scanJob(row)
		if err != nil {
			return Job{}, false, 0, err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dead_letter (job_id, reason)
			VALUES ($1, $2)
			ON CONFLICT (job_id) DO UPDATE
			SET dead_at = now(), reason = EXCLUDED.reason
		`, jobID, message); err != nil {
			return Job{}, false, 0, err
		}
		if _, err := tx.Exec(ctx, finishRunSQL(), jobID, "dead", message); err != nil {
			return Job{}, false, 0, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Job{}, false, 0, err
		}
		return job, true, 0, nil
	}

	delay := q.backoff(job.Attempts)
	row = tx.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'ready',
		    locked_by = NULL,
		    locked_until = NULL,
		    run_at = now() + $2::interval,
		    last_error = $3,
		    updated_at = now()
		WHERE id = $1
		RETURNING id, tenant_id, queue, handler, payload, status, priority, attempts, max_attempts, run_at,
			locked_by, locked_until, idempotency_key, last_error, created_at, updated_at
	`, jobID, intervalLiteral(delay), message)
	job, err = scanJob(row)
	if err != nil {
		return Job{}, false, 0, err
	}
	if _, err := tx.Exec(ctx, finishRunSQL(), jobID, "failed", message); err != nil {
		return Job{}, false, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, 0, err
	}
	return job, false, delay, nil
}

func (q *Queue) ExtendLease(ctx context.Context, jobID uuid.UUID, workerID string, lease time.Duration) error {
	tag, err := q.db.Exec(ctx, `
		UPDATE jobs
		SET locked_until = now() + $3::interval,
		    updated_at = now()
		WHERE id = $1
		  AND status = 'running'
		  AND locked_by = $2
	`, jobID, workerID, intervalLiteral(lease))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrInvalidTransition
	}
	return nil
}

func (q *Queue) Cancel(ctx context.Context, jobID uuid.UUID) (Job, error) {
	row := q.db.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'failed',
		    last_error = 'cancelled',
		    updated_at = now()
		WHERE id = $1
		  AND status = 'ready'
		RETURNING id, tenant_id, queue, handler, payload, status, priority, attempts, max_attempts, run_at,
			locked_by, locked_until, idempotency_key, last_error, created_at, updated_at
	`, jobID)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrInvalidTransition
		}
		return Job{}, err
	}
	return job, nil
}

func (q *Queue) RequeueDLQ(ctx context.Context, jobID uuid.UUID) (Job, error) {
	tx, err := q.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, err
	}
	defer rollback(ctx, tx)
	row := tx.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'ready',
		    attempts = 0,
		    locked_by = NULL,
		    locked_until = NULL,
		    run_at = now(),
		    last_error = NULL,
		    updated_at = now()
		WHERE id = $1
		  AND status = 'dead'
		RETURNING id, tenant_id, queue, handler, payload, status, priority, attempts, max_attempts, run_at,
			locked_by, locked_until, idempotency_key, last_error, created_at, updated_at
	`, jobID)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrInvalidTransition
		}
		return Job{}, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM dead_letter WHERE job_id = $1`, jobID); err != nil {
		return Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return job, nil
}

func (q *Queue) GetJob(ctx context.Context, jobID uuid.UUID) (Job, error) {
	row := q.db.QueryRow(ctx, `
		SELECT id, tenant_id, queue, handler, payload, status, priority, attempts, max_attempts, run_at,
			locked_by, locked_until, idempotency_key, last_error, created_at, updated_at
		FROM jobs
		WHERE id = $1
	`, jobID)
	return scanJob(row)
}

func (q *Queue) ListDLQ(ctx context.Context, limit int) ([]DeadJob, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := q.db.Query(ctx, `
		SELECT j.id, j.tenant_id, j.queue, j.handler, j.payload, j.status, j.priority, j.attempts, j.max_attempts, j.run_at,
			j.locked_by, j.locked_until, j.idempotency_key, j.last_error, j.created_at, j.updated_at,
			d.dead_at, COALESCE(d.reason, '')
		FROM dead_letter d
		JOIN jobs j ON j.id = d.job_id
		ORDER BY d.dead_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeadJob
	for rows.Next() {
		var job Job
		var deadAt time.Time
		var reason string
		if err := scanJobFields(rows, &job, &deadAt, &reason); err != nil {
			return nil, err
		}
		out = append(out, DeadJob{Job: job, DeadAt: deadAt, Reason: reason})
	}
	return out, rows.Err()
}

func (q *Queue) RecentJobs(ctx context.Context, limit int) ([]Job, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := q.db.Query(ctx, `
		SELECT id, tenant_id, queue, handler, payload, status, priority, attempts, max_attempts, run_at,
			locked_by, locked_until, idempotency_key, last_error, created_at, updated_at
		FROM jobs
		ORDER BY updated_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	return scanJobs(rows)
}

func (q *Queue) Depth(ctx context.Context, queueName string) (int64, error) {
	var depth int64
	err := q.db.QueryRow(ctx, `
		SELECT count(*)
		FROM jobs
		WHERE queue = $1
		  AND status = 'ready'
	`, queueName).Scan(&depth)
	return depth, err
}

func (q *Queue) StateCounts(ctx context.Context, queueName string) (StateCounts, error) {
	rows, err := q.db.Query(ctx, `
		SELECT status, count(*)
		FROM jobs
		WHERE queue = $1
		GROUP BY status
	`, queueName)
	if err != nil {
		return StateCounts{}, err
	}
	defer rows.Close()
	var counts StateCounts
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return StateCounts{}, err
		}
		switch status {
		case StatusReady:
			counts.Ready = count
		case StatusRunning:
			counts.Running = count
		case StatusSucceeded:
			counts.Succeeded = count
		case StatusFailed:
			counts.Failed = count
		case StatusDead:
			counts.Dead = count
		}
	}
	return counts, rows.Err()
}

func (q *Queue) RunCount(ctx context.Context) (int64, error) {
	var count int64
	err := q.db.QueryRow(ctx, `SELECT count(*) FROM job_runs`).Scan(&count)
	return count, err
}

func (q *Queue) backoff(attempts int) time.Duration {
	q.rngMu.Lock()
	defer q.rngMu.Unlock()
	return BackoffDelay(q.baseBackoff, attempts, q.rng)
}

// BackoffDelay implements capped exponential backoff:
// delay = base * 2^attempts + jitter, where jitter = rand(0, base). The jitter
// spreads retries from a burst of failed jobs over a small window so they do not
// stampede Postgres or a recovering downstream service at the same instant.
func BackoffDelay(base time.Duration, attempts int, rng *rand.Rand) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if attempts < 0 {
		attempts = 0
	}
	pow := math.Pow(2, float64(attempts))
	delay := time.Duration(float64(base) * pow)
	if rng != nil {
		delay += time.Duration(rng.Int63n(int64(base)))
	}
	const capDelay = 10 * time.Minute
	if delay > capDelay {
		return capDelay
	}
	return delay
}

func normalizeEnqueueParams(params EnqueueParams) EnqueueParams {
	if params.TenantID == uuid.Nil {
		params.TenantID = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	}
	if params.Queue == "" {
		params.Queue = "default"
	}
	if len(params.Payload) == 0 {
		params.Payload = json.RawMessage(`{}`)
	}
	if params.Priority == 0 {
		params.Priority = 5
	}
	if params.MaxAttempts <= 0 {
		params.MaxAttempts = 5
	}
	if params.RunAt.IsZero() {
		params.RunAt = time.Now()
	}
	return params
}

func ensureTenant(ctx context.Context, db *pgxpool.Pool, tenantID uuid.UUID) error {
	name := "default"
	if tenantID != uuid.MustParse("00000000-0000-0000-0000-000000000001") {
		name = tenantID.String()
	}
	_, err := db.Exec(ctx, `
		INSERT INTO tenants (id, name)
		VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING
	`, tenantID, name)
	return err
}

func intervalLiteral(duration time.Duration) string {
	if duration <= 0 {
		duration = time.Second
	}
	return fmt.Sprintf("%f seconds", duration.Seconds())
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func finishRunSQL() string {
	return `
		UPDATE job_runs
		SET finished_at = now(),
		    status = $2,
		    error = NULLIF($3, '')
		WHERE id = (
			SELECT id
			FROM job_runs
			WHERE job_id = $1
			  AND status = 'running'
			ORDER BY started_at DESC
			LIMIT 1
		)
	`
}

func rollback(ctx context.Context, tx pgx.Tx) {
	_ = tx.Rollback(ctx)
}

func scanJob(row pgx.Row) (Job, error) {
	var job Job
	err := scanJobFields(row, &job)
	return job, err
}

func scanJobWithCreated(row pgx.Row, created *bool) (Job, error) {
	var job Job
	err := scanJobFields(row, &job, created)
	return job, err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJobFields(row rowScanner, job *Job, extra ...any) error {
	var payload []byte
	var lockedBy sql.NullString
	var lockedUntil sql.NullTime
	var idem sql.NullString
	var lastErr sql.NullString
	dest := []any{
		&job.ID,
		&job.TenantID,
		&job.Queue,
		&job.Handler,
		&payload,
		&job.Status,
		&job.Priority,
		&job.Attempts,
		&job.MaxAttempts,
		&job.RunAt,
		&lockedBy,
		&lockedUntil,
		&idem,
		&lastErr,
		&job.CreatedAt,
		&job.UpdatedAt,
	}
	dest = append(dest, extra...)
	if err := row.Scan(dest...); err != nil {
		return err
	}
	job.Payload = append(job.Payload[:0], payload...)
	if lockedBy.Valid {
		job.LockedBy = &lockedBy.String
	}
	if lockedUntil.Valid {
		job.LockedUntil = &lockedUntil.Time
	}
	if idem.Valid {
		job.IdempotencyKey = &idem.String
	}
	if lastErr.Valid {
		job.LastError = &lastErr.String
	}
	return nil
}

func scanJobs(rows pgx.Rows) ([]Job, error) {
	defer rows.Close()
	jobs := make([]Job, 0)
	for rows.Next() {
		var job Job
		if err := scanJobFields(rows, &job); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}
