package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"

	"github.com/connorg45/forge-queue/internal/queue"
)

const advisoryLockKey int64 = 0xF076E

type Scheduler struct {
	db     *pgxpool.Pool
	queue  *queue.Queue
	parser cron.Parser
	logger *slog.Logger
}

type Schedule struct {
	ID        uuid.UUID       `json:"id"`
	TenantID  uuid.UUID       `json:"tenant_id"`
	Name      string          `json:"name"`
	CronExpr  string          `json:"cron_expr"`
	Payload   json.RawMessage `json:"payload"`
	Queue     string          `json:"queue"`
	Handler   string          `json:"handler"`
	NextRunAt time.Time       `json:"next_run_at"`
	Enabled   bool            `json:"enabled"`
}

func New(db *pgxpool.Pool, q *queue.Queue, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{
		db:     db,
		queue:  q,
		parser: cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		logger: logger,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Tick(ctx, time.Now()); err != nil {
				s.logger.Warn("scheduler tick failed", "error", err)
			}
		}
	}
}

func (s *Scheduler) Tick(ctx context.Context, now time.Time) error {
	conn, err := s.db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	ok, err := tryLock(ctx, conn)
	if err != nil || !ok {
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := unlock(unlockCtx, conn); err != nil {
			s.logger.Error("scheduler advisory unlock failed; closing session", "error", err)
			_ = conn.Hijack().Close(context.Background())
		}
	}()
	if err := s.initializeNextRuns(ctx, now); err != nil {
		return err
	}
	rows, err := s.db.Query(ctx, `
		SELECT id, tenant_id, COALESCE(name, ''), cron_expr, COALESCE(payload, '{}'::jsonb),
		       COALESCE(queue, 'default'), handler, next_run_at
		FROM schedules
		WHERE enabled = true
		  AND next_run_at <= $1
		ORDER BY next_run_at ASC
	`, now)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var schedule Schedule
		if err := rows.Scan(&schedule.ID, &schedule.TenantID, &schedule.Name, &schedule.CronExpr, &schedule.Payload, &schedule.Queue, &schedule.Handler, &schedule.NextRunAt); err != nil {
			return err
		}
		if err := s.fire(ctx, schedule); err != nil {
			s.logger.Warn("schedule fire failed", "schedule_id", schedule.ID, "error", err)
		}
	}
	return rows.Err()
}

func (s *Scheduler) fire(ctx context.Context, schedule Schedule) error {
	next, err := Next(schedule.CronExpr, schedule.NextRunAt, s.parser)
	if err != nil {
		return err
	}
	key := IdempotencyKey(schedule.ID, schedule.NextRunAt)
	_, _, _, err = s.queue.Enqueue(ctx, queue.EnqueueParams{
		TenantID:       schedule.TenantID,
		Queue:          schedule.Queue,
		Handler:        schedule.Handler,
		Payload:        schedule.Payload,
		IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, `
		UPDATE schedules
		SET last_run_at = $2,
		    next_run_at = $3
		WHERE id = $1
	`, schedule.ID, schedule.NextRunAt, next)
	return err
}

func (s *Scheduler) initializeNextRuns(ctx context.Context, now time.Time) error {
	rows, err := s.db.Query(ctx, `
		SELECT id, cron_expr
		FROM schedules
		WHERE enabled = true
		  AND next_run_at IS NULL
	`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type missing struct {
		id   uuid.UUID
		expr string
	}
	var schedules []missing
	for rows.Next() {
		var schedule missing
		if err := rows.Scan(&schedule.id, &schedule.expr); err != nil {
			return err
		}
		schedules = append(schedules, schedule)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, schedule := range schedules {
		next, err := Next(schedule.expr, now, s.parser)
		if err != nil {
			return err
		}
		if _, err := s.db.Exec(ctx, `UPDATE schedules SET next_run_at = $2 WHERE id = $1`, schedule.id, next); err != nil {
			return err
		}
	}
	return nil
}

func Next(expr string, from time.Time, parser cron.Parser) (time.Time, error) {
	if parser == (cron.Parser{}) {
		parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	}
	schedule, err := parser.Parse(expr)
	if err != nil {
		return time.Time{}, err
	}
	return schedule.Next(from), nil
}

func IdempotencyKey(scheduleID uuid.UUID, runAt time.Time) string {
	return fmt.Sprintf("schedule:%s:%s", scheduleID, runAt.UTC().Format(time.RFC3339))
}

type advisoryLockConn interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func tryLock(ctx context.Context, db advisoryLockConn) (bool, error) {
	var ok bool
	err := db.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, advisoryLockKey).Scan(&ok)
	return ok, err
}

func unlock(ctx context.Context, db advisoryLockConn) error {
	_, err := db.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryLockKey)
	return err
}
