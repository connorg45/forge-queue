package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"

	"github.com/connorg45/forge-queue/internal/config"
	"github.com/connorg45/forge-queue/internal/queue"
	"github.com/connorg45/forge-queue/internal/testutil"
)

func TestNextAndIdempotencyKey(t *testing.T) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	from := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	next, err := Next("*/5 * * * *", from, parser)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 5, 7, 12, 5, 0, 0, time.UTC), next)
	id := uuid.MustParse("00000000-0000-0000-0000-000000000010")
	require.Equal(t, "schedule:00000000-0000-0000-0000-000000000010:2026-05-07T12:00:00Z", IdempotencyKey(id, from))
}

func TestSchedulerTickEnqueuesDueSchedule(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := queue.New(db)
	s := New(db, q, nil)
	scheduleID := uuid.New()
	_, err := db.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, 'default') ON CONFLICT DO NOTHING`, config.DefaultTenantID)
	require.NoError(t, err)
	_, err = db.Exec(ctx, `
		INSERT INTO schedules (id, tenant_id, name, cron_expr, payload, queue, handler, next_run_at)
		VALUES ($1, $2, 'every minute', '* * * * *', '{"from":"schedule"}', 'default', 'echo', $3)
	`, scheduleID, config.DefaultTenantID, time.Now().Add(-time.Minute))
	require.NoError(t, err)
	require.NoError(t, s.Tick(ctx, time.Now()))
	var jobs int
	require.NoError(t, db.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE idempotency_key LIKE $1`, "schedule:"+scheduleID.String()+":%").Scan(&jobs))
	require.Equal(t, 1, jobs)
	var nextRun time.Time
	require.NoError(t, db.QueryRow(ctx, `SELECT next_run_at FROM schedules WHERE id = $1`, scheduleID).Scan(&nextRun))
	require.True(t, nextRun.After(time.Now().Add(-2*time.Minute)))
}

func TestSchedulerInitializesMissingNextRunAndInvalidCron(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := queue.New(db)
	s := New(db, q, nil)
	_, err := db.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, 'default') ON CONFLICT DO NOTHING`, config.DefaultTenantID)
	require.NoError(t, err)
	_, err = db.Exec(ctx, `
		INSERT INTO schedules (id, tenant_id, name, cron_expr, payload, queue, handler)
		VALUES ($1, $2, 'missing next', '*/10 * * * *', '{}', 'default', 'echo')
	`, uuid.New(), config.DefaultTenantID)
	require.NoError(t, err)
	require.NoError(t, s.Tick(ctx, time.Now()))
	var populated int
	require.NoError(t, db.QueryRow(ctx, `SELECT count(*) FROM schedules WHERE next_run_at IS NOT NULL`).Scan(&populated))
	require.Equal(t, 1, populated)

	_, err = Next("not cron", time.Now(), cron.NewParser(cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow|cron.Descriptor))
	require.Error(t, err)
}

func TestRunStopsOnContext(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := queue.New(db)
	s := New(db, q, nil)
	runCtx, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, s.Run(runCtx), context.Canceled)
}
