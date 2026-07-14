package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/connorg45/forge-queue/internal/api"
	"github.com/connorg45/forge-queue/internal/config"
	"github.com/connorg45/forge-queue/internal/queue"
	"github.com/connorg45/forge-queue/internal/testutil"
)

func TestPoolProcessesJobs(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := queue.New(db)
	for i := 0; i < 40; i++ {
		_, _, _, err := q.Enqueue(ctx, queue.EnqueueParams{
			TenantID: config.DefaultTenantID,
			Queue:    "default",
			Handler:  "echo",
			Payload:  []byte(`{}`),
		})
		require.NoError(t, err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	pool := &Pool{
		Queue:         q,
		Queues:        []string{"default"},
		Registry:      DefaultRegistry(db),
		Concurrency:   4,
		WorkerID:      "test-worker",
		Lease:         time.Second,
		JobTimeout:    time.Second,
		PollInterval:  10 * time.Millisecond,
		ShutdownGrace: 2 * time.Second,
	}
	go func() { done <- pool.Run(runCtx) }()
	testutil.WaitFor(t, 10*time.Second, func() bool {
		counts, err := q.StateCounts(ctx, "default")
		return err == nil && counts.Succeeded == 40
	})
	cancel()
	require.NoError(t, <-done)
	runs, err := q.RunCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(40), runs)
}

func TestDefaultHandlers(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	registry := DefaultRegistry(db)
	job := queue.Job{Payload: []byte(`{"duration":"1ms","message":"custom"}`)}
	require.NoError(t, registry["echo"](ctx, job))
	require.NoError(t, registry["sleep"](ctx, job))
	require.EqualError(t, registry["fail"](ctx, job), "custom")
	_, err := db.Exec(ctx, `CREATE TABLE chaos_results (job_id uuid PRIMARY KEY, created_at timestamptz DEFAULT now())`)
	require.NoError(t, err)
	job.ID = config.DefaultTenantID
	job.Payload = []byte(`{"ms":1}`)
	require.NoError(t, registry["chaos"](ctx, job))
	require.NoError(t, registry["chaos"](ctx, job))
}

func TestPoolExecuteUnknownHandlerGoesToDLQ(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := queue.New(db)
	q.SetBaseBackoff(time.Millisecond)
	job, _, _, err := q.Enqueue(ctx, queue.EnqueueParams{
		TenantID:    config.DefaultTenantID,
		Queue:       "default",
		Handler:     "missing",
		Payload:     []byte(`{}`),
		MaxAttempts: 1,
	})
	require.NoError(t, err)
	var picked []queue.Job
	testutil.WaitFor(t, 2*time.Second, func() bool {
		picked, err = q.Dequeue(ctx, []string{"default"}, 1, "w", time.Second)
		return err == nil && len(picked) == 1
	})
	require.NoError(t, err)
	require.Equal(t, job.ID, picked[0].ID)
	pool := &Pool{Queue: q, Registry: Registry{}, WorkerID: "w", Lease: time.Second, JobTimeout: time.Second}
	pool.normalize()
	pool.execute(ctx, picked[0], pool.Logger)
	dlq, err := q.ListDLQ(ctx, 10)
	require.NoError(t, err)
	require.Len(t, dlq, 1)
}

func TestSleepStopsOnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sleep(ctx, time.Hour)
}

func TestRecordAndPublishNilSafe(t *testing.T) {
	p := &Pool{}
	p.publish(apiEventForTest())
	p.recordComplete("default", "succeeded", time.Millisecond)
}

func apiEventForTest() api.Event {
	return api.Event{Type: "test", Payload: map[string]string{"ok": "true"}}
}

func TestFailHandlerRetriesToDLQ(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := queue.New(db)
	q.SetBaseBackoff(time.Millisecond)
	_, _, _, err := q.Enqueue(ctx, queue.EnqueueParams{
		TenantID:    config.DefaultTenantID,
		Queue:       "default",
		Handler:     "fail",
		Payload:     []byte(`{"message":"nope"}`),
		MaxAttempts: 1,
	})
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	pool := &Pool{
		Queue:         q,
		Queues:        []string{"default"},
		Registry:      DefaultRegistry(db),
		Concurrency:   1,
		WorkerID:      "test-worker",
		Lease:         time.Second,
		JobTimeout:    time.Second,
		PollInterval:  10 * time.Millisecond,
		ShutdownGrace: time.Second,
	}
	go func() { done <- pool.Run(runCtx) }()
	testutil.WaitFor(t, 10*time.Second, func() bool {
		dlq, err := q.ListDLQ(ctx, 10)
		return err == nil && len(dlq) == 1
	})
	cancel()
	require.NoError(t, <-done)
}
