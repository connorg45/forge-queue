package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/connorg45/forge-queue/internal/config"
	"github.com/connorg45/forge-queue/internal/queue"
	"github.com/connorg45/forge-queue/internal/testutil"
	"github.com/connorg45/forge-queue/internal/worker"
)

func TestEnqueueThousandJobsAndRunWorker(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := queue.New(db)
	for i := 0; i < 1000; i++ {
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
	pool := &worker.Pool{
		Queue:         q,
		Queues:        []string{"default"},
		Registry:      worker.DefaultRegistry(db),
		Concurrency:   16,
		WorkerID:      "integration-worker",
		Lease:         5 * time.Second,
		JobTimeout:    time.Second,
		PollInterval:  5 * time.Millisecond,
		ShutdownGrace: 2 * time.Second,
	}
	go func() { done <- pool.Run(runCtx) }()
	testutil.WaitFor(t, 30*time.Second, func() bool {
		counts, err := q.StateCounts(ctx, "default")
		return err == nil && counts.Succeeded == 1000
	})
	cancel()
	require.NoError(t, <-done)
	var mismatches int
	err := db.QueryRow(ctx, `
		SELECT count(*)
		FROM jobs j
		WHERE j.attempts <> (
			SELECT count(*)
			FROM job_runs r
			WHERE r.job_id = j.id
		)
	`).Scan(&mismatches)
	require.NoError(t, err)
	require.Zero(t, mismatches)
}
