package queue

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/connorg45/forge-queue/internal/config"
	"github.com/connorg45/forge-queue/internal/testutil"
)

func TestBackoffDelay(t *testing.T) {
	base := time.Second
	tests := []struct {
		name     string
		attempts int
		want     time.Duration
	}{
		{name: "first retry", attempts: 0, want: time.Second},
		{name: "third attempt", attempts: 3, want: 8 * time.Second},
		{name: "capped", attempts: 20, want: 10 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, BackoffDelay(base, tt.attempts, nil))
		})
	}
	withJitter := BackoffDelay(base, 2, rand.New(rand.NewSource(1)))
	require.GreaterOrEqual(t, withJitter, 4*time.Second)
	require.Less(t, withJitter, 5*time.Second)
}

func TestIdempotencyDedup(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := New(db)
	first, created, _, err := q.Enqueue(ctx, EnqueueParams{
		TenantID:       config.DefaultTenantID,
		Queue:          "default",
		Handler:        "echo",
		Payload:        []byte(`{"n":1}`),
		IdempotencyKey: "same-key",
	})
	require.NoError(t, err)
	require.True(t, created)
	second, created, _, err := q.Enqueue(ctx, EnqueueParams{
		TenantID:       config.DefaultTenantID,
		Queue:          "default",
		Handler:        "echo",
		Payload:        []byte(`{"n":2}`),
		IdempotencyKey: "same-key",
	})
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, first.ID, second.ID)
	require.JSONEq(t, string(first.Payload), string(second.Payload))
}

func TestStateMachineTransitions(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := New(db)
	q.SetBaseBackoff(time.Millisecond)
	job, _, _, err := q.Enqueue(ctx, EnqueueParams{
		TenantID:    config.DefaultTenantID,
		Queue:       "default",
		Handler:     "fail",
		Payload:     []byte(`{}`),
		MaxAttempts: 1,
	})
	require.NoError(t, err)
	require.Equal(t, StatusReady, job.Status)

	picked := dequeueEventually(t, ctx, q, "w1", time.Second)
	require.Equal(t, StatusRunning, picked[0].Status)
	require.Equal(t, 1, picked[0].Attempts)

	dead, isDead, _, err := q.Nack(ctx, picked[0].ID, "w1", errors.New("boom"))
	require.NoError(t, err)
	require.True(t, isDead)
	require.Equal(t, StatusDead, dead.Status)
	dlq, err := q.ListDLQ(ctx, 10)
	require.NoError(t, err)
	require.Len(t, dlq, 1)
	require.Equal(t, picked[0].ID, dlq[0].Job.ID)

	requeued, err := q.RequeueDLQ(ctx, picked[0].ID)
	require.NoError(t, err)
	require.Equal(t, StatusReady, requeued.Status)
	require.Equal(t, 0, requeued.Attempts)
	picked, err = q.Dequeue(ctx, []string{"default"}, 1, "w1", time.Second)
	require.NoError(t, err)
	require.Len(t, picked, 1)
	acked, err := q.Ack(ctx, picked[0].ID, "w1")
	require.NoError(t, err)
	require.Equal(t, StatusSucceeded, acked.Status)
	_, err = q.Cancel(ctx, picked[0].ID)
	require.ErrorIs(t, err, ErrInvalidTransition)
}

func TestRetryCancelStatsAndRecentJobs(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := New(db)
	q.SetBaseBackoff(time.Millisecond)
	job, _, _, err := q.Enqueue(ctx, EnqueueParams{
		TenantID:    config.DefaultTenantID,
		Queue:       "default",
		Handler:     "fail",
		Payload:     []byte(`{}`),
		MaxAttempts: 2,
	})
	require.NoError(t, err)
	depth, err := q.Depth(ctx, "default")
	require.NoError(t, err)
	require.Equal(t, int64(1), depth)
	recent, err := q.RecentJobs(ctx, 10)
	require.NoError(t, err)
	require.NotEmpty(t, recent)

	picked := dequeueEventually(t, ctx, q, "w1", time.Second)
	require.NoError(t, q.ExtendLease(ctx, picked[0].ID, "w1", time.Second))
	_, isDead, delay, err := q.Nack(ctx, picked[0].ID, "w1", errors.New("retry"))
	require.NoError(t, err)
	require.False(t, isDead)
	require.Positive(t, delay)
	counts, err := q.StateCounts(ctx, "default")
	require.NoError(t, err)
	require.Equal(t, int64(1), counts.Ready)

	err = q.ExtendLease(ctx, picked[0].ID, "w1", time.Second)
	require.ErrorIs(t, err, ErrInvalidTransition)
	_, err = q.GetJob(ctx, uuid.New())
	require.Error(t, err)

	cancelJob, _, _, err := q.Enqueue(ctx, EnqueueParams{
		TenantID: config.DefaultTenantID,
		Queue:    "default",
		Handler:  "echo",
		Payload:  []byte(`{}`),
	})
	require.NoError(t, err)
	cancelled, err := q.Cancel(ctx, cancelJob.ID)
	require.NoError(t, err)
	require.Equal(t, StatusFailed, cancelled.Status)
	require.NotEqual(t, job.ID, cancelJob.ID)
}

func TestExpiredLeaseIsReclaimed(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := New(db)
	job, _, _, err := q.Enqueue(ctx, EnqueueParams{
		TenantID: config.DefaultTenantID,
		Queue:    "default",
		Handler:  "echo",
		Payload:  []byte(`{}`),
	})
	require.NoError(t, err)
	picked := dequeueEventually(t, ctx, q, "dead-worker", time.Millisecond)
	require.Equal(t, job.ID, picked[0].ID)
	testutil.WaitFor(t, 2*time.Second, func() bool {
		picked, err = q.Dequeue(ctx, []string{"default"}, 1, "new-worker", time.Second)
		return err == nil && len(picked) == 1
	})
	require.Equal(t, job.ID, picked[0].ID)
	require.Equal(t, 2, picked[0].Attempts)
}

func TestMultipleNullIdempotencyKeysAllowed(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := New(db)
	var ids []uuid.UUID
	for i := 0; i < 3; i++ {
		job, created, _, err := q.Enqueue(ctx, EnqueueParams{TenantID: config.DefaultTenantID, Queue: "default", Handler: "echo", Payload: []byte(`{}`)})
		require.NoError(t, err)
		require.True(t, created)
		ids = append(ids, job.ID)
	}
	require.NotEqual(t, ids[0], ids[1])
	require.NotEqual(t, ids[1], ids[2])
}

func TestContextCancellation(t *testing.T) {
	ctx, db, _ := testutil.Postgres(t)
	q := New(db)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err := q.Dequeue(cancelled, []string{"default"}, 1, "w1", time.Second)
	require.Error(t, err)
}

func dequeueEventually(t *testing.T, ctx context.Context, q *Queue, workerID string, lease time.Duration) []Job {
	t.Helper()
	var picked []Job
	var err error
	testutil.WaitFor(t, 2*time.Second, func() bool {
		picked, err = q.Dequeue(ctx, []string{"default"}, 1, workerID, lease)
		return err == nil && len(picked) == 1
	})
	require.NoError(t, err)
	return picked
}
