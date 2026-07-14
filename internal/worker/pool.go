package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/connorg45/forge-queue/internal/api"
	"github.com/connorg45/forge-queue/internal/metrics"
	"github.com/connorg45/forge-queue/internal/queue"
)

type Pool struct {
	Queue         *queue.Queue
	Queues        []string
	Registry      Registry
	Concurrency   int
	WorkerID      string
	Lease         time.Duration
	JobTimeout    time.Duration
	PollInterval  time.Duration
	ShutdownGrace time.Duration
	Events        *api.RedisEvents
	Stats         *metrics.Recorder
	Logger        *slog.Logger
}

func (p *Pool) Run(ctx context.Context) error {
	p.normalize()
	var wg sync.WaitGroup
	for i := 0; i < p.Concurrency; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			p.loop(ctx, slot)
		}(i)
	}
	<-ctx.Done()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(p.ShutdownGrace):
		return fmt.Errorf("shutdown grace exceeded: %w", context.DeadlineExceeded)
	}
}

func (p *Pool) loop(ctx context.Context, slot int) {
	logger := p.Logger.With("worker_id", p.WorkerID, "slot", slot)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		jobs, err := p.Queue.Dequeue(ctx, p.Queues, 1, p.WorkerID, p.Lease)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Warn("dequeue failed", "error", err)
			}
			sleep(ctx, p.PollInterval)
			continue
		}
		if len(jobs) == 0 {
			sleep(ctx, p.PollInterval)
			continue
		}
		p.execute(ctx, jobs[0], logger)
	}
}

func (p *Pool) execute(parent context.Context, job queue.Job, logger *slog.Logger) {
	start := time.Now()
	logger = logger.With("job_id", job.ID, "tenant_id", job.TenantID, "queue", job.Queue, "handler", job.Handler)
	handler, ok := p.Registry[job.Handler]
	if !ok {
		handler = func(context.Context, queue.Job) error {
			return fmt.Errorf("unknown handler %q", job.Handler)
		}
	}
	p.publish(api.Event{Type: "job.started", Payload: job})
	jobCtx, cancelJob := context.WithTimeout(parent, p.JobTimeout)
	defer cancelJob()
	heartbeatCtx, cancelHeartbeat := context.WithCancel(jobCtx)
	defer cancelHeartbeat()
	go p.heartbeat(heartbeatCtx, job, logger)
	err := handler(jobCtx, job)
	cancelHeartbeat()
	duration := time.Since(start)
	if err != nil {
		opCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
		updated, dead, delay, nackErr := p.Queue.Nack(opCtx, job.ID, p.WorkerID, err)
		cancel()
		if nackErr != nil {
			logger.Error("nack failed", "error", nackErr)
			return
		}
		status := queue.StatusFailed
		if dead {
			status = queue.StatusDead
		}
		p.recordComplete(updated.Queue, status, duration)
		p.publish(api.Event{Type: "job.failed", Payload: map[string]any{
			"job":       updated,
			"dead":      dead,
			"retry_in":  delay.String(),
			"error":     err.Error(),
			"duration":  duration.String(),
			"worker_id": p.WorkerID,
		}})
		logger.Warn("job failed", "error", err, "dead", dead, "retry_in", delay)
		return
	}
	opCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	updated, err := p.Queue.Ack(opCtx, job.ID, p.WorkerID)
	cancel()
	if err != nil {
		logger.Error("ack failed", "error", err)
		return
	}
	p.recordComplete(updated.Queue, queue.StatusSucceeded, duration)
	p.publish(api.Event{Type: "job.succeeded", Payload: map[string]any{
		"job":       updated,
		"duration":  duration.String(),
		"worker_id": p.WorkerID,
	}})
	logger.Info("job succeeded", "duration", duration)
}

func (p *Pool) heartbeat(ctx context.Context, job queue.Job, logger *slog.Logger) {
	interval := p.Lease / 3
	if interval <= 0 || interval > 5*time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.Queue.ExtendLease(ctx, job.ID, p.WorkerID, p.Lease); err != nil {
				logger.Warn("lease heartbeat failed", "error", err)
				return
			}
		}
	}
}

func (p *Pool) normalize() {
	if p.Concurrency <= 0 {
		p.Concurrency = 4
	}
	if len(p.Queues) == 0 {
		p.Queues = []string{"default"}
	}
	if p.WorkerID == "" {
		p.WorkerID = "worker"
	}
	if p.Lease <= 0 {
		p.Lease = 10 * time.Second
	}
	if p.JobTimeout <= 0 {
		p.JobTimeout = 30 * time.Second
	}
	if p.PollInterval <= 0 {
		p.PollInterval = 100 * time.Millisecond
	}
	if p.ShutdownGrace <= 0 {
		p.ShutdownGrace = 20 * time.Second
	}
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	if p.Registry == nil {
		p.Registry = DefaultRegistry(p.Queue.DB())
	}
}

func sleep(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (p *Pool) publish(event api.Event) {
	if p.Events != nil {
		_ = p.Events.Publish(context.Background(), event)
	}
}

func (p *Pool) recordComplete(queueName, status string, duration time.Duration) {
	if p.Stats != nil {
		p.Stats.RecordComplete(context.Background(), queueName, status, duration)
	}
}
