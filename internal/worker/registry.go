package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/connorg45/forge-queue/internal/queue"
)

type Handler func(context.Context, queue.Job) error

type Registry map[string]Handler

func DefaultRegistry(db *pgxpool.Pool) Registry {
	return Registry{
		"echo":  echoHandler,
		"sleep": sleepHandler,
		"fail":  failHandler,
		"chaos": chaosHandler(db),
	}
}

func echoHandler(ctx context.Context, job queue.Job) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func sleepHandler(ctx context.Context, job queue.Job) error {
	duration := 100 * time.Millisecond
	var payload struct {
		Duration string `json:"duration"`
		MS       int    `json:"ms"`
	}
	if err := json.Unmarshal(job.Payload, &payload); err == nil {
		if payload.Duration != "" {
			if parsed, err := time.ParseDuration(payload.Duration); err == nil {
				duration = parsed
			}
		}
		if payload.MS > 0 {
			duration = time.Duration(payload.MS) * time.Millisecond
		}
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func failHandler(ctx context.Context, job queue.Job) error {
	var payload struct {
		Message string `json:"message"`
	}
	_ = json.Unmarshal(job.Payload, &payload)
	if payload.Message == "" {
		payload.Message = "intentional failure"
	}
	return errors.New(payload.Message)
}

func chaosHandler(db *pgxpool.Pool) Handler {
	return func(ctx context.Context, job queue.Job) error {
		if db == nil {
			return fmt.Errorf("chaos handler requires database")
		}
		var payload struct {
			Duration string `json:"duration"`
			MS       int    `json:"ms"`
		}
		_ = json.Unmarshal(job.Payload, &payload)
		duration := 8 * time.Millisecond
		if payload.Duration != "" {
			if parsed, err := time.ParseDuration(payload.Duration); err == nil {
				duration = parsed
			}
		}
		if payload.MS > 0 {
			duration = time.Duration(payload.MS) * time.Millisecond
		}
		if _, err := db.Exec(ctx, `
			INSERT INTO chaos_results (job_id)
			VALUES ($1)
			ON CONFLICT (job_id) DO NOTHING
		`, job.ID); err != nil {
			return err
		}
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
}
