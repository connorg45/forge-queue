package grpcsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/connorg45/forge-queue/internal/api"
	"github.com/connorg45/forge-queue/internal/config"
	"github.com/connorg45/forge-queue/internal/metrics"
	"github.com/connorg45/forge-queue/internal/queue"
	"github.com/connorg45/forge-queue/proto/forgepb"
)

type Server struct {
	forgepb.UnimplementedForgeServer
	queue  *queue.Queue
	stats  *metrics.Recorder
	events *api.RedisEvents
}

func New(q *queue.Queue, stats *metrics.Recorder, events *api.RedisEvents) *Server {
	return &Server{queue: q, stats: stats, events: events}
}

func (s *Server) SubmitJob(ctx context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	fields := req.AsMap()
	tenantID := config.DefaultTenantID
	if raw, ok := fields["tenant_id"].(string); ok && raw != "" {
		parsed, err := uuid.Parse(raw)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid tenant_id")
		}
		tenantID = parsed
	}
	payload := json.RawMessage(`{}`)
	if raw, ok := fields["payload"]; ok {
		encoded, err := json.Marshal(raw)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid payload")
		}
		payload = encoded
	}
	params := queue.EnqueueParams{
		TenantID:       tenantID,
		Queue:          stringField(fields, "queue"),
		Handler:        stringField(fields, "handler"),
		Payload:        payload,
		Priority:       int16(numberField(fields, "priority")),
		MaxAttempts:    numberField(fields, "max_attempts"),
		IdempotencyKey: stringField(fields, "idempotency_key"),
	}
	job, _, latency, err := s.queue.Enqueue(ctx, params)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if s.stats != nil {
		s.stats.RecordEnqueue(ctx, job.Queue, tenantID.String(), latency)
	}
	if s.events != nil {
		_ = s.events.Publish(ctx, api.Event{Type: "job.enqueued", Payload: job})
	}
	return toStruct(job)
}

func (s *Server) GetJob(ctx context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	id, err := idField(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	job, err := s.queue.GetJob(ctx, id)
	if err != nil {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	return toStruct(job)
}

func (s *Server) CancelJob(ctx context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	id, err := idField(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	job, err := s.queue.Cancel(ctx, id)
	if err != nil {
		if errors.Is(err, queue.ErrInvalidTransition) {
			return nil, status.Error(codes.FailedPrecondition, "job is not cancellable")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toStruct(job)
}

func (s *Server) GetQueueStats(ctx context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	queueName := stringField(req.AsMap(), "queue")
	if queueName == "" {
		queueName = "default"
	}
	depth, err := s.queue.Depth(ctx, queueName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toStruct(s.stats.Snapshot(ctx, queueName, depth))
}

func (s *Server) ListDLQ(ctx context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	limit := numberField(req.AsMap(), "limit")
	jobs, err := s.queue.ListDLQ(ctx, limit)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toStruct(map[string]any{"jobs": jobs})
}

func (s *Server) RequeueDLQ(ctx context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	id, err := idField(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	job, err := s.queue.RequeueDLQ(ctx, id)
	if err != nil {
		if errors.Is(err, queue.ErrInvalidTransition) {
			return nil, status.Error(codes.FailedPrecondition, "job is not requeueable")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toStruct(job)
}

func stringField(fields map[string]any, key string) string {
	value, _ := fields[key].(string)
	return value
}

func numberField(fields map[string]any, key string) int {
	value, ok := fields[key]
	if !ok {
		return 0
	}
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	default:
		return 0
	}
}

func idField(req *structpb.Struct) (uuid.UUID, error) {
	raw := stringField(req.AsMap(), "id")
	if raw == "" {
		raw = stringField(req.AsMap(), "job_id")
	}
	if raw == "" {
		return uuid.Nil, fmt.Errorf("missing id")
	}
	return uuid.Parse(raw)
}

func toStruct(value any) (*structpb.Struct, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	normalizeTimes(decoded)
	out, err := structpb.NewStruct(decoded)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return out, nil
}

func normalizeTimes(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if t, ok := child.(time.Time); ok {
				typed[key] = t.Format(time.RFC3339Nano)
				continue
			}
			normalizeTimes(child)
		}
	case []any:
		for _, child := range typed {
			normalizeTimes(child)
		}
	}
}
