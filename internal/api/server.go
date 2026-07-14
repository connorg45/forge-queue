package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	"github.com/connorg45/forge-queue/internal/config"
	"github.com/connorg45/forge-queue/internal/metrics"
	"github.com/connorg45/forge-queue/internal/queue"
	"github.com/connorg45/forge-queue/internal/ratelimit"
	"github.com/connorg45/forge-queue/internal/scheduler"
)

type Server struct {
	queue      *queue.Queue
	hub        *Hub
	events     *RedisEvents
	stats      *metrics.Recorder
	limiter    *ratelimit.Limiter
	logger     *slog.Logger
	defaultQ   string
	tenantName string
	schedules  *scheduler.Store
}

func NewServer(q *queue.Queue, hub *Hub, events *RedisEvents, stats *metrics.Recorder, limiter *ratelimit.Limiter, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		queue:      q,
		hub:        hub,
		events:     events,
		stats:      stats,
		limiter:    limiter,
		logger:     logger,
		defaultQ:   "default",
		tenantName: "default",
		schedules:  scheduler.NewStore(q.DB()),
	}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(s.cors)
	r.Use(middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.queue.DB().Ping(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "database unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	r.Handle("/metrics", metrics.Handler())
	r.Route("/v1", func(r chi.Router) {
		r.Post("/jobs", s.submitJob)
		r.Get("/jobs", s.listJobs)
		r.Get("/jobs/{id}", s.getJob)
		r.Post("/jobs/{id}/cancel", s.cancelJob)
		r.Get("/queues/{name}/stats", s.queueStats)
		r.Get("/dlq", s.listDLQ)
		r.Post("/dlq/{id}/requeue", s.requeueDLQ)
		r.Get("/events", s.eventsHandler)
		r.Get("/schedules", s.listSchedules)
		r.Post("/schedules", s.createSchedule)
		r.Post("/schedules/{id}/pause", s.pauseSchedule)
		r.Post("/schedules/{id}/resume", s.resumeSchedule)
		r.Delete("/schedules/{id}", s.deleteSchedule)
	})
	return r
}

func (s *Server) StartStatsTicks(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			depth, err := s.queue.Depth(ctx, s.defaultQ)
			if err != nil {
				s.logger.Warn("stats depth failed", "error", err)
				continue
			}
			snap := s.stats.Snapshot(ctx, s.defaultQ, depth)
			counts, _ := s.queue.StateCounts(ctx, s.defaultQ)
			active, _ := s.queue.RecentJobs(ctx, 50)
			dlq, _ := s.queue.ListDLQ(ctx, 100)
			s.hub.Publish(Event{Type: "stats.tick", Payload: map[string]any{
				"stats":  snap,
				"counts": counts,
				"jobs":   active,
				"dlq":    dlq,
			}})
		}
	}
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) {
	var req submitJobRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if req.Handler == "" || len(req.Handler) > 128 || len(req.Queue) > 128 || req.Priority < 0 || req.Priority > 32767 || req.MaxAttempts < 0 || req.MaxAttempts > 100 {
		writeError(w, http.StatusBadRequest, "invalid job parameters")
		return
	}
	tenantID := config.DefaultTenantID
	if req.TenantID != "" {
		parsed, err := uuid.Parse(req.TenantID)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid tenant_id")
			return
		}
		tenantID = parsed
	}
	if s.limiter != nil {
		ok, retryAfter, err := s.limiter.Allow(r.Context(), tenantID.String())
		if err != nil {
			s.logger.Warn("rate limiter degraded", "error", err)
		}
		if !ok {
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds()+1)))
			writeError(w, http.StatusTooManyRequests, "rate limited")
			return
		}
	}
	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	job, created, latency, err := s.queue.Enqueue(r.Context(), queue.EnqueueParams{
		TenantID:       tenantID,
		Queue:          req.Queue,
		Handler:        req.Handler,
		Payload:        payload,
		Priority:       int16(req.Priority),
		MaxAttempts:    req.MaxAttempts,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		s.logger.Error("enqueue failed", "error", err, "tenant_id", tenantID)
		writeError(w, http.StatusInternalServerError, "enqueue failed")
		return
	}
	s.stats.RecordEnqueue(r.Context(), job.Queue, tenantID.String(), latency)
	_ = s.events.Publish(r.Context(), Event{Type: "job.enqueued", Payload: job})
	s.logger.Info("job enqueued", "job_id", job.ID, "tenant_id", tenantID, "queue", job.Queue, "handler", job.Handler)
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, job)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	job, err := s.queue.GetJob(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	jobs, err := s.queue.RecentJobs(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list jobs failed")
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	job, err := s.queue.Cancel(r.Context(), id)
	if err != nil {
		status := http.StatusConflict
		if !errors.Is(err, queue.ErrInvalidTransition) {
			status = http.StatusInternalServerError
		}
		writeError(w, status, "job is not cancellable")
		return
	}
	_ = s.events.Publish(r.Context(), Event{Type: "job.failed", Payload: job})
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) queueStats(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if name == "" {
		name = s.defaultQ
	}
	depth, err := s.queue.Depth(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "stats failed")
		return
	}
	snap := s.stats.Snapshot(r.Context(), name, depth)
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) listDLQ(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	jobs, err := s.queue.ListDLQ(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list dlq failed")
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) requeueDLQ(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	job, err := s.queue.RequeueDLQ(r.Context(), id)
	if err != nil {
		status := http.StatusConflict
		if !errors.Is(err, queue.ErrInvalidTransition) {
			status = http.StatusInternalServerError
		}
		writeError(w, status, "job is not requeueable")
		return
	}
	_ = s.events.Publish(r.Context(), Event{Type: "job.enqueued", Payload: job})
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) eventsHandler(w http.ResponseWriter, r *http.Request) {
	s.hub.ServeSSE(w, r)
}

func (s *Server) listSchedules(w http.ResponseWriter, r *http.Request) {
	items, err := s.schedules.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list schedules failed")
		return
	}
	writeJSON(w, http.StatusOK, items)
}

type createScheduleRequest struct {
	TenantID string          `json:"tenant_id"`
	Name     string          `json:"name"`
	CronExpr string          `json:"cron_expr"`
	Payload  json.RawMessage `json:"payload"`
	Queue    string          `json:"queue"`
	Handler  string          `json:"handler"`
}

func (s *Server) createSchedule(w http.ResponseWriter, r *http.Request) {
	var req createScheduleRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	tenantID := config.DefaultTenantID
	if req.TenantID != "" {
		parsed, err := uuid.Parse(req.TenantID)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid tenant_id")
			return
		}
		tenantID = parsed
	}
	item, err := s.schedules.Create(r.Context(), scheduler.CreateParams{TenantID: tenantID, Name: req.Name, CronExpr: req.CronExpr, Payload: req.Payload, Queue: req.Queue, Handler: req.Handler})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, item)
}

func (s *Server) pauseSchedule(w http.ResponseWriter, r *http.Request) {
	s.setScheduleEnabled(w, r, false)
}
func (s *Server) resumeSchedule(w http.ResponseWriter, r *http.Request) {
	s.setScheduleEnabled(w, r, true)
}

func (s *Server) setScheduleEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	item, err := s.schedules.SetEnabled(r.Context(), id, enabled)
	if err != nil {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	if err := s.schedules.Delete(r.Context(), id); err != nil {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "content-type, idempotency-key")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain exactly one JSON object")
	}
	return nil
}

type submitJobRequest struct {
	TenantID       string          `json:"tenant_id"`
	Queue          string          `json:"queue"`
	Handler        string          `json:"handler"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey string          `json:"idempotency_key"`
}

func parseID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return uuid.Nil, false
	}
	return id, true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
