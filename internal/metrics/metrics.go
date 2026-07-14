package metrics

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

var (
	JobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "forge_jobs_enqueued_total",
		Help: "Total jobs enqueued.",
	}, []string{"queue", "tenant"})
	JobsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "forge_jobs_completed_total",
		Help: "Total jobs completed by final status.",
	}, []string{"queue", "status"})
	JobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "forge_job_duration_seconds",
		Help:    "Job handler duration.",
		Buckets: prometheus.DefBuckets,
	}, []string{"queue"})
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "forge_queue_depth",
		Help: "Ready jobs by queue.",
	}, []string{"queue"})
	EnqueueLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "forge_enqueue_latency_seconds",
		Help:    "Latency of enqueue writes.",
		Buckets: prometheus.DefBuckets,
	})
)

type Snapshot struct {
	Queue             string    `json:"queue"`
	Depth             int64     `json:"depth"`
	ThroughputPerSec  float64   `json:"throughput_per_sec"`
	TotalProcessed    int64     `json:"total_processed"`
	P50EnqueueLatency float64   `json:"p50_enqueue_latency_ms"`
	P95EnqueueLatency float64   `json:"p95_enqueue_latency_ms"`
	P99EnqueueLatency float64   `json:"p99_enqueue_latency_ms"`
	SampledAt         time.Time `json:"sampled_at"`
}

type Recorder struct {
	redis *redis.Client
}

func NewRecorder(redisClient *redis.Client) *Recorder {
	return &Recorder{redis: redisClient}
}

func Handler() http.Handler {
	return promhttp.Handler()
}

func (r *Recorder) RecordEnqueue(ctx context.Context, queue, tenant string, latency time.Duration) {
	JobsEnqueued.WithLabelValues(queue, tenant).Inc()
	EnqueueLatency.Observe(latency.Seconds())
	if r == nil || r.redis == nil {
		return
	}
	pipe := r.redis.Pipeline()
	ms := float64(latency.Microseconds()) / 1000
	pipe.LPush(ctx, "forge:latency:"+queue, ms)
	pipe.LTrim(ctx, "forge:latency:"+queue, 0, 1999)
	pipe.Incr(ctx, "forge:enqueued:"+queue)
	_, _ = pipe.Exec(ctx)
}

func (r *Recorder) RecordComplete(ctx context.Context, queue, status string, duration time.Duration) {
	JobsCompleted.WithLabelValues(queue, status).Inc()
	JobDuration.WithLabelValues(queue).Observe(duration.Seconds())
	if r == nil || r.redis == nil {
		return
	}
	second := time.Now().Unix()
	pipe := r.redis.Pipeline()
	pipe.Incr(ctx, "forge:completed:"+queue+":"+status)
	pipe.Incr(ctx, "forge:throughput:"+queue+":"+strconv.FormatInt(second, 10))
	pipe.Expire(ctx, "forge:throughput:"+queue+":"+strconv.FormatInt(second, 10), 5*time.Minute)
	_, _ = pipe.Exec(ctx)
}

func (r *Recorder) Snapshot(ctx context.Context, queue string, depth int64) Snapshot {
	snap := Snapshot{Queue: queue, Depth: depth, SampledAt: time.Now()}
	QueueDepth.WithLabelValues(queue).Set(float64(depth))
	if r == nil || r.redis == nil {
		return snap
	}
	latencies, _ := r.redis.LRange(ctx, "forge:latency:"+queue, 0, 1999).Result()
	values := make([]float64, 0, len(latencies))
	for _, raw := range latencies {
		value, err := strconv.ParseFloat(raw, 64)
		if err == nil {
			values = append(values, value)
		}
	}
	sort.Float64s(values)
	snap.P50EnqueueLatency = percentile(values, 0.50)
	snap.P95EnqueueLatency = percentile(values, 0.95)
	snap.P99EnqueueLatency = percentile(values, 0.99)
	now := time.Now().Unix()
	var total int64
	for i := int64(0); i < 60; i++ {
		value, _ := r.redis.Get(ctx, "forge:throughput:"+queue+":"+strconv.FormatInt(now-i, 10)).Int64()
		total += value
	}
	snap.ThroughputPerSec = float64(total) / 60.0
	for _, status := range []string{"succeeded", "dead", "failed"} {
		value, _ := r.redis.Get(ctx, "forge:completed:"+queue+":"+status).Int64()
		snap.TotalProcessed += value
	}
	return snap
}

func StartDepthSampler(ctx context.Context, db *pgxpool.Pool, queues []string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, queue := range queues {
				var depth int64
				err := db.QueryRow(ctx, `
					SELECT count(*)
					FROM jobs
					WHERE queue = $1
					  AND status = 'ready'
				`, queue).Scan(&depth)
				if err == nil {
					QueueDepth.WithLabelValues(queue).Set(float64(depth))
				}
			}
		}
	}
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * p)
	return values[idx]
}
