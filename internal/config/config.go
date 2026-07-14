package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

var DefaultTenantID = uuid.MustParse("00000000-0000-0000-0000-000000000001")

type Config struct {
	HTTPAddr        string
	GRPCAddr        string
	DatabaseURL     string
	RedisAddr       string
	OTLPEndpoint    string
	WorkerID        string
	Queues          []string
	Concurrency     int
	LeaseDuration   time.Duration
	JobTimeout      time.Duration
	ShutdownGrace   time.Duration
	RateLimitPerSec int
	RateLimitBurst  int
}

func Load() Config {
	hostname, _ := os.Hostname()
	return Config{
		HTTPAddr:        env("HTTP_ADDR", ":8080"),
		GRPCAddr:        env("GRPC_ADDR", ":9090"),
		DatabaseURL:     env("DATABASE_URL", "postgres://forge:forge@localhost:5432/forge?sslmode=disable"),
		RedisAddr:       env("REDIS_ADDR", "localhost:6379"),
		OTLPEndpoint:    env("OTLP_ENDPOINT", ""),
		WorkerID:        env("WORKER_ID", hostname),
		Queues:          split(env("QUEUES", "default")),
		Concurrency:     envInt("CONCURRENCY", 8),
		LeaseDuration:   envDuration("LEASE_DURATION", 10*time.Second),
		JobTimeout:      envDuration("JOB_TIMEOUT", 30*time.Second),
		ShutdownGrace:   envDuration("SHUTDOWN_GRACE", 20*time.Second),
		RateLimitPerSec: envInt("RATE_LIMIT_PER_SEC", 0),
		RateLimitBurst:  envInt("RATE_LIMIT_BURST", 100),
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func split(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	if len(out) == 0 {
		return []string{"default"}
	}
	return out
}
