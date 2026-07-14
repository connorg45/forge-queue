package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	redis *redis.Client
	rate  int
	burst int
}

func New(redisClient *redis.Client, rate, burst int) *Limiter {
	if rate <= 0 || redisClient == nil {
		return nil
	}
	if burst <= 0 {
		burst = rate
	}
	return &Limiter{redis: redisClient, rate: rate, burst: burst}
}

func (l *Limiter) Allow(ctx context.Context, tenant string) (bool, time.Duration, error) {
	if l == nil {
		return true, 0, nil
	}
	result, err := bucketScript.Run(ctx, l.redis, []string{"forge:rl:" + tenant}, l.rate, l.burst, time.Now().UnixMilli()).Int64()
	if err != nil {
		return true, 0, err
	}
	if result >= 0 {
		return true, 0, nil
	}
	return false, time.Second, nil
}

var bucketScript = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local tokens_key = key .. ":tokens"
local ts_key = key .. ":ts"
local tokens = tonumber(redis.call("GET", tokens_key) or burst)
local ts = tonumber(redis.call("GET", ts_key) or now)
local elapsed = math.max(0, now - ts) / 1000
tokens = math.min(burst, tokens + elapsed * rate)
if tokens < 1 then
  redis.call("SET", tokens_key, tokens, "EX", 60)
  redis.call("SET", ts_key, now, "EX", 60)
  return -1
end
tokens = tokens - 1
redis.call("SET", tokens_key, tokens, "EX", 60)
redis.call("SET", ts_key, now, "EX", 60)
return tokens
`)
