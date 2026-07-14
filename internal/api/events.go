package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisEventChannel = "forge:events"

type Event struct {
	Type    string `json:"type"`
	Payload any    `json:"payload"`
}

type Hub struct {
	mu      sync.Mutex
	clients map[chan Event]struct{}
}

func NewHub() *Hub {
	return &Hub{clients: make(map[chan Event]struct{})}
}

func (h *Hub) Subscribe(ctx context.Context) <-chan Event {
	ch := make(chan Event, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	go func() {
		<-ctx.Done()
		h.mu.Lock()
		delete(h.clients, ch)
		h.mu.Unlock()
		close(ch)
	}()
	return ch
}

func (h *Hub) Publish(event Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- event:
		default:
		}
	}
}

type RedisEvents struct {
	redis *redis.Client
	hub   *Hub
}

func NewRedisEvents(redisClient *redis.Client, hub *Hub) *RedisEvents {
	return &RedisEvents{redis: redisClient, hub: hub}
}

func (r *RedisEvents) Publish(ctx context.Context, event Event) error {
	if r == nil || r.redis == nil {
		if r != nil && r.hub != nil {
			r.hub.Publish(event)
		}
		return nil
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return r.redis.Publish(ctx, redisEventChannel, payload).Err()
}

func (r *RedisEvents) Run(ctx context.Context) {
	if r == nil || r.redis == nil || r.hub == nil {
		return
	}
	sub := r.redis.Subscribe(ctx, redisEventChannel)
	defer func() { _ = sub.Close() }()
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var event Event
			if err := json.Unmarshal([]byte(msg.Payload), &event); err == nil {
				r.hub.Publish(event)
			}
		}
	}
}

func (h *Hub) ServeSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	events := h.Subscribe(r.Context())
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	writer := bufio.NewWriter(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			_, _ = writer.WriteString(": ping\n\n")
			_ = writer.Flush()
			flusher.Flush()
		case event, ok := <-events:
			if !ok {
				return
			}
			payload, err := json.Marshal(event)
			if err != nil {
				continue
			}
			_, _ = fmt.Fprintf(writer, "data: %s\n\n", payload)
			_ = writer.Flush()
			flusher.Flush()
		}
	}
}
