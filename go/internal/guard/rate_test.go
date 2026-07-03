package guard

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestLocalRateLimiterBlocksOverLimit(t *testing.T) {
	limiter := NewLocalRateLimiter(map[string]ToolLimit{"shell": {PerSecond: 1}})
	if err := limiter.Allow(context.Background(), "shell"); err != nil {
		t.Fatalf("first allow: %v", err)
	}
	if err := limiter.Allow(context.Background(), "shell"); err == nil {
		t.Fatalf("expected second call to be rate limited")
	}
}

func TestRedisRateLimiterBlocksOverLimit(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	limiter := NewRedisRateLimiter(client, map[string]ToolLimit{"shell": {PerMinute: 1}})
	if err := limiter.Allow(context.Background(), "shell"); err != nil {
		t.Fatalf("first allow: %v", err)
	}
	if err := limiter.Allow(context.Background(), "shell"); err == nil {
		t.Fatalf("expected second call to be rate limited")
	}
}
