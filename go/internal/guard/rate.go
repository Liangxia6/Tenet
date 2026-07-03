package guard

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tenet/orchestrator/internal/config"
)

type RateLimiter interface {
	Allow(context.Context, string) error
	Backend() string
}

type ToolLimit struct {
	PerSecond int
	PerMinute int
}

func NewConfiguredRateLimiter(ctx context.Context, cfg *config.RuntimeConfig) RateLimiter {
	if cfg == nil {
		cfg = config.Default()
	}
	limits := limitsFromConfig(cfg)
	if cfg.Redis.Addr == "" {
		return NewLocalRateLimiter(limits)
	}
	client := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB, DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond})
	pingCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return NewLocalRateLimiter(limits)
	}
	return NewRedisRateLimiter(client, limits)
}

func limitsFromConfig(cfg *config.RuntimeConfig) map[string]ToolLimit {
	return map[string]ToolLimit{
		"shell":      {PerSecond: cfg.RateLimits.Shell.MaxPerSecond, PerMinute: cfg.RateLimits.Shell.MaxPerMinute},
		"web_search": {PerMinute: cfg.RateLimits.WebSearch.MaxPerMinute},
		"write_file": {PerSecond: cfg.RateLimits.WriteFile.MaxPerSecond},
	}
}

type LocalRateLimiter struct {
	mu     sync.Mutex
	limits map[string]ToolLimit
	counts map[string]int
}

func NewLocalRateLimiter(limits map[string]ToolLimit) *LocalRateLimiter {
	return &LocalRateLimiter{limits: limits, counts: map[string]int{}}
}

func (l *LocalRateLimiter) Backend() string { return "local" }

func (l *LocalRateLimiter) Allow(_ context.Context, tool string) error {
	limit := l.limits[tool]
	if limit.PerSecond <= 0 && limit.PerMinute <= 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if limit.PerSecond > 0 {
		key := fmt.Sprintf("%s:%d", tool, now.Unix())
		l.counts[key]++
		if l.counts[key] > limit.PerSecond {
			return fmt.Errorf("tool %s exceeded per-second limit %d", tool, limit.PerSecond)
		}
	}
	if limit.PerMinute > 0 {
		key := fmt.Sprintf("%s:%d", tool, now.Unix()/60)
		l.counts[key]++
		if l.counts[key] > limit.PerMinute {
			return fmt.Errorf("tool %s exceeded per-minute limit %d", tool, limit.PerMinute)
		}
	}
	return nil
}

type RedisRateLimiter struct {
	client redis.UniversalClient
	limits map[string]ToolLimit
}

func NewRedisRateLimiter(client redis.UniversalClient, limits map[string]ToolLimit) *RedisRateLimiter {
	return &RedisRateLimiter{client: client, limits: limits}
}

func (l *RedisRateLimiter) Backend() string { return "redis" }

func (l *RedisRateLimiter) Allow(ctx context.Context, tool string) error {
	limit := l.limits[tool]
	if limit.PerSecond <= 0 && limit.PerMinute <= 0 {
		return nil
	}
	now := time.Now()
	if limit.PerSecond > 0 {
		if err := l.check(ctx, tool, now.Unix(), time.Second+time.Second, limit.PerSecond); err != nil {
			return err
		}
	}
	if limit.PerMinute > 0 {
		if err := l.check(ctx, tool, now.Unix()/60, time.Minute+time.Second, limit.PerMinute); err != nil {
			return err
		}
	}
	return nil
}

func (l *RedisRateLimiter) check(ctx context.Context, tool string, window int64, ttl time.Duration, limit int) error {
	key := fmt.Sprintf("tool_rl:%s:%d", tool, window)
	count, err := rateLimitScript.Run(ctx, l.client, []string{key}, int(ttl.Seconds())).Int()
	if err != nil {
		return err
	}
	if count > limit {
		return fmt.Errorf("tool %s exceeded limit %d", tool, limit)
	}
	return nil
}

var rateLimitScript = redis.NewScript(`
local count = redis.call("INCR", KEYS[1])
if count == 1 then
  redis.call("EXPIRE", KEYS[1], ARGV[1])
end
return count
`)
