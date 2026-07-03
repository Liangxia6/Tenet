package guard

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/tenet/orchestrator/internal/config"
)

func TestConfiguredLockManagerFallsBackToLocal(t *testing.T) {
	cfg := config.Default()
	cfg.Redis.Addr = "127.0.0.1:1"

	manager := NewConfiguredLockManager(context.Background(), cfg)
	if manager.Backend() != "local" {
		t.Fatalf("backend = %q, want local", manager.Backend())
	}
}

func TestConfiguredLockManagerUsesRedisWhenAvailable(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := config.Default()
	cfg.Redis.Addr = server.Addr()

	manager := NewConfiguredLockManager(context.Background(), cfg)
	if manager.Backend() != "redis" {
		t.Fatalf("backend = %q, want redis", manager.Backend())
	}
}

func TestRedisLockManagerLifecycle(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager := NewRedisLockManager(client, time.Minute)

	lease, err := manager.Acquire(ctx, "session:redis", "agent-a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.Token != 1 || !lease.FencingRequired || lease.Backend != "redis" {
		t.Fatalf("lease = %+v", lease)
	}
	if err := manager.Validate(ctx, lease); err != nil {
		t.Fatalf("validate lease: %v", err)
	}
	if _, err := manager.Acquire(ctx, "session:redis", "agent-b"); err == nil {
		t.Fatalf("expected second agent acquire to fail")
	}

	oldLease := lease
	lease, err = manager.Renew(ctx, lease)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if lease.Token != 2 {
		t.Fatalf("renew token = %d, want 2", lease.Token)
	}
	if err := manager.Validate(ctx, oldLease); err == nil {
		t.Fatalf("old fencing token should not validate")
	}
	if err := manager.Validate(ctx, lease); err != nil {
		t.Fatalf("new fencing token should validate: %v", err)
	}

	if err := manager.Release(ctx, lease); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := manager.Acquire(ctx, "session:redis", "agent-b"); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

func TestLocalLockManagerExcludesOtherAgents(t *testing.T) {
	ctx := context.Background()
	manager := NewLocalLockManager(time.Minute)
	lease, err := manager.Acquire(ctx, "session:1", "agent-a")
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	if lease.Token != 1 {
		t.Fatalf("token = %d, want 1", lease.Token)
	}
	if lease.FencingRequired {
		t.Fatalf("local fallback should not require shared fencing")
	}

	_, err = manager.Acquire(ctx, "session:1", "agent-b")
	if err == nil || !strings.Contains(err.Error(), "locked by another agent") {
		t.Fatalf("expected locked error, got %v", err)
	}
}

func TestLocalLockManagerValidateRejectsOldToken(t *testing.T) {
	ctx := context.Background()
	manager := NewLocalLockManager(time.Minute)
	oldLease, err := manager.Acquire(ctx, "session:1", "agent-a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	newLease, err := manager.Renew(ctx, oldLease)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := manager.Validate(ctx, oldLease); err == nil {
		t.Fatalf("old lease should not validate")
	}
	if err := manager.Validate(ctx, newLease); err != nil {
		t.Fatalf("new lease should validate: %v", err)
	}
}
