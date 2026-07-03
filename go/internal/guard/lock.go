package guard

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tenet/orchestrator/internal/config"
)

type FencingLease struct {
	SessionID       string
	AgentID         string
	Token           int64
	ExpiresAt       time.Time
	Backend         string
	FencingRequired bool
}

type LockManager interface {
	Acquire(context.Context, string, string) (FencingLease, error)
	Renew(context.Context, FencingLease) (FencingLease, error)
	Validate(context.Context, FencingLease) error
	Release(context.Context, FencingLease) error
	Backend() string
}

var ErrLeaseNotCurrent = errors.New("lease is no longer current")

func NewConfiguredLockManager(ctx context.Context, cfg *config.RuntimeConfig) LockManager {
	if cfg == nil {
		cfg = config.Default()
	}
	ttl := time.Duration(cfg.Redis.SessionLockTTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if cfg.Redis.Addr == "" {
		return NewLocalLockManager(ttl)
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:        cfg.Redis.Addr,
		Password:    cfg.Redis.Password,
		DB:          cfg.Redis.DB,
		DialTimeout: 200 * time.Millisecond,
		ReadTimeout: 200 * time.Millisecond,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return NewLocalLockManager(ttl)
	}
	return NewRedisLockManager(rdb, ttl)
}

type LocalLockManager struct {
	ttl    time.Duration
	mu     sync.Mutex
	locks  map[string]FencingLease
	tokens map[string]int64
}

func NewLocalLockManager(ttl time.Duration) *LocalLockManager {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &LocalLockManager{
		ttl:    ttl,
		locks:  make(map[string]FencingLease),
		tokens: make(map[string]int64),
	}
}

func (m *LocalLockManager) Backend() string {
	return "local"
}

func (m *LocalLockManager) Acquire(_ context.Context, sessionID, agentID string) (FencingLease, error) {
	if sessionID == "" {
		return FencingLease{}, errors.New("session_id is required")
	}
	if agentID == "" {
		return FencingLease{}, errors.New("agent_id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if lease, ok := m.locks[sessionID]; ok && lease.ExpiresAt.After(now) && lease.AgentID != agentID {
		return FencingLease{}, fmt.Errorf("session %s locked by another agent", sessionID)
	}
	m.tokens[sessionID]++
	lease := FencingLease{
		SessionID: sessionID,
		AgentID:   agentID,
		Token:     m.tokens[sessionID],
		ExpiresAt: now.Add(m.ttl),
		Backend:   m.Backend(),
	}
	m.locks[sessionID] = lease
	return lease, nil
}

func (m *LocalLockManager) Renew(_ context.Context, lease FencingLease) (FencingLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.locks[lease.SessionID]
	if !ok || current.AgentID != lease.AgentID || current.Token != lease.Token {
		return FencingLease{}, fmt.Errorf("%w: session %s", ErrLeaseNotCurrent, lease.SessionID)
	}
	m.tokens[lease.SessionID]++
	current.Token = m.tokens[lease.SessionID]
	current.ExpiresAt = time.Now().Add(m.ttl)
	current.Backend = m.Backend()
	m.locks[lease.SessionID] = current
	return current, nil
}

func (m *LocalLockManager) Validate(_ context.Context, lease FencingLease) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.locks[lease.SessionID]
	if ok && current.AgentID == lease.AgentID && current.Token == lease.Token && current.ExpiresAt.After(time.Now()) {
		return nil
	}
	return fmt.Errorf("%w: session %s", ErrLeaseNotCurrent, lease.SessionID)
}

func (m *LocalLockManager) Release(_ context.Context, lease FencingLease) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.locks[lease.SessionID]
	if ok && current.AgentID == lease.AgentID && current.Token == lease.Token {
		delete(m.locks, lease.SessionID)
	}
	return nil
}

type RedisLockManager struct {
	client redis.UniversalClient
	ttl    time.Duration
}

func NewRedisLockManager(client redis.UniversalClient, ttl time.Duration) *RedisLockManager {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &RedisLockManager{client: client, ttl: ttl}
}

func (m *RedisLockManager) Backend() string {
	return "redis"
}

func (m *RedisLockManager) Acquire(ctx context.Context, sessionID, agentID string) (FencingLease, error) {
	if sessionID == "" {
		return FencingLease{}, errors.New("session_id is required")
	}
	if agentID == "" {
		return FencingLease{}, errors.New("agent_id is required")
	}
	ok, err := m.client.SetNX(ctx, lockKey(sessionID), agentID, m.ttl).Result()
	if err != nil {
		return FencingLease{}, err
	}
	if !ok {
		return FencingLease{}, fmt.Errorf("session %s locked by another agent", sessionID)
	}
	token, err := m.client.Incr(ctx, fencingKey(sessionID)).Result()
	if err != nil {
		_ = m.Release(ctx, FencingLease{SessionID: sessionID, AgentID: agentID})
		return FencingLease{}, err
	}
	return FencingLease{
		SessionID:       sessionID,
		AgentID:         agentID,
		Token:           token,
		ExpiresAt:       time.Now().Add(m.ttl),
		Backend:         m.Backend(),
		FencingRequired: true,
	}, nil
}

func (m *RedisLockManager) Renew(ctx context.Context, lease FencingLease) (FencingLease, error) {
	result, err := renewLeaseScript.Run(ctx, m.client, []string{lockKey(lease.SessionID), fencingKey(lease.SessionID)}, lease.AgentID, int(m.ttl.Seconds())).Int64()
	if err != nil {
		return FencingLease{}, err
	}
	if result <= 0 {
		return FencingLease{}, fmt.Errorf("%w: session %s", ErrLeaseNotCurrent, lease.SessionID)
	}
	lease.Token = result
	lease.ExpiresAt = time.Now().Add(m.ttl)
	lease.Backend = m.Backend()
	lease.FencingRequired = true
	return lease, nil
}

func (m *RedisLockManager) Validate(ctx context.Context, lease FencingLease) error {
	values, err := m.client.MGet(ctx, lockKey(lease.SessionID), fencingKey(lease.SessionID)).Result()
	if err != nil {
		return err
	}
	if len(values) != 2 || values[0] == nil || values[1] == nil {
		return fmt.Errorf("%w: session %s", ErrLeaseNotCurrent, lease.SessionID)
	}
	if owner, ok := values[0].(string); !ok || owner != lease.AgentID {
		return fmt.Errorf("%w: session %s owner changed", ErrLeaseNotCurrent, lease.SessionID)
	}
	tokenText := fmt.Sprint(values[1])
	if tokenText != fmt.Sprint(lease.Token) {
		return fmt.Errorf("%w: session %s fencing token mismatch", ErrLeaseNotCurrent, lease.SessionID)
	}
	return nil
}

func (m *RedisLockManager) Release(ctx context.Context, lease FencingLease) error {
	_, err := releaseLeaseScript.Run(ctx, m.client, []string{lockKey(lease.SessionID)}, lease.AgentID).Result()
	return err
}

func lockKey(sessionID string) string {
	return "session_lock:" + sessionID
}

func fencingKey(sessionID string) string {
	return "session_fencing:" + sessionID
}

var renewLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("EXPIRE", KEYS[1], ARGV[2])
  return redis.call("INCR", KEYS[2])
end
return 0
`)

var releaseLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)
