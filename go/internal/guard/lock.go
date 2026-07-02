package guard

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type FencingLease struct {
	SessionID string
	AgentID   string
	Token     int64
	ExpiresAt time.Time
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

func (m *LocalLockManager) Acquire(sessionID, agentID string) (FencingLease, error) {
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
	}
	m.locks[sessionID] = lease
	return lease, nil
}

func (m *LocalLockManager) Renew(lease FencingLease) (FencingLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.locks[lease.SessionID]
	if !ok || current.AgentID != lease.AgentID || current.Token != lease.Token {
		return FencingLease{}, fmt.Errorf("lease for session %s is no longer current", lease.SessionID)
	}
	m.tokens[lease.SessionID]++
	current.Token = m.tokens[lease.SessionID]
	current.ExpiresAt = time.Now().Add(m.ttl)
	m.locks[lease.SessionID] = current
	return current, nil
}

func (m *LocalLockManager) Validate(lease FencingLease) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.locks[lease.SessionID]
	return ok && current.AgentID == lease.AgentID && current.Token == lease.Token && current.ExpiresAt.After(time.Now())
}

func (m *LocalLockManager) Release(lease FencingLease) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.locks[lease.SessionID]
	if ok && current.AgentID == lease.AgentID && current.Token == lease.Token {
		delete(m.locks, lease.SessionID)
	}
}
