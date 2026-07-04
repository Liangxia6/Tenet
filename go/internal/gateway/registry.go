package gateway

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var ErrNoAvailableWorker = errors.New("no available worker")

type RegisteredWorker struct {
	AgentID        string   `json:"agent_id"`
	Address        string   `json:"address"`
	ListenPort     int      `json:"listen_port"`
	MaxConcurrency int      `json:"max_concurrency"`
	ActiveCalls    int32    `json:"active_calls"`
	Status         string   `json:"status"`
	LastHeartbeat  int64    `json:"last_heartbeat_unix"`
	Capabilities   []string `json:"capabilities,omitempty"`
}

type WorkerRegistry struct {
	mu               sync.RWMutex
	workers          map[string]*RegisteredWorker
	heartbeatTimeout time.Duration
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{workers: map[string]*RegisteredWorker{}, heartbeatTimeout: 30 * time.Second}
}

func (r *WorkerRegistry) Register(agentID string, listenPort, maxConcurrency int, host string) (*RegisteredWorker, error) {
	if agentID == "" {
		return nil, errors.New("agent_id is required")
	}
	if listenPort <= 0 {
		return nil, errors.New("listen_port must be positive")
	}
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	if host == "" || host == "::1" || host == "[::1]" {
		host = "127.0.0.1"
	}
	worker := &RegisteredWorker{
		AgentID:        agentID,
		Address:        fmt.Sprintf("%s:%d", host, listenPort),
		ListenPort:     listenPort,
		MaxConcurrency: maxConcurrency,
		Status:         "healthy",
		LastHeartbeat:  time.Now().UTC().Unix(),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[agentID] = worker
	return worker, nil
}

func (r *WorkerRegistry) Heartbeat(agentID string, capabilities []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	worker := r.workers[agentID]
	if worker == nil {
		return fmt.Errorf("worker %q is not registered", agentID)
	}
	worker.Status = "healthy"
	worker.LastHeartbeat = time.Now().UTC().Unix()
	worker.Capabilities = append([]string(nil), capabilities...)
	return nil
}

func (r *WorkerRegistry) MarkUnhealthy(agentID string, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	worker := r.workers[agentID]
	if worker == nil {
		return fmt.Errorf("worker %q is not registered", agentID)
	}
	if reason == "" {
		reason = "unhealthy"
	}
	worker.Status = reason
	return nil
}

func (r *WorkerRegistry) Unregister(agentID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.workers, agentID)
}

func (r *WorkerRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.workers)
}

func (r *WorkerRegistry) Snapshot() []RegisteredWorker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RegisteredWorker, 0, len(r.workers))
	for _, worker := range r.workers {
		out = append(out, *worker)
	}
	return out
}

func (r *WorkerRegistry) Lease() (*WorkerLease, error) {
	r.mu.RLock()
	var selected *RegisteredWorker
	for _, candidate := range r.workers {
		if !r.available(candidate, time.Now()) {
			continue
		}
		active := atomic.LoadInt32(&candidate.ActiveCalls)
		if int(active) >= candidate.MaxConcurrency {
			continue
		}
		if selected == nil || active < atomic.LoadInt32(&selected.ActiveCalls) {
			selected = candidate
		}
	}
	r.mu.RUnlock()
	if selected == nil {
		return nil, ErrNoAvailableWorker
	}
	atomic.AddInt32(&selected.ActiveCalls, 1)
	return &WorkerLease{worker: selected}, nil
}

func (r *WorkerRegistry) available(worker *RegisteredWorker, now time.Time) bool {
	if worker == nil || worker.Status != "healthy" {
		return false
	}
	if r.heartbeatTimeout <= 0 || worker.LastHeartbeat == 0 {
		return true
	}
	return now.Unix()-worker.LastHeartbeat <= int64(r.heartbeatTimeout.Seconds())
}

type WorkerLease struct {
	worker *RegisteredWorker
	once   sync.Once
}

func (l *WorkerLease) Worker() *RegisteredWorker {
	if l == nil {
		return nil
	}
	return l.worker
}

func (l *WorkerLease) Release() {
	if l == nil || l.worker == nil {
		return
	}
	l.once.Do(func() {
		atomic.AddInt32(&l.worker.ActiveCalls, -1)
	})
}
