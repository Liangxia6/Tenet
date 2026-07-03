package gateway

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var ErrNoAvailableWorker = errors.New("no available worker")

type RegisteredWorker struct {
	AgentID        string `json:"agent_id"`
	Address        string `json:"address"`
	ListenPort     int    `json:"listen_port"`
	MaxConcurrency int    `json:"max_concurrency"`
	ActiveCalls    int32  `json:"active_calls"`
}

type WorkerRegistry struct {
	mu      sync.RWMutex
	workers map[string]*RegisteredWorker
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{workers: map[string]*RegisteredWorker{}}
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
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[agentID] = worker
	return worker, nil
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
