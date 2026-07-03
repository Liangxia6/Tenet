package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/tenet/orchestrator/internal/storage"
	"github.com/tenet/orchestrator/internal/workflow"
)

var ErrShuttingDown = errors.New("scheduler is shutting down")

type Scheduler struct {
	store         storage.Store
	registry      *workflow.Registry
	queue         chan *workflow.TaskHandle
	results       chan *workflow.TaskResult
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	stopOnce      sync.Once
	closeResults  sync.Once
	mu            sync.RWMutex
	activeTasks   map[string]*workflow.TaskHandle
	shuttingDown  bool
	maxConcurrent int
}

func New(store storage.Store, registry *workflow.Registry, maxConcurrent int, queueSize int) *Scheduler {
	if registry == nil {
		registry = workflow.NewRegistry()
	}
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	if queueSize <= 0 {
		queueSize = 100
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		store:         store,
		registry:      registry,
		queue:         make(chan *workflow.TaskHandle, queueSize),
		results:       make(chan *workflow.TaskResult, queueSize),
		ctx:           ctx,
		cancel:        cancel,
		activeTasks:   map[string]*workflow.TaskHandle{},
		maxConcurrent: maxConcurrent,
	}
	for i := 0; i < maxConcurrent; i++ {
		s.wg.Add(1)
		go s.worker(i + 1)
	}
	return s
}

func (s *Scheduler) Submit(ctx context.Context, task *workflow.TaskHandle) (err error) {
	if task == nil {
		return errors.New("task is required")
	}
	s.mu.RLock()
	if s.shuttingDown {
		s.mu.RUnlock()
		return ErrShuttingDown
	}
	queue := s.queue
	s.mu.RUnlock()
	defer func() {
		if recover() != nil {
			err = ErrShuttingDown
		}
	}()
	select {
	case queue <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *Scheduler) Results() <-chan *workflow.TaskResult {
	return s.results
}

func (s *Scheduler) Stop() {
	_ = s.Shutdown(context.Background())
}

func (s *Scheduler) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.shuttingDown = true
		close(s.queue)
		s.mu.Unlock()
	})
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		s.closeResults.Do(func() { close(s.results) })
		return nil
	case <-ctx.Done():
		s.cancel()
		<-done
		s.closeResults.Do(func() { close(s.results) })
		return ctx.Err()
	}
}

type Stats struct {
	Queued        int      `json:"queued"`
	Active        int      `json:"active"`
	MaxConcurrent int      `json:"max_concurrent"`
	ShuttingDown  bool     `json:"shutting_down"`
	ActiveStreams []string `json:"active_streams"`
}

func (s *Scheduler) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	streams := make([]string, 0, len(s.activeTasks))
	for streamID := range s.activeTasks {
		streams = append(streams, streamID)
	}
	return Stats{
		Queued:        len(s.queue),
		Active:        len(s.activeTasks),
		MaxConcurrent: s.maxConcurrent,
		ShuttingDown:  s.shuttingDown,
		ActiveStreams: streams,
	}
}

func (s *Scheduler) worker(_ int) {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case task, ok := <-s.queue:
			if !ok {
				return
			}
			s.markActive(task)
			taskCtx, cancel := s.taskContext(task)
			result, err := workflow.Execute(taskCtx, s.store, s.registry, task)
			cancel()
			s.markDone(task.StreamID)
			if result == nil {
				result = &workflow.TaskResult{StreamID: task.StreamID, Workflow: task.WorkflowType, Err: err}
			}
			select {
			case s.results <- result:
			case <-s.ctx.Done():
				return
			}
		}
	}
}

func (s *Scheduler) taskContext(task *workflow.TaskHandle) (context.Context, context.CancelFunc) {
	timeout := time.Duration(0)
	if task != nil && task.Config != nil && task.Config.GRPC.ExecuteTimeoutSeconds > 0 {
		timeout = time.Duration(task.Config.GRPC.ExecuteTimeoutSeconds) * time.Second
	}
	if timeout > 0 {
		return context.WithTimeout(s.ctx, timeout)
	}
	return context.WithCancel(s.ctx)
}

func (s *Scheduler) markActive(task *workflow.TaskHandle) {
	if task == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeTasks[task.StreamID] = task
}

func (s *Scheduler) markDone(streamID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.activeTasks, streamID)
}
