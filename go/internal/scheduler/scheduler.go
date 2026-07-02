package scheduler

import (
	"context"
	"sync"

	"github.com/tenet/orchestrator/internal/storage"
	"github.com/tenet/orchestrator/internal/workflow"
)

type Scheduler struct {
	store    storage.Store
	registry *workflow.Registry
	queue    chan *workflow.TaskHandle
	results  chan *workflow.TaskResult
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
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
		store:    store,
		registry: registry,
		queue:    make(chan *workflow.TaskHandle, queueSize),
		results:  make(chan *workflow.TaskResult, queueSize),
		ctx:      ctx,
		cancel:   cancel,
	}
	for i := 0; i < maxConcurrent; i++ {
		s.wg.Add(1)
		go s.worker()
	}
	return s
}

func (s *Scheduler) Submit(ctx context.Context, task *workflow.TaskHandle) error {
	select {
	case s.queue <- task:
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
	s.cancel()
	s.wg.Wait()
	close(s.results)
}

func (s *Scheduler) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case task := <-s.queue:
			result, err := workflow.Execute(s.ctx, s.store, s.registry, task)
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
