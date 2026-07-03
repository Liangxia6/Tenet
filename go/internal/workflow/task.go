package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/guard"
	"github.com/tenet/orchestrator/internal/storage"
	"github.com/tenet/orchestrator/internal/worker"
)

type WorkflowFunc func(context.Context, *WorkflowContext, *TaskHandle) (any, error)

type TaskHandle struct {
	StreamID     string
	ParentID     string
	Mode         ContextMode
	WorkflowType string
	SessionID    string
	Query        string
	Workspace    string
	SystemPrompt string
	Messages     []worker.Message
	Tools        []worker.ToolDefinition
	AgentRole    string
	Model        string
	Config       *config.RuntimeConfig
	Client       worker.Client
	LockManager  guard.LockManager
	RateLimiter  guard.RateLimiter
	Subtasks     []*TaskHandle

	lockMu       sync.RWMutex
	fencingLease guard.FencingLease
}

type TaskResult struct {
	StreamID string
	Workflow string
	Result   any
	Err      error
}

type Registry struct {
	workflows map[string]WorkflowFunc
}

func NewRegistry() *Registry {
	r := &Registry{workflows: make(map[string]WorkflowFunc)}
	r.Register("simple", SimpleWorkflow)
	r.Register("react", ReactWorkflow)
	r.Register("dag", DAGWorkflow)
	r.Register("interactive", InteractiveWorkflow)
	r.Register("scientific", ScientificWorkflow)
	r.Register("coding", CodingWorkflow)
	return r
}

func (r *Registry) Register(name string, fn WorkflowFunc) {
	r.workflows[strings.ToLower(name)] = fn
}

func (r *Registry) Get(name string) (WorkflowFunc, bool) {
	fn, ok := r.workflows[strings.ToLower(name)]
	return fn, ok
}

func Execute(ctx context.Context, store storage.Store, registry *Registry, task *TaskHandle) (*TaskResult, error) {
	if task == nil {
		return nil, errors.New("task is required")
	}
	if registry == nil {
		registry = NewRegistry()
	}
	if task.Config == nil {
		task.Config = config.Default()
	}
	if task.Client == nil {
		c := worker.NewEchoClient()
		task.Client = c
	}
	if task.Mode == "" {
		task.Mode = ContextModeExecution
	}
	if task.StreamID == "" {
		return nil, errors.New("task stream_id is required")
	}
	if task.WorkflowType == "" || task.WorkflowType == "auto" {
		task.WorkflowType = "simple"
	}
	if task.SessionID == "" {
		task.SessionID = task.StreamID
	}
	if task.AgentRole == "" {
		task.AgentRole = "default"
	}
	if task.Workspace == "" {
		task.Workspace = "."
	}
	abs, err := filepath.Abs(task.Workspace)
	if err == nil {
		task.Workspace = abs
	}
	fn, ok := registry.Get(task.WorkflowType)
	if !ok {
		fn, _ = registry.Get("simple")
		task.WorkflowType = "simple"
	}
	runCtx := ctx
	var release func()
	if task.Mode == ContextModeExecution {
		var err error
		runCtx, release, err = task.acquireSessionLease(ctx)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	wfctx, err := NewContext(runCtx, store, task.StreamID, task.ParentID, task.Mode, task.Config)
	if err != nil {
		return nil, err
	}
	result, runErr := fn(runCtx, wfctx, task)
	if runErr != nil {
		_ = wfctx.Record(runCtx, "TaskFailed", map[string]any{"error": runErr.Error()})
	}
	if err := wfctx.Commit(runCtx); err != nil {
		return nil, err
	}
	return &TaskResult{
		StreamID: task.StreamID,
		Workflow: task.WorkflowType,
		Result:   result,
		Err:      runErr,
	}, runErr
}

func (task *TaskHandle) acquireSessionLease(ctx context.Context) (context.Context, func(), error) {
	if task.LockManager == nil {
		task.LockManager = guard.NewConfiguredLockManager(ctx, task.Config)
	}
	if task.RateLimiter == nil {
		task.RateLimiter = guard.NewConfiguredRateLimiter(ctx, task.Config)
	}
	lease, err := task.LockManager.Acquire(ctx, task.SessionID, task.AgentRole)
	if err != nil {
		return nil, nil, err
	}
	task.setFencingLease(lease)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	heartbeat := time.Duration(task.Config.Redis.SessionHeartbeatSeconds) * time.Second
	if heartbeat <= 0 {
		heartbeat = 10 * time.Second
	}
	go task.renewSessionLease(runCtx, heartbeat, cancel, done)

	release := func() {
		close(done)
		cancel()
		_ = task.LockManager.Release(context.Background(), task.CurrentFencingLease())
	}
	return runCtx, release, nil
}

func (task *TaskHandle) renewSessionLease(ctx context.Context, heartbeat time.Duration, cancel context.CancelFunc, done <-chan struct{}) {
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			lease, err := task.LockManager.Renew(ctx, task.CurrentFencingLease())
			if err != nil {
				cancel()
				return
			}
			task.setFencingLease(lease)
		}
	}
}

func (task *TaskHandle) setFencingLease(lease guard.FencingLease) {
	task.lockMu.Lock()
	defer task.lockMu.Unlock()
	task.fencingLease = lease
}

func (task *TaskHandle) CurrentFencingLease() guard.FencingLease {
	task.lockMu.RLock()
	defer task.lockMu.RUnlock()
	return task.fencingLease
}

func (task *TaskHandle) CurrentFencingToken() int64 {
	lease := task.CurrentFencingLease()
	if !lease.FencingRequired {
		return 0
	}
	return lease.Token
}

func (task *TaskHandle) ValidateFencingLease(ctx context.Context) error {
	if task.LockManager == nil {
		return nil
	}
	lease := task.CurrentFencingLease()
	if lease.SessionID == "" || !lease.FencingRequired {
		return nil
	}
	return task.LockManager.Validate(ctx, lease)
}

func (task *TaskHandle) CheckToolRateLimit(ctx context.Context, toolName string) error {
	if task.RateLimiter == nil {
		return nil
	}
	return task.RateLimiter.Allow(ctx, toolName)
}

func coerce[T any](value any) (T, error) {
	var out T
	if typed, ok := value.(T); ok {
		return typed, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return out, fmt.Errorf("marshal decision result: %w", err)
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, fmt.Errorf("decode decision result: %w", err)
	}
	return out, nil
}
