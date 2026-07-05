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
	workspacepkg "github.com/tenet/orchestrator/internal/workspace"
)

type WorkflowFunc func(context.Context, *WorkflowContext, *TaskHandle) (any, error)

type TaskHandle struct {
	StreamID     string
	ParentID     string
	Mode         ContextMode
	WorkflowType string
	SessionID    string
	TurnID       string
	RunID        string
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
	tokenMu      sync.Mutex
	tokenUsed    int64
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

// Execute 是 Tenet 后端最核心的运行入口。
// 它把一次用户输入包装成 Run：获取 session 锁、创建 WorkflowContext、
// 写入 RunStarted/RunCompleted/RunFailed/RunPaused 事件，并在成功后保存记忆和工作区 checkpoint。
// 阅读项目时可以把它理解成“Orchestrator 调度一个 Agent 回合”的主函数。
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
	if task.TurnID == "" {
		task.TurnID = "turn:" + task.StreamID
	}
	if task.RunID == "" {
		task.RunID = "run:" + task.TurnID
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
	if task.Mode == ContextModeExecution {
		if err := wfctx.Record(runCtx, "RunStarted", map[string]any{
			"session_id":    task.SessionID,
			"turn_id":       task.TurnID,
			"run_id":        task.RunID,
			"workflow_type": task.WorkflowType,
			"query":         task.Query,
			"workspace":     task.Workspace,
		}); err != nil {
			return nil, err
		}
		if err := recordAgentCheckpoint(runCtx, wfctx, task, "run_started", map[string]any{
			"tokens": map[string]any{"used": task.tokenUsed},
		}); err != nil {
			return nil, err
		}
	}
	result, runErr := fn(runCtx, wfctx, task)
	if errors.Is(runErr, ErrWorkflowSuspended) {
		_ = wfctx.Record(runCtx, "TaskPaused", map[string]any{
			"session_id": task.SessionID,
			"turn_id":    task.TurnID,
			"run_id":     task.RunID,
			"reason":     runErr.Error(),
		})
		if task.Mode == ContextModeExecution {
			_ = wfctx.Record(runCtx, "RunPaused", map[string]any{
				"session_id": task.SessionID,
				"turn_id":    task.TurnID,
				"run_id":     task.RunID,
				"reason":     runErr.Error(),
			})
		}
	} else if runErr != nil {
		_ = wfctx.Record(runCtx, "TaskFailed", map[string]any{
			"session_id": task.SessionID,
			"turn_id":    task.TurnID,
			"run_id":     task.RunID,
			"error":      runErr.Error(),
		})
		if task.Mode == ContextModeExecution {
			_ = wfctx.Record(runCtx, "RunFailed", map[string]any{
				"session_id": task.SessionID,
				"turn_id":    task.TurnID,
				"run_id":     task.RunID,
				"error":      runErr.Error(),
			})
		}
	} else if task.Mode == ContextModeExecution {
		_ = wfctx.Record(runCtx, "RunCompleted", map[string]any{
			"session_id":   task.SessionID,
			"turn_id":      task.TurnID,
			"run_id":       task.RunID,
			"final_answer": fmt.Sprint(result),
			"result":       result,
		})
		if err := recordAgentCheckpoint(runCtx, wfctx, task, "run_completed", map[string]any{
			"tokens": map[string]any{"used": task.tokenUsed},
		}); err != nil {
			return nil, err
		}
	}
	if runErr == nil && wfctx.GetVersion("summary-memory", 2) >= 2 {
		_ = wfctx.Record(runCtx, "SessionSummaryCreated", map[string]any{
			"session_id": task.SessionID,
			"turn_id":    task.TurnID,
			"run_id":     task.RunID,
			"query":      task.Query,
			"summary":    summarizeText(fmt.Sprintf("User: %s\nAssistant: %v", task.Query, result), 1200),
		})
		_ = wfctx.Record(runCtx, "WorkspaceSummaryCreated", map[string]any{
			"session_id": task.SessionID,
			"turn_id":    task.TurnID,
			"run_id":     task.RunID,
			"workspace":  task.Workspace,
			"summary":    summarizeText(fmt.Sprintf("Workspace %s after run %s", task.Workspace, task.RunID), 1200),
		})
	}
	if err := wfctx.Commit(runCtx); err != nil {
		return nil, err
	}
	if task.Mode == ContextModeExecution && runErr == nil {
		_ = task.saveRunMemory(runCtx, store, result)
		_ = task.captureRunCheckpoint(runCtx, store)
	}
	return &TaskResult{
		StreamID: task.StreamID,
		Workflow: task.WorkflowType,
		Result:   result,
		Err:      runErr,
	}, runErr
}

func summarizeText(value string, maxChars int) string {
	value = strings.TrimSpace(value)
	if maxChars <= 0 || len(value) <= maxChars {
		return value
	}
	return value[:maxChars] + "...truncated..."
}

func (task *TaskHandle) saveRunMemory(ctx context.Context, store storage.Store, result any) error {
	sessionSummary := summarizeText(fmt.Sprintf("User: %s\nAssistant: %v", task.Query, result), 1200)
	if strings.TrimSpace(sessionSummary) != "" {
		if _, err := store.SaveMemoryEntry(ctx, storage.MemoryEntry{
			StreamID:     task.StreamID,
			TurnID:       task.TurnID,
			RunID:        task.RunID,
			Workspace:    task.Workspace,
			Kind:         "session_summary",
			Content:      sessionSummary,
			SummaryLevel: 1,
			Importance:   0.7,
		}); err != nil {
			return err
		}
	}
	workspaceSummary := summarizeText(fmt.Sprintf("Workspace %s after run %s", task.Workspace, task.RunID), 1200)
	if strings.TrimSpace(workspaceSummary) != "" {
		if _, err := store.SaveMemoryEntry(ctx, storage.MemoryEntry{
			StreamID:     task.StreamID,
			TurnID:       task.TurnID,
			RunID:        task.RunID,
			Workspace:    task.Workspace,
			Kind:         "workspace_summary",
			Content:      workspaceSummary,
			SummaryLevel: 1,
			Importance:   0.6,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (task *TaskHandle) captureRunCheckpoint(ctx context.Context, store storage.Store) error {
	if task.Config == nil || strings.TrimSpace(task.Config.Workspace.SnapshotDriver) == "" {
		return nil
	}
	seq, err := store.LatestSeq(task.StreamID)
	if err != nil {
		return err
	}
	state := map[string]any{
		"session_id":    task.SessionID,
		"turn_id":       task.TurnID,
		"run_id":        task.RunID,
		"workflow_type": task.WorkflowType,
		"checkpoint":    "run_completed",
	}
	result, err := workspacepkg.NewManager(task.Config).CaptureSnapshot(ctx, store, task.StreamID, task.Workspace, task.SessionID, seq+1, state, task.LockManager, task.CurrentFencingLease())
	if err != nil {
		_, appendErr := store.AppendEvent(ctx, storage.AppendEvent{
			StreamID:  task.StreamID,
			EventType: "WorkspaceCheckpointFailed",
			Payload: map[string]any{
				"session_id": task.SessionID,
				"turn_id":    task.TurnID,
				"run_id":     task.RunID,
				"workspace":  task.Workspace,
				"error":      err.Error(),
			},
		})
		if appendErr != nil {
			return appendErr
		}
		return err
	}
	_, err = store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  task.StreamID,
		EventType: "WorkspaceCheckpointCreated",
		Payload: map[string]any{
			"session_id":     task.SessionID,
			"turn_id":        task.TurnID,
			"run_id":         task.RunID,
			"workspace":      task.Workspace,
			"snapshot_type":  result.Snapshot.Type,
			"snapshot_ref":   result.Snapshot.Ref,
			"snapshot_seq":   result.Snapshot.StreamSeq,
			"snapshot_event": result.Event.StreamSeq,
		},
	})
	if err != nil {
		return err
	}
	checkpoint, err := store.SaveAgentCheckpoint(ctx, storage.AgentCheckpoint{
		StreamID:            task.StreamID,
		TurnID:              task.TurnID,
		RunID:               task.RunID,
		EventSeq:            result.Event.StreamSeq,
		WorkflowType:        task.WorkflowType,
		Reason:              "workspace_checkpoint",
		WorkspaceSnapshotID: result.Snapshot.ID,
		ContextStateJSON:    "{}",
		MemoryStateJSON:     "{}",
		TokenStateJSON:      "{}",
		ToolStateJSON:       "{}",
	})
	if err != nil {
		return err
	}
	_, err = store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  task.StreamID,
		EventType: "AgentCheckpointCreated",
		Payload: map[string]any{
			"checkpoint_id":         checkpoint.ID,
			"stream_id":             checkpoint.StreamID,
			"session_id":            task.SessionID,
			"turn_id":               task.TurnID,
			"run_id":                task.RunID,
			"event_seq":             checkpoint.EventSeq,
			"workflow_type":         checkpoint.WorkflowType,
			"reason":                checkpoint.Reason,
			"workspace_snapshot_id": checkpoint.WorkspaceSnapshotID,
		},
	})
	return err
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
