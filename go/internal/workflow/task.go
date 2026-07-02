package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/tenet/orchestrator/internal/config"
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
	Subtasks     []*TaskHandle
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
	wfctx, err := NewContext(ctx, store, task.StreamID, task.ParentID, task.Mode, task.Config)
	if err != nil {
		return nil, err
	}
	result, runErr := fn(ctx, wfctx, task)
	if runErr != nil {
		_ = wfctx.Record(ctx, "TaskFailed", map[string]any{"error": runErr.Error()})
	}
	if err := wfctx.Commit(ctx); err != nil {
		return nil, err
	}
	return &TaskResult{
		StreamID: task.StreamID,
		Workflow: task.WorkflowType,
		Result:   result,
		Err:      runErr,
	}, runErr
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
