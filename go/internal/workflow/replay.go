package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/storage"
	"github.com/tenet/orchestrator/internal/worker"
)

type ReplayResult struct {
	StreamID       string
	SessionID      string
	TurnID         string
	RunID          string
	Workflow       string
	EventsReplayed int
	LatestSeq      int64
	Result         any
}

func Replay(ctx context.Context, store storage.Store, registry *Registry, task *TaskHandle) (*ReplayResult, error) {
	if store == nil {
		return nil, errors.New("store is required")
	}
	if task == nil || task.StreamID == "" {
		return nil, errors.New("task stream_id is required")
	}
	if registry == nil {
		registry = NewRegistry()
	}
	cfg := task.Config
	if cfg == nil {
		cfg = config.Default()
	}
	events, err := store.Read(task.StreamID, 1)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("stream %s has no events", task.StreamID)
	}
	beforeSeq, err := store.LatestSeq(task.StreamID)
	if err != nil {
		return nil, err
	}
	spec, err := buildReplaySpec(task, events)
	if err != nil {
		return nil, err
	}
	if task.WorkflowType == "" {
		task.WorkflowType = spec.workflowType
	}
	if task.WorkflowType == "" || task.WorkflowType == "auto" {
		task.WorkflowType = "simple"
	}
	fn, ok := registry.Get(task.WorkflowType)
	if !ok {
		return nil, fmt.Errorf("workflow %q is not registered", task.WorkflowType)
	}
	if task.SessionID == "" {
		task.SessionID = firstReplayNonEmpty(spec.sessionID, task.StreamID)
	}
	if task.TurnID == "" {
		task.TurnID = spec.turnID
	}
	if task.RunID == "" {
		task.RunID = spec.runID
	}
	if task.Query == "" {
		task.Query = spec.query
	}
	if task.Workspace == "" {
		task.Workspace = spec.workspace
	}
	if task.Workspace == "" {
		task.Workspace = "."
	}
	if abs, err := filepath.Abs(task.Workspace); err == nil {
		task.Workspace = abs
	}
	task.Mode = ContextModeReplay
	task.Config = cfg
	task.Client = replayGuardClient{}
	if task.AgentRole == "" {
		task.AgentRole = "default"
	}

	wfctx := &WorkflowContext{
		store:           store,
		streamID:        task.StreamID,
		parentID:        task.ParentID,
		mode:            ContextModeReplay,
		history:         append([]storage.Event(nil), spec.history...),
		recordBatchSize: replayRecordBatchSize(cfg),
		config:          cfg,
		versionMarkers:  map[string]bool{},
	}
	if spec.hasRunEvents {
		if err := wfctx.Record(ctx, "RunStarted", map[string]any{
			"session_id":    task.SessionID,
			"turn_id":       task.TurnID,
			"run_id":        task.RunID,
			"workflow_type": task.WorkflowType,
			"query":         task.Query,
			"workspace":     task.Workspace,
		}); err != nil {
			return nil, err
		}
	}
	result, runErr := fn(ctx, wfctx, task)
	if errors.Is(runErr, ErrWorkflowSuspended) {
		if err := wfctx.Record(ctx, "TaskPaused", map[string]any{
			"session_id": task.SessionID,
			"turn_id":    task.TurnID,
			"run_id":     task.RunID,
			"reason":     runErr.Error(),
		}); err != nil {
			return nil, err
		}
		if spec.hasRunEvents {
			if err := wfctx.Record(ctx, "RunPaused", map[string]any{
				"session_id": task.SessionID,
				"turn_id":    task.TurnID,
				"run_id":     task.RunID,
				"reason":     runErr.Error(),
			}); err != nil {
				return nil, err
			}
		}
	} else if runErr != nil {
		if err := wfctx.Record(ctx, "TaskFailed", map[string]any{
			"session_id": task.SessionID,
			"turn_id":    task.TurnID,
			"run_id":     task.RunID,
			"error":      runErr.Error(),
		}); err != nil {
			return nil, err
		}
		if spec.hasRunEvents {
			if err := wfctx.Record(ctx, "RunFailed", map[string]any{
				"session_id": task.SessionID,
				"turn_id":    task.TurnID,
				"run_id":     task.RunID,
				"error":      runErr.Error(),
			}); err != nil {
				return nil, err
			}
		}
	} else if spec.hasRunEvents {
		if err := wfctx.Record(ctx, "RunCompleted", map[string]any{
			"session_id":   task.SessionID,
			"turn_id":      task.TurnID,
			"run_id":       task.RunID,
			"final_answer": fmt.Sprint(result),
			"result":       result,
		}); err != nil {
			return nil, err
		}
	}
	if runErr == nil && wfctx.GetVersion("summary-memory", 2) >= 2 {
		if err := wfctx.Record(ctx, "SessionSummaryCreated", map[string]any{
			"session_id": task.SessionID,
			"turn_id":    task.TurnID,
			"run_id":     task.RunID,
			"query":      task.Query,
			"summary":    summarizeText(fmt.Sprintf("User: %s\nAssistant: %v", task.Query, result), 1200),
		}); err != nil {
			return nil, err
		}
		if err := wfctx.Record(ctx, "WorkspaceSummaryCreated", map[string]any{
			"session_id": task.SessionID,
			"turn_id":    task.TurnID,
			"run_id":     task.RunID,
			"workspace":  task.Workspace,
			"summary":    summarizeText(fmt.Sprintf("Workspace %s after run %s", task.Workspace, task.RunID), 1200),
		}); err != nil {
			return nil, err
		}
	}
	if err := wfctx.Commit(ctx); err != nil {
		return nil, err
	}
	if wfctx.HistoryPosition() != wfctx.HistoryLength() {
		return nil, fmt.Errorf("replay did not consume all history: consumed=%d total=%d", wfctx.HistoryPosition(), wfctx.HistoryLength())
	}
	afterSeq, err := store.LatestSeq(task.StreamID)
	if err != nil {
		return nil, err
	}
	if afterSeq != beforeSeq {
		return nil, fmt.Errorf("replay appended events: before_seq=%d after_seq=%d", beforeSeq, afterSeq)
	}
	if runErr != nil && !errors.Is(runErr, ErrWorkflowSuspended) {
		return nil, runErr
	}
	return &ReplayResult{
		StreamID:       task.StreamID,
		SessionID:      task.SessionID,
		TurnID:         task.TurnID,
		RunID:          task.RunID,
		Workflow:       task.WorkflowType,
		EventsReplayed: wfctx.HistoryPosition(),
		LatestSeq:      afterSeq,
		Result:         result,
	}, nil
}

type replaySpec struct {
	history      []storage.Event
	hasRunEvents bool
	workflowType string
	sessionID    string
	turnID       string
	runID        string
	query        string
	workspace    string
}

func buildReplaySpec(task *TaskHandle, events []storage.Event) (replaySpec, error) {
	spec := replaySpec{}
	selectedRunID := task.RunID
	runStart := -1
	runEnd := -1
	for i, event := range events {
		payload := decodeReplayPayload(event)
		spec.sessionID = firstReplayNonEmpty(spec.sessionID, stringReplayValue(payload, "session_id"))
		spec.turnID = firstReplayNonEmpty(spec.turnID, stringReplayValue(payload, "turn_id"))
		spec.runID = firstReplayNonEmpty(spec.runID, stringReplayValue(payload, "run_id"))
		spec.query = firstReplayNonEmpty(spec.query, stringReplayValue(payload, "query"), stringReplayValue(payload, "content"))
		spec.workspace = firstReplayNonEmpty(spec.workspace, stringReplayValue(payload, "workspace"))
		spec.workflowType = firstReplayNonEmpty(spec.workflowType, stringReplayValue(payload, "workflow_type"), stringReplayValue(payload, "selected_workflow"))
		if event.EventType != "RunStarted" {
			continue
		}
		runID := stringReplayValue(payload, "run_id")
		if selectedRunID != "" && runID != selectedRunID {
			continue
		}
		runStart = i
		runEnd = -1
		spec.hasRunEvents = true
		spec.sessionID = firstReplayNonEmpty(stringReplayValue(payload, "session_id"), spec.sessionID)
		spec.turnID = stringReplayValue(payload, "turn_id")
		spec.runID = runID
		spec.query = firstReplayNonEmpty(stringReplayValue(payload, "query"), spec.query)
		spec.workspace = firstReplayNonEmpty(stringReplayValue(payload, "workspace"), spec.workspace)
		spec.workflowType = firstReplayNonEmpty(stringReplayValue(payload, "workflow_type"), spec.workflowType)
	}
	if runStart >= 0 {
		targetRunID := spec.runID
		for i := runStart + 1; i < len(events); i++ {
			if events[i].EventType != "RunCompleted" && events[i].EventType != "RunFailed" && events[i].EventType != "RunPaused" {
				continue
			}
			payload := decodeReplayPayload(events[i])
			if targetRunID == "" || stringReplayValue(payload, "run_id") == targetRunID {
				runEnd = i
				break
			}
		}
		if runEnd < 0 {
			return replaySpec{}, fmt.Errorf("run %q has no terminal RunCompleted/RunFailed/RunPaused event", targetRunID)
		}
		for runEnd+1 < len(events) && postRunReplayEvent(events[runEnd+1].EventType) {
			runEnd++
		}
		spec.history = append([]storage.Event(nil), events[runStart:runEnd+1]...)
	} else {
		for _, event := range events {
			if replayMetadataEvent(event.EventType) {
				continue
			}
			spec.history = append(spec.history, event)
		}
	}
	spec.workflowType = firstReplayNonEmpty(task.WorkflowType, spec.workflowType, "simple")
	spec.sessionID = firstReplayNonEmpty(task.SessionID, spec.sessionID, task.StreamID)
	spec.turnID = firstReplayNonEmpty(task.TurnID, spec.turnID)
	spec.runID = firstReplayNonEmpty(task.RunID, spec.runID)
	spec.query = firstReplayNonEmpty(task.Query, spec.query)
	spec.workspace = firstReplayNonEmpty(task.Workspace, spec.workspace, ".")
	if len(spec.history) == 0 {
		return replaySpec{}, fmt.Errorf("stream %s has no workflow events to replay", task.StreamID)
	}
	return spec, nil
}

func postRunReplayEvent(eventType string) bool {
	switch eventType {
	case "VersionMarker", "SessionSummaryCreated", "WorkspaceSummaryCreated":
		return true
	default:
		return false
	}
}

func replayMetadataEvent(eventType string) bool {
	switch eventType {
	case "SessionCreated", "TurnCreated", "TaskCreated", "ComplexityAnalyzed", "UserMessage", "TaskResumed", "ForkCreated":
		return true
	default:
		return false
	}
}

func replayRecordBatchSize(cfg *config.RuntimeConfig) int {
	if cfg != nil && cfg.Workflow.RecordBatchSize > 0 {
		return cfg.Workflow.RecordBatchSize
	}
	return 20
}

func decodeReplayPayload(event storage.Event) map[string]any {
	payload := map[string]any{}
	_ = json.Unmarshal([]byte(event.Payload), &payload)
	return payload
}

func stringReplayValue(values map[string]any, key string) string {
	if value, ok := values[key].(string); ok {
		return value
	}
	return ""
}

func firstReplayNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

type replayGuardClient struct{}

func (replayGuardClient) GenerateThought(context.Context, worker.GenerateThoughtRequest) (worker.GenerateThoughtResponse, error) {
	return worker.GenerateThoughtResponse{}, errors.New("replay attempted external LLM call")
}

func (replayGuardClient) ExecuteTool(context.Context, worker.ExecuteToolRequest) (worker.ExecuteToolResponse, error) {
	return worker.ExecuteToolResponse{}, errors.New("replay attempted external tool call")
}

func (replayGuardClient) HealthCheck(context.Context) (worker.HealthCheckResponse, error) {
	return worker.HealthCheckResponse{Status: "REPLAY"}, nil
}
