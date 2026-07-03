package projection

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/storage"
)

type TaskStatus string

const (
	StatusRunning   TaskStatus = "RUNNING"
	StatusPaused    TaskStatus = "PAUSED"
	StatusCompleted TaskStatus = "COMPLETED"
	StatusFailed    TaskStatus = "FAILED"
)

type TaskView struct {
	StreamID     string         `json:"stream_id"`
	Status       TaskStatus     `json:"status"`
	Query        string         `json:"query,omitempty"`
	Workspace    string         `json:"workspace,omitempty"`
	WorkflowType string         `json:"workflow_type,omitempty"`
	CurrentPhase string         `json:"current_phase,omitempty"`
	Progress     Progress       `json:"progress"`
	Subtasks     []SubTaskState `json:"subtasks,omitempty"`
	FinalAnswer  string         `json:"final_answer,omitempty"`
	Error        string         `json:"error,omitempty"`
	Timeline     TimelineState  `json:"timeline"`
	Tokens       TokenState     `json:"tokens"`
}

type Progress struct {
	CompletedSteps int `json:"completed_steps"`
	TotalSteps     int `json:"total_steps"`
}

type SubTaskState struct {
	ID        string     `json:"id"`
	AgentRole string     `json:"agent_role,omitempty"`
	Status    TaskStatus `json:"status"`
	Result    string     `json:"result,omitempty"`
}

type TimelineState struct {
	StreamID       string         `json:"stream_id"`
	Steps          []TimelineStep `json:"steps"`
	TotalSteps     int            `json:"total_steps"`
	DurationMillis int64          `json:"duration_ms"`
}

type TimelineStep struct {
	Seq        int64     `json:"seq"`
	Type       string    `json:"type"`
	Content    string    `json:"content,omitempty"`
	ToolName   string    `json:"tool_name,omitempty"`
	Stdout     string    `json:"stdout,omitempty"`
	Stderr     string    `json:"stderr,omitempty"`
	DurationMS int64     `json:"duration_ms,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}

type TokenState struct {
	TotalTokens      int64            `json:"total_tokens"`
	PromptTokens     int64            `json:"prompt_tokens"`
	CompletionTokens int64            `json:"completion_tokens"`
	TotalCostUSD     float64          `json:"total_cost_usd"`
	ByAgent          map[string]int64 `json:"by_agent"`
	ByModel          map[string]int64 `json:"by_model"`
	BudgetLimit      int64            `json:"budget_limit"`
	BudgetExceeded   bool             `json:"budget_exceeded"`
}

type Engine struct {
	store       storage.Store
	config      *config.RuntimeConfig
	mu          sync.RWMutex
	projections map[string]*TaskProjection
}

func NewEngine(store storage.Store, cfg *config.RuntimeConfig) *Engine {
	if cfg == nil {
		cfg = config.Default()
	}
	return &Engine{store: store, config: cfg, projections: map[string]*TaskProjection{}}
}

func (e *Engine) Apply(event storage.Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	projection := e.projections[event.StreamID]
	if projection == nil {
		projection = NewTaskProjection(event.StreamID, int64(e.config.Agent.DefaultTokenBudget))
		e.projections[event.StreamID] = projection
	}
	if err := projection.Apply(event); err != nil {
		return err
	}
	if e.shouldPersistSnapshot(event) {
		return e.persistSnapshotLocked(context.Background(), projection, event.StreamSeq)
	}
	return nil
}

func (e *Engine) ProjectTask(streamID string) (TaskView, error) {
	if streamID == "" {
		return TaskView{}, fmt.Errorf("stream_id is required")
	}
	e.mu.RLock()
	if projection := e.projections[streamID]; projection != nil {
		state := projection.State()
		e.mu.RUnlock()
		return state, nil
	}
	e.mu.RUnlock()
	view := NewTaskProjection(streamID, int64(e.config.Agent.DefaultTokenBudget))
	fromSeq := int64(1)
	snapshot, err := e.store.LatestProjectionSnapshot(streamID)
	if err == nil {
		if err := view.Restore([]byte(snapshot.StateBlob)); err != nil {
			return TaskView{}, err
		}
		fromSeq = snapshot.StreamSeq + 1
	} else if !errors.Is(err, sql.ErrNoRows) {
		return TaskView{}, err
	}
	events, err := e.store.Read(streamID, fromSeq)
	if err != nil {
		return TaskView{}, err
	}
	for _, event := range events {
		if err := view.Apply(event); err != nil {
			return TaskView{}, err
		}
	}
	if len(events) > 0 && e.shouldPersistSnapshot(events[len(events)-1]) {
		if err := e.persistSnapshotLocked(context.Background(), view, events[len(events)-1].StreamSeq); err != nil {
			return TaskView{}, err
		}
	}
	e.mu.Lock()
	e.projections[streamID] = view
	e.mu.Unlock()
	return view.State(), nil
}

func (e *Engine) shouldPersistSnapshot(event storage.Event) bool {
	switch event.EventType {
	case "TaskCompleted", "TaskFailed", "TaskCancelled", "TaskPaused":
		return true
	}
	interval := e.config.Workflow.SnapshotEventInterval
	if interval <= 0 {
		interval = 50
	}
	return event.StreamSeq > 0 && event.StreamSeq%int64(interval) == 0
}

func (e *Engine) persistSnapshotLocked(ctx context.Context, projection *TaskProjection, seq int64) error {
	if projection == nil || seq <= 0 {
		return nil
	}
	data, err := projection.Snapshot()
	if err != nil {
		return err
	}
	_, err = e.store.SaveProjectionSnapshot(ctx, storage.ProjectionSnapshot{
		StreamID:  projection.state.StreamID,
		StreamSeq: seq,
		StateBlob: string(data),
	})
	return err
}

type TaskProjection struct {
	state       TaskView
	subtasks    map[string]SubTaskState
	firstAction time.Time
	lastAction  time.Time
}

func NewTaskProjection(streamID string, budgetLimit int64) *TaskProjection {
	return &TaskProjection{
		state: TaskView{
			StreamID: streamID,
			Status:   StatusRunning,
			Progress: Progress{},
			Timeline: TimelineState{StreamID: streamID},
			Tokens: TokenState{
				ByAgent:     map[string]int64{},
				ByModel:     map[string]int64{},
				BudgetLimit: budgetLimit,
			},
		},
		subtasks: map[string]SubTaskState{},
	}
}

func (p *TaskProjection) Apply(event storage.Event) error {
	payload, err := decodePayload(event.Payload)
	if err != nil {
		return err
	}
	switch event.EventType {
	case "TaskCreated", "TaskStarted":
		p.state.Status = StatusRunning
		p.state.Query = stringValue(payload, "query")
		p.state.Workspace = stringValue(payload, "workspace")
		p.state.WorkflowType = stringValue(payload, "workflow_type")
	case "TaskDecomposed":
		p.state.Progress.TotalSteps = intValue(payload, "total_steps")
	case "SubTaskDispatched":
		id := stringValue(payload, "subtask_id")
		if id != "" {
			p.subtasks[id] = SubTaskState{ID: id, AgentRole: stringValue(payload, "agent_role"), Status: StatusRunning}
		}
	case "SubTaskCompleted":
		id := stringValue(payload, "subtask_id")
		if id != "" {
			subtask := p.subtasks[id]
			subtask.ID = id
			subtask.Status = StatusCompleted
			subtask.Result = stringValue(payload, "result")
			p.subtasks[id] = subtask
			p.state.Progress.CompletedSteps++
		}
	case "CodingPhaseStarted":
		p.state.CurrentPhase = stringValue(payload, "phase")
	case "GenerateThought":
		p.appendThought(event, payload)
	case "ToolExecuted":
		p.appendTool(event, payload)
	case "WaitingForHumanInput":
		p.state.Status = StatusPaused
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "waiting", Content: stringValue(payload, "prompt"), Timestamp: event.Timestamp})
	case "TaskPaused":
		p.state.Status = StatusPaused
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "paused", Content: stringValue(payload, "reason"), Timestamp: event.Timestamp})
	case "TimerScheduled":
		p.state.Status = StatusPaused
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "timer_scheduled", Content: stringValue(payload, "timer_id"), Timestamp: event.Timestamp})
	case "TimerFired":
		if p.state.Status == StatusPaused {
			p.state.Status = StatusRunning
		}
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "timer_fired", Content: stringValue(payload, "timer_id"), Timestamp: event.Timestamp})
	case "TokenUsed":
		p.applyToken(payload)
	case "TaskCompleted":
		p.state.Status = StatusCompleted
		p.state.FinalAnswer = firstNonEmpty(stringValue(payload, "final_answer"), stringValue(payload, "result"))
		totalSteps := intValue(payload, "total_steps")
		if totalSteps > 0 {
			p.state.Progress.TotalSteps = totalSteps
			p.state.Progress.CompletedSteps = totalSteps
		}
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "completed", Content: p.state.FinalAnswer, Timestamp: event.Timestamp})
	case "TaskFailed":
		p.state.Status = StatusFailed
		p.state.Error = stringValue(payload, "error")
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "failed", Content: p.state.Error, Timestamp: event.Timestamp})
	case "TaskCancelled":
		p.state.Status = StatusFailed
		p.state.Error = firstNonEmpty(stringValue(payload, "reason"), "cancelled")
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "cancelled", Content: p.state.Error, Timestamp: event.Timestamp})
	case "TaskResumed":
		p.state.Status = StatusRunning
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "resumed", Content: stringValue(payload, "note"), Timestamp: event.Timestamp})
	}
	return nil
}

func (p *TaskProjection) State() TaskView {
	out := p.state
	out.Subtasks = make([]SubTaskState, 0, len(p.subtasks))
	for _, subtask := range p.subtasks {
		out.Subtasks = append(out.Subtasks, subtask)
	}
	out.Timeline.TotalSteps = len(out.Timeline.Steps)
	if !p.firstAction.IsZero() && !p.lastAction.IsZero() && p.lastAction.After(p.firstAction) {
		out.Timeline.DurationMillis = p.lastAction.Sub(p.firstAction).Milliseconds()
	}
	return out
}

func (p *TaskProjection) Snapshot() ([]byte, error) {
	state := p.State()
	return json.Marshal(state)
}

func (p *TaskProjection) Restore(data []byte) error {
	var state TaskView
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	p.state = state
	p.subtasks = map[string]SubTaskState{}
	for _, subtask := range state.Subtasks {
		p.subtasks[subtask.ID] = subtask
	}
	return nil
}

func (p *TaskProjection) appendThought(event storage.Event, payload map[string]any) {
	result := mapValue(payload, "result")
	content := firstNonEmpty(stringValue(result, "thought"), stringValue(result, "Thought"), stringValue(payload, "thought"))
	p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "thought", Content: content, Timestamp: event.Timestamp})
}

func (p *TaskProjection) appendTool(event storage.Event, payload map[string]any) {
	result := mapValue(payload, "result")
	step := TimelineStep{
		Seq:        event.StreamSeq,
		Type:       "tool",
		ToolName:   firstNonEmpty(stringValue(payload, "tool_name"), stringValue(result, "tool_name"), stringValue(result, "ToolName")),
		Content:    firstNonEmpty(stringValue(result, "stdout"), stringValue(result, "Stdout")),
		Stdout:     firstNonEmpty(stringValue(result, "stdout"), stringValue(result, "Stdout")),
		Stderr:     firstNonEmpty(stringValue(result, "stderr"), stringValue(result, "Stderr")),
		DurationMS: int64Value(result, "duration_ms", "DurationMS"),
		Timestamp:  event.Timestamp,
	}
	p.appendTimeline(event, step)
}

func (p *TaskProjection) appendTimeline(event storage.Event, step TimelineStep) {
	if step.Timestamp.IsZero() {
		step.Timestamp = event.Timestamp
	}
	if step.Type == "thought" && p.firstAction.IsZero() {
		p.firstAction = step.Timestamp
	}
	if step.Type == "completed" || step.Type == "failed" {
		p.lastAction = step.Timestamp
	}
	p.state.Timeline.Steps = append(p.state.Timeline.Steps, step)
}

func (p *TaskProjection) applyToken(payload map[string]any) {
	prompt := int64Value(payload, "prompt_tokens")
	completion := int64Value(payload, "completion_tokens")
	total := int64Value(payload, "total_tokens")
	if total == 0 {
		total = prompt + completion
	}
	agent := firstNonEmpty(stringValue(payload, "agent"), stringValue(payload, "agent_name"))
	model := stringValue(payload, "model")
	p.state.Tokens.PromptTokens += prompt
	p.state.Tokens.CompletionTokens += completion
	p.state.Tokens.TotalTokens += total
	p.state.Tokens.TotalCostUSD += floatValue(payload, "cost_usd")
	if agent != "" {
		p.state.Tokens.ByAgent[agent] += total
	}
	if model != "" {
		p.state.Tokens.ByModel[model] += total
	}
	p.state.Tokens.BudgetExceeded = p.state.Tokens.BudgetLimit > 0 && p.state.Tokens.TotalTokens > p.state.Tokens.BudgetLimit
}

func decodePayload(raw string) (map[string]any, error) {
	out := map[string]any{}
	if raw == "" {
		return out, nil
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("decode event payload: %w", err)
	}
	return out, nil
}

func mapValue(values map[string]any, key string) map[string]any {
	if nested, ok := values[key].(map[string]any); ok {
		return nested
	}
	return map[string]any{}
}

func stringValue(values map[string]any, key string) string {
	if value, ok := values[key].(string); ok {
		return value
	}
	return ""
}

func intValue(values map[string]any, key string) int {
	return int(int64Value(values, key))
}

func int64Value(values map[string]any, keys ...string) int64 {
	for _, key := range keys {
		switch value := values[key].(type) {
		case int:
			return int64(value)
		case int64:
			return value
		case float64:
			return int64(value)
		case json.Number:
			out, _ := value.Int64()
			return out
		}
	}
	return 0
}

func floatValue(values map[string]any, key string) float64 {
	switch value := values[key].(type) {
	case float64:
		return value
	case int:
		return float64(value)
	case int64:
		return float64(value)
	case json.Number:
		out, _ := value.Float64()
		return out
	default:
		return 0
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
