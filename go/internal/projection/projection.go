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
	"github.com/tenet/orchestrator/internal/statemachine"
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
	StreamID         string          `json:"stream_id"`
	SessionID        string          `json:"session_id,omitempty"`
	Status           TaskStatus      `json:"status"`
	Query            string          `json:"query,omitempty"`
	Workspace        string          `json:"workspace,omitempty"`
	WorkflowType     string          `json:"workflow_type,omitempty"`
	CurrentTurnID    string          `json:"current_turn_id,omitempty"`
	CurrentRunID     string          `json:"current_run_id,omitempty"`
	CurrentPhase     string          `json:"current_phase,omitempty"`
	Progress         Progress        `json:"progress"`
	Turns            []TurnState     `json:"turns,omitempty"`
	Runs             []RunState      `json:"runs,omitempty"`
	LLMCalls         []LLMCallState  `json:"llm_calls,omitempty"`
	Contexts         []ContextState  `json:"contexts,omitempty"`
	ToolCalls        []ToolCallState `json:"tool_calls,omitempty"`
	Subtasks         []SubTaskState  `json:"subtasks,omitempty"`
	FinalAnswer      string          `json:"final_answer,omitempty"`
	SessionSummary   string          `json:"session_summary,omitempty"`
	WorkspaceSummary string          `json:"workspace_summary,omitempty"`
	Error            string          `json:"error,omitempty"`
	Timeline         TimelineState   `json:"timeline"`
	Tokens           TokenState      `json:"tokens"`
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

type TurnState struct {
	ID           string     `json:"id"`
	Query        string     `json:"query"`
	Status       TaskStatus `json:"status"`
	RunID        string     `json:"run_id,omitempty"`
	Result       string     `json:"result,omitempty"`
	CreatedSeq   int64      `json:"created_seq"`
	CompletedSeq int64      `json:"completed_seq,omitempty"`
}

type RunState struct {
	ID           string     `json:"id"`
	TurnID       string     `json:"turn_id"`
	Status       TaskStatus `json:"status"`
	WorkflowType string     `json:"workflow_type,omitempty"`
	StartedSeq   int64      `json:"started_seq"`
	CompletedSeq int64      `json:"completed_seq,omitempty"`
	Error        string     `json:"error,omitempty"`
}

type LLMCallState struct {
	ID               string     `json:"id"`
	SessionID        string     `json:"session_id,omitempty"`
	TurnID           string     `json:"turn_id,omitempty"`
	RunID            string     `json:"run_id,omitempty"`
	Status           TaskStatus `json:"status"`
	Provider         string     `json:"provider,omitempty"`
	Model            string     `json:"model,omitempty"`
	SystemPromptHash string     `json:"system_prompt_hash,omitempty"`
	MessageCount     int        `json:"message_count,omitempty"`
	ToolsCount       int        `json:"tools_count,omitempty"`
	InputChars       int        `json:"input_chars,omitempty"`
	PromptTokens     int64      `json:"prompt_tokens,omitempty"`
	CompletionTokens int64      `json:"completion_tokens,omitempty"`
	TotalTokens      int64      `json:"total_tokens,omitempty"`
	CostUSD          float64    `json:"cost_usd,omitempty"`
	LatencyMS        int64      `json:"latency_ms,omitempty"`
	RetryCount       int        `json:"retry_count,omitempty"`
	FinishReason     string     `json:"finish_reason,omitempty"`
	Error            string     `json:"error,omitempty"`
	StartedSeq       int64      `json:"started_seq"`
	CompletedSeq     int64      `json:"completed_seq,omitempty"`
}

type ContextState struct {
	Seq             int64      `json:"seq"`
	SessionID       string     `json:"session_id,omitempty"`
	TurnID          string     `json:"turn_id,omitempty"`
	RunID           string     `json:"run_id,omitempty"`
	MessageCount    int        `json:"message_count"`
	EstimatedTokens int        `json:"estimated_tokens"`
	InputChars      int        `json:"input_chars"`
	TokenBudget     int        `json:"token_budget,omitempty"`
	Compacted       bool       `json:"compacted"`
	OmittedCount    int        `json:"omitted_count,omitempty"`
	IncludedRefs    []EventRef `json:"included_refs,omitempty"`
	OmittedRefs     []EventRef `json:"omitted_refs,omitempty"`
}

type ToolCallState struct {
	ID           string     `json:"id"`
	SessionID    string     `json:"session_id,omitempty"`
	TurnID       string     `json:"turn_id,omitempty"`
	RunID        string     `json:"run_id,omitempty"`
	ToolName     string     `json:"tool_name"`
	Status       TaskStatus `json:"status"`
	ErrorCode    string     `json:"error_code,omitempty"`
	Error        string     `json:"error,omitempty"`
	Stdout       string     `json:"stdout,omitempty"`
	Stderr       string     `json:"stderr,omitempty"`
	ExitCode     int        `json:"exit_code,omitempty"`
	DurationMS   int64      `json:"duration_ms,omitempty"`
	TouchedFiles []string   `json:"touched_files,omitempty"`
	StartedSeq   int64      `json:"started_seq"`
	CompletedSeq int64      `json:"completed_seq,omitempty"`
}

type EventRef struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
	Role  string `json:"role,omitempty"`
	Chars int    `json:"chars,omitempty"`
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
	case "RunCompleted", "RunFailed", "RunPaused", "TaskCompleted", "TaskFailed", "TaskCancelled", "TaskPaused":
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
	turns       map[string]int
	runs        map[string]int
	llmCalls    map[string]int
	toolCalls   map[string]int
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
		turns:     map[string]int{},
		runs:      map[string]int{},
		llmCalls:  map[string]int{},
		toolCalls: map[string]int{},
		subtasks:  map[string]SubTaskState{},
	}
}

func (p *TaskProjection) Apply(event storage.Event) error {
	payload, err := decodePayload(event.Payload)
	if err != nil {
		return err
	}
	switch event.EventType {
	case "SessionCreated":
		p.state.Status = StatusRunning
		p.state.SessionID = firstNonEmpty(stringValue(payload, "session_id"), event.StreamID)
		p.state.Query = firstNonEmpty(stringValue(payload, "query"), p.state.Query)
		p.state.Workspace = firstNonEmpty(stringValue(payload, "workspace"), p.state.Workspace)
		p.state.WorkflowType = firstNonEmpty(stringValue(payload, "workflow_type"), p.state.WorkflowType)
	case "TurnCreated":
		turnID := stringValue(payload, "turn_id")
		if turnID != "" {
			p.state.Status = StatusRunning
			p.state.SessionID = firstNonEmpty(stringValue(payload, "session_id"), p.state.SessionID, event.StreamID)
			p.state.CurrentTurnID = turnID
			p.upsertTurn(TurnState{
				ID:         turnID,
				Query:      stringValue(payload, "query"),
				Status:     StatusRunning,
				CreatedSeq: event.StreamSeq,
			})
			if p.state.Query == "" {
				p.state.Query = stringValue(payload, "query")
			}
		}
	case "RunStarted":
		runID := stringValue(payload, "run_id")
		turnID := stringValue(payload, "turn_id")
		if runID != "" {
			if idx, ok := p.runs[runID]; ok {
				if err := statemachine.ValidateTransition("run", runID, string(p.state.Runs[idx].Status), string(StatusRunning)); err != nil {
					return err
				}
			}
			p.state.Status = StatusRunning
			p.state.SessionID = firstNonEmpty(stringValue(payload, "session_id"), p.state.SessionID, event.StreamID)
			p.state.CurrentTurnID = firstNonEmpty(turnID, p.state.CurrentTurnID)
			p.state.CurrentRunID = runID
			p.state.WorkflowType = firstNonEmpty(stringValue(payload, "workflow_type"), p.state.WorkflowType)
			p.upsertRun(RunState{
				ID:           runID,
				TurnID:       firstNonEmpty(turnID, p.state.CurrentTurnID),
				Status:       StatusRunning,
				WorkflowType: stringValue(payload, "workflow_type"),
				StartedSeq:   event.StreamSeq,
			})
			p.linkTurnRun(firstNonEmpty(turnID, p.state.CurrentTurnID), runID)
			p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "run_started", Content: runID, Timestamp: event.Timestamp})
		}
	case "RunCompleted":
		runID := stringValue(payload, "run_id")
		turnID := stringValue(payload, "turn_id")
		result := firstNonEmpty(stringValue(payload, "final_answer"), stringValue(payload, "result"))
		if runID != "" {
			if err := p.completeRun(runID, StatusCompleted, event.StreamSeq, ""); err != nil {
				return err
			}
			p.completeTurn(firstNonEmpty(turnID, p.runTurn(runID), p.state.CurrentTurnID), StatusCompleted, runID, result, event.StreamSeq)
		}
		p.state.Status = StatusCompleted
		p.state.FinalAnswer = firstNonEmpty(result, p.state.FinalAnswer)
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "run_completed", Content: p.state.FinalAnswer, Timestamp: event.Timestamp})
	case "RunFailed":
		runID := stringValue(payload, "run_id")
		turnID := stringValue(payload, "turn_id")
		errText := stringValue(payload, "error")
		if runID != "" {
			if err := p.completeRun(runID, StatusFailed, event.StreamSeq, errText); err != nil {
				return err
			}
			p.completeTurn(firstNonEmpty(turnID, p.runTurn(runID), p.state.CurrentTurnID), StatusFailed, runID, errText, event.StreamSeq)
		}
		p.state.Status = StatusFailed
		p.state.Error = errText
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "run_failed", Content: errText, Timestamp: event.Timestamp})
	case "RunPaused":
		runID := stringValue(payload, "run_id")
		turnID := stringValue(payload, "turn_id")
		reason := stringValue(payload, "reason")
		if runID != "" {
			if err := p.completeRun(runID, StatusPaused, event.StreamSeq, reason); err != nil {
				return err
			}
			p.completeTurn(firstNonEmpty(turnID, p.runTurn(runID), p.state.CurrentTurnID), StatusPaused, runID, reason, event.StreamSeq)
		}
		p.state.Status = StatusPaused
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "run_paused", Content: reason, Timestamp: event.Timestamp})
	case "LLMCallStarted":
		callID := stringValue(payload, "call_id")
		if callID != "" {
			p.upsertLLMCall(LLMCallState{
				ID:               callID,
				SessionID:        stringValue(payload, "session_id"),
				TurnID:           stringValue(payload, "turn_id"),
				RunID:            stringValue(payload, "run_id"),
				Status:           StatusRunning,
				Provider:         stringValue(payload, "provider"),
				Model:            stringValue(payload, "model"),
				SystemPromptHash: stringValue(payload, "system_prompt_hash"),
				MessageCount:     intValue(payload, "message_count"),
				ToolsCount:       intValue(payload, "tools_count"),
				InputChars:       intValue(payload, "input_chars"),
				StartedSeq:       event.StreamSeq,
			})
			p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "llm_started", Content: callID, Timestamp: event.Timestamp})
		}
	case "LLMCallCompleted":
		callID := stringValue(payload, "call_id")
		if callID != "" {
			p.completeLLMCall(callID, StatusCompleted, event.StreamSeq, payload)
			p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "llm_completed", Content: callID, DurationMS: int64Value(payload, "latency_ms"), Timestamp: event.Timestamp})
		}
	case "LLMCallFailed":
		callID := stringValue(payload, "call_id")
		if callID != "" {
			p.completeLLMCall(callID, StatusFailed, event.StreamSeq, payload)
			p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "llm_failed", Content: stringValue(payload, "error"), DurationMS: int64Value(payload, "latency_ms"), Timestamp: event.Timestamp})
		}
	case "ContextAssembled":
		state := ContextState{
			Seq:             event.StreamSeq,
			SessionID:       stringValue(payload, "session_id"),
			TurnID:          stringValue(payload, "turn_id"),
			RunID:           stringValue(payload, "run_id"),
			MessageCount:    intValue(payload, "message_count"),
			EstimatedTokens: intValue(payload, "estimated_tokens"),
			InputChars:      intValue(payload, "input_chars"),
			TokenBudget:     intValue(payload, "token_budget"),
			Compacted:       boolValue(payload, "compacted"),
			OmittedCount:    intValue(payload, "omitted_count"),
			IncludedRefs:    eventRefsValue(payload, "included_refs"),
			OmittedRefs:     eventRefsValue(payload, "omitted_refs"),
		}
		p.state.Contexts = append(p.state.Contexts, state)
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "context_assembled", Content: fmt.Sprintf("messages=%d tokens~=%d", state.MessageCount, state.EstimatedTokens), Timestamp: event.Timestamp})
	case "ContextCompacted":
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "context_compacted", Content: fmt.Sprintf("omitted=%d tokens~=%d", intValue(payload, "omitted_count"), intValue(payload, "estimated_tokens")), Timestamp: event.Timestamp})
	case "ToolCallStarted":
		callID := stringValue(payload, "tool_call_id")
		if callID != "" {
			p.upsertToolCall(ToolCallState{
				ID:         callID,
				SessionID:  stringValue(payload, "session_id"),
				TurnID:     stringValue(payload, "turn_id"),
				RunID:      stringValue(payload, "run_id"),
				ToolName:   stringValue(payload, "tool_name"),
				Status:     StatusRunning,
				StartedSeq: event.StreamSeq,
			})
			p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "tool_started", ToolName: stringValue(payload, "tool_name"), Content: callID, Timestamp: event.Timestamp})
		}
	case "ToolCallCompleted":
		callID := stringValue(payload, "tool_call_id")
		if callID != "" {
			p.completeToolCall(callID, StatusCompleted, event.StreamSeq, payload)
			p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "tool_completed", ToolName: stringValue(payload, "tool_name"), Stdout: stringValue(payload, "stdout"), Stderr: stringValue(payload, "stderr"), DurationMS: int64Value(payload, "duration_ms"), Timestamp: event.Timestamp})
		}
	case "ToolCallFailed":
		callID := stringValue(payload, "tool_call_id")
		if callID != "" {
			p.completeToolCall(callID, StatusFailed, event.StreamSeq, payload)
			p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "tool_failed", ToolName: stringValue(payload, "tool_name"), Content: firstNonEmpty(stringValue(payload, "error_code"), stringValue(payload, "error")), Stderr: stringValue(payload, "stderr"), DurationMS: int64Value(payload, "duration_ms"), Timestamp: event.Timestamp})
		}
	case "TaskCreated", "TaskStarted":
		p.state.Status = StatusRunning
		p.state.SessionID = firstNonEmpty(stringValue(payload, "session_id"), p.state.SessionID, event.StreamID)
		p.state.CurrentTurnID = firstNonEmpty(stringValue(payload, "turn_id"), p.state.CurrentTurnID)
		p.state.CurrentRunID = firstNonEmpty(stringValue(payload, "run_id"), p.state.CurrentRunID)
		p.state.Query = firstNonEmpty(stringValue(payload, "query"), p.state.Query)
		p.state.Workspace = firstNonEmpty(stringValue(payload, "workspace"), p.state.Workspace)
		p.state.WorkflowType = firstNonEmpty(stringValue(payload, "workflow_type"), p.state.WorkflowType)
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
	case "TokenBudgetExceeded":
		p.state.Tokens.BudgetExceeded = true
		p.state.Tokens.BudgetLimit = int64Value(payload, "budget_limit")
		p.state.Tokens.TotalTokens = firstNonZeroInt64(int64Value(payload, "used_tokens"), p.state.Tokens.TotalTokens)
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "token_budget_exceeded", Content: fmt.Sprintf("used=%d budget=%d", int64Value(payload, "used_tokens"), int64Value(payload, "budget_limit")), Timestamp: event.Timestamp})
	case "TaskCompleted":
		p.state.Status = StatusCompleted
		p.state.FinalAnswer = firstNonEmpty(stringValue(payload, "final_answer"), stringValue(payload, "result"))
		runID := firstNonEmpty(stringValue(payload, "run_id"), p.state.CurrentRunID)
		turnID := firstNonEmpty(stringValue(payload, "turn_id"), p.runTurn(runID), p.state.CurrentTurnID)
		if runID != "" {
			if err := p.completeRun(runID, StatusCompleted, event.StreamSeq, ""); err != nil {
				return err
			}
		}
		if turnID != "" {
			p.completeTurn(turnID, StatusCompleted, runID, p.state.FinalAnswer, event.StreamSeq)
		}
		totalSteps := intValue(payload, "total_steps")
		if totalSteps > 0 {
			p.state.Progress.TotalSteps = totalSteps
			p.state.Progress.CompletedSteps = totalSteps
		}
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "completed", Content: p.state.FinalAnswer, Timestamp: event.Timestamp})
	case "TaskFailed":
		p.state.Status = StatusFailed
		p.state.Error = stringValue(payload, "error")
		runID := firstNonEmpty(stringValue(payload, "run_id"), p.state.CurrentRunID)
		turnID := firstNonEmpty(stringValue(payload, "turn_id"), p.runTurn(runID), p.state.CurrentTurnID)
		if runID != "" {
			if err := p.completeRun(runID, StatusFailed, event.StreamSeq, p.state.Error); err != nil {
				return err
			}
		}
		if turnID != "" {
			p.completeTurn(turnID, StatusFailed, runID, p.state.Error, event.StreamSeq)
		}
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "failed", Content: p.state.Error, Timestamp: event.Timestamp})
	case "TaskCancelled":
		p.state.Status = StatusFailed
		p.state.Error = firstNonEmpty(stringValue(payload, "reason"), "cancelled")
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "cancelled", Content: p.state.Error, Timestamp: event.Timestamp})
	case "TaskResumed":
		p.state.Status = StatusRunning
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "resumed", Content: stringValue(payload, "note"), Timestamp: event.Timestamp})
	case "WorkspaceCheckpointCreated":
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "workspace_checkpoint", Content: firstNonEmpty(stringValue(payload, "snapshot_ref"), stringValue(payload, "workspace")), Timestamp: event.Timestamp})
	case "WorkspaceCheckpointFailed":
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "workspace_checkpoint_failed", Content: stringValue(payload, "error"), Timestamp: event.Timestamp})
	case "SessionSummaryCreated":
		p.state.SessionSummary = stringValue(payload, "summary")
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "session_summary", Content: p.state.SessionSummary, Timestamp: event.Timestamp})
	case "WorkspaceSummaryCreated":
		p.state.WorkspaceSummary = stringValue(payload, "summary")
		p.appendTimeline(event, TimelineStep{Seq: event.StreamSeq, Type: "workspace_summary", Content: p.state.WorkspaceSummary, Timestamp: event.Timestamp})
	}
	return nil
}

func (p *TaskProjection) State() TaskView {
	out := p.state
	out.Turns = append([]TurnState(nil), p.state.Turns...)
	out.Runs = append([]RunState(nil), p.state.Runs...)
	out.LLMCalls = append([]LLMCallState(nil), p.state.LLMCalls...)
	out.Contexts = append([]ContextState(nil), p.state.Contexts...)
	out.ToolCalls = append([]ToolCallState(nil), p.state.ToolCalls...)
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
	p.turns = map[string]int{}
	for i, turn := range state.Turns {
		p.turns[turn.ID] = i
	}
	p.runs = map[string]int{}
	for i, run := range state.Runs {
		p.runs[run.ID] = i
	}
	p.llmCalls = map[string]int{}
	for i, call := range state.LLMCalls {
		p.llmCalls[call.ID] = i
	}
	p.toolCalls = map[string]int{}
	for i, call := range state.ToolCalls {
		p.toolCalls[call.ID] = i
	}
	p.subtasks = map[string]SubTaskState{}
	for _, subtask := range state.Subtasks {
		p.subtasks[subtask.ID] = subtask
	}
	return nil
}

func (p *TaskProjection) upsertTurn(turn TurnState) {
	if turn.ID == "" {
		return
	}
	if existing, ok := p.turns[turn.ID]; ok {
		current := p.state.Turns[existing]
		if turn.Query != "" {
			current.Query = turn.Query
		}
		if turn.Status != "" {
			current.Status = turn.Status
		}
		if turn.RunID != "" {
			current.RunID = turn.RunID
		}
		if turn.Result != "" {
			current.Result = turn.Result
		}
		if turn.CreatedSeq > 0 && current.CreatedSeq == 0 {
			current.CreatedSeq = turn.CreatedSeq
		}
		if turn.CompletedSeq > 0 {
			current.CompletedSeq = turn.CompletedSeq
		}
		p.state.Turns[existing] = current
		return
	}
	p.turns[turn.ID] = len(p.state.Turns)
	p.state.Turns = append(p.state.Turns, turn)
}

func (p *TaskProjection) upsertRun(run RunState) {
	if run.ID == "" {
		return
	}
	if existing, ok := p.runs[run.ID]; ok {
		current := p.state.Runs[existing]
		if run.TurnID != "" {
			current.TurnID = run.TurnID
		}
		if run.Status != "" {
			current.Status = run.Status
		}
		if run.WorkflowType != "" {
			current.WorkflowType = run.WorkflowType
		}
		if run.StartedSeq > 0 && current.StartedSeq == 0 {
			current.StartedSeq = run.StartedSeq
		}
		if run.CompletedSeq > 0 {
			current.CompletedSeq = run.CompletedSeq
		}
		if run.Error != "" {
			current.Error = run.Error
		}
		p.state.Runs[existing] = current
		return
	}
	p.runs[run.ID] = len(p.state.Runs)
	p.state.Runs = append(p.state.Runs, run)
}

func (p *TaskProjection) linkTurnRun(turnID, runID string) {
	if turnID == "" || runID == "" {
		return
	}
	idx, ok := p.turns[turnID]
	if !ok {
		p.upsertTurn(TurnState{ID: turnID, Status: StatusRunning})
		idx = p.turns[turnID]
	}
	turn := p.state.Turns[idx]
	turn.RunID = runID
	turn.Status = StatusRunning
	p.state.Turns[idx] = turn
}

func (p *TaskProjection) completeRun(runID string, status TaskStatus, seq int64, errText string) error {
	idx, ok := p.runs[runID]
	if !ok {
		p.upsertRun(RunState{ID: runID, TurnID: p.state.CurrentTurnID, Status: status, CompletedSeq: seq, Error: errText})
		return nil
	}
	currentStatus := p.state.Runs[idx].Status
	if err := statemachine.ValidateTransition("run", runID, string(currentStatus), string(status)); err != nil {
		return err
	}
	run := p.state.Runs[idx]
	run.Status = status
	if seq > 0 {
		run.CompletedSeq = seq
	}
	if errText != "" {
		run.Error = errText
	}
	p.state.Runs[idx] = run
	return nil
}

func (p *TaskProjection) completeTurn(turnID string, status TaskStatus, runID, result string, seq int64) {
	if turnID == "" {
		return
	}
	idx, ok := p.turns[turnID]
	if !ok {
		p.upsertTurn(TurnState{ID: turnID, Status: status, RunID: runID, Result: result, CompletedSeq: seq})
		return
	}
	turn := p.state.Turns[idx]
	turn.Status = status
	if runID != "" {
		turn.RunID = runID
	}
	if result != "" {
		turn.Result = result
	}
	if seq > 0 {
		turn.CompletedSeq = seq
	}
	p.state.Turns[idx] = turn
}

func (p *TaskProjection) runTurn(runID string) string {
	if runID == "" {
		return ""
	}
	idx, ok := p.runs[runID]
	if !ok {
		return ""
	}
	return p.state.Runs[idx].TurnID
}

func (p *TaskProjection) upsertLLMCall(call LLMCallState) {
	if call.ID == "" {
		return
	}
	if existing, ok := p.llmCalls[call.ID]; ok {
		current := p.state.LLMCalls[existing]
		if call.SessionID != "" {
			current.SessionID = call.SessionID
		}
		if call.TurnID != "" {
			current.TurnID = call.TurnID
		}
		if call.RunID != "" {
			current.RunID = call.RunID
		}
		if call.Status != "" {
			current.Status = call.Status
		}
		if call.Provider != "" {
			current.Provider = call.Provider
		}
		if call.Model != "" {
			current.Model = call.Model
		}
		if call.SystemPromptHash != "" {
			current.SystemPromptHash = call.SystemPromptHash
		}
		if call.MessageCount > 0 {
			current.MessageCount = call.MessageCount
		}
		if call.ToolsCount > 0 {
			current.ToolsCount = call.ToolsCount
		}
		if call.InputChars > 0 {
			current.InputChars = call.InputChars
		}
		if call.StartedSeq > 0 && current.StartedSeq == 0 {
			current.StartedSeq = call.StartedSeq
		}
		p.state.LLMCalls[existing] = current
		return
	}
	p.llmCalls[call.ID] = len(p.state.LLMCalls)
	p.state.LLMCalls = append(p.state.LLMCalls, call)
}

func (p *TaskProjection) completeLLMCall(callID string, status TaskStatus, seq int64, payload map[string]any) {
	idx, ok := p.llmCalls[callID]
	if !ok {
		p.upsertLLMCall(LLMCallState{ID: callID, Status: status, CompletedSeq: seq})
		idx = p.llmCalls[callID]
	}
	call := p.state.LLMCalls[idx]
	call.Status = status
	call.CompletedSeq = seq
	call.FinishReason = firstNonEmpty(stringValue(payload, "finish_reason"), call.FinishReason)
	call.Error = firstNonEmpty(stringValue(payload, "error"), call.Error)
	call.LatencyMS = firstNonZeroInt64(int64Value(payload, "latency_ms"), call.LatencyMS)
	call.RetryCount = firstNonZeroInt(intValue(payload, "retry_count"), call.RetryCount)
	call.PromptTokens = firstNonZeroInt64(int64Value(payload, "prompt_tokens"), call.PromptTokens)
	call.CompletionTokens = firstNonZeroInt64(int64Value(payload, "completion_tokens"), call.CompletionTokens)
	call.TotalTokens = firstNonZeroInt64(int64Value(payload, "total_tokens"), call.TotalTokens)
	call.CostUSD = firstNonZeroFloat(floatValue(payload, "cost_usd"), call.CostUSD)
	p.state.LLMCalls[idx] = call
}

func (p *TaskProjection) upsertToolCall(call ToolCallState) {
	if call.ID == "" {
		return
	}
	if existing, ok := p.toolCalls[call.ID]; ok {
		current := p.state.ToolCalls[existing]
		if call.SessionID != "" {
			current.SessionID = call.SessionID
		}
		if call.TurnID != "" {
			current.TurnID = call.TurnID
		}
		if call.RunID != "" {
			current.RunID = call.RunID
		}
		if call.ToolName != "" {
			current.ToolName = call.ToolName
		}
		if call.Status != "" {
			current.Status = call.Status
		}
		if call.StartedSeq > 0 && current.StartedSeq == 0 {
			current.StartedSeq = call.StartedSeq
		}
		p.state.ToolCalls[existing] = current
		return
	}
	p.toolCalls[call.ID] = len(p.state.ToolCalls)
	p.state.ToolCalls = append(p.state.ToolCalls, call)
}

func (p *TaskProjection) completeToolCall(callID string, status TaskStatus, seq int64, payload map[string]any) {
	idx, ok := p.toolCalls[callID]
	if !ok {
		p.upsertToolCall(ToolCallState{ID: callID, Status: status, CompletedSeq: seq})
		idx = p.toolCalls[callID]
	}
	call := p.state.ToolCalls[idx]
	call.Status = status
	call.CompletedSeq = seq
	call.ToolName = firstNonEmpty(stringValue(payload, "tool_name"), call.ToolName)
	call.ErrorCode = firstNonEmpty(stringValue(payload, "error_code"), call.ErrorCode)
	call.Error = firstNonEmpty(stringValue(payload, "error"), call.Error)
	call.Stdout = firstNonEmpty(stringValue(payload, "stdout"), call.Stdout)
	call.Stderr = firstNonEmpty(stringValue(payload, "stderr"), call.Stderr)
	call.ExitCode = firstNonZeroInt(intValue(payload, "exit_code"), call.ExitCode)
	call.DurationMS = firstNonZeroInt64(int64Value(payload, "duration_ms"), call.DurationMS)
	if touchedFiles := stringSliceValue(payload, "touched_files"); len(touchedFiles) > 0 {
		call.TouchedFiles = touchedFiles
	}
	p.state.ToolCalls[idx] = call
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

func boolValue(values map[string]any, key string) bool {
	if value, ok := values[key].(bool); ok {
		return value
	}
	return false
}

func eventRefsValue(values map[string]any, key string) []EventRef {
	raw, ok := values[key].([]any)
	if !ok {
		return nil
	}
	refs := make([]EventRef, 0, len(raw))
	for _, item := range raw {
		value, ok := item.(map[string]any)
		if !ok {
			continue
		}
		refs = append(refs, EventRef{
			Type:  stringValue(value, "type"),
			Index: intValue(value, "index"),
			Role:  stringValue(value, "role"),
			Chars: intValue(value, "chars"),
		})
	}
	return refs
}

func stringSliceValue(values map[string]any, key string) []string {
	raw, ok := values[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		value, ok := item.(string)
		if ok && value != "" {
			out = append(out, value)
		}
	}
	return out
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

func firstNonZeroInt(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstNonZeroFloat(values ...float64) float64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}
