package projection

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/storage"
)

func TestEngineProjectsTaskTimelineAndTokens(t *testing.T) {
	ctx := context.Background()
	store := openProjectionStore(t)
	defer store.Close()
	_, err := store.AppendEvents(ctx, []storage.AppendEvent{
		{StreamID: "task:project", EventType: "TaskCreated", Payload: map[string]any{
			"query": "summarize", "workspace": "/tmp/work", "workflow_type": "simple",
		}},
		{StreamID: "task:project", EventType: "GenerateThought", Payload: map[string]any{
			"result": map[string]any{"Thought": "I can answer.", "IsFinal": true},
		}},
		{StreamID: "task:project", EventType: "TokenUsed", Payload: map[string]any{
			"agent": "default", "model": "echo", "prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14, "cost_usd": 0.02,
		}},
		{StreamID: "task:project", EventType: "TaskCompleted", Payload: map[string]any{
			"final_answer": "done", "total_steps": 1,
		}},
	})
	if err != nil {
		t.Fatalf("append events: %v", err)
	}

	cfg := config.Default()
	cfg.Agent.DefaultTokenBudget = 100
	view, err := NewEngine(store, cfg).ProjectTask("task:project")
	if err != nil {
		t.Fatalf("ProjectTask: %v", err)
	}
	if view.Status != StatusCompleted {
		t.Fatalf("status = %s, want COMPLETED", view.Status)
	}
	if view.Query != "summarize" || view.WorkflowType != "simple" {
		t.Fatalf("view = %+v", view)
	}
	if view.FinalAnswer != "done" {
		t.Fatalf("final answer = %q", view.FinalAnswer)
	}
	if view.Timeline.TotalSteps != 2 {
		t.Fatalf("timeline steps = %d, want 2", view.Timeline.TotalSteps)
	}
	if view.Timeline.Steps[0].Type != "thought" || view.Timeline.Steps[0].Content != "I can answer." {
		t.Fatalf("thought step = %+v", view.Timeline.Steps[0])
	}
	if view.Tokens.TotalTokens != 14 || view.Tokens.ByAgent["default"] != 14 || view.Tokens.ByModel["echo"] != 14 {
		t.Fatalf("tokens = %+v", view.Tokens)
	}
	if view.Tokens.BudgetExceeded {
		t.Fatalf("budget should not be exceeded")
	}
}

func TestTaskProjectionSnapshotRestore(t *testing.T) {
	projection := NewTaskProjection("task:snapshot", 10)
	if err := projection.Apply(storage.Event{
		StreamID:  "task:snapshot",
		StreamSeq: 1,
		EventType: "TaskCreated",
		Payload:   `{"query":"hello"}`,
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	data, err := projection.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	restored := NewTaskProjection("task:snapshot", 10)
	if err := restored.Restore(data); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restored.State().Query != "hello" {
		t.Fatalf("restored = %+v", restored.State())
	}
}

func TestTaskProjectionSessionTurnsAndRuns(t *testing.T) {
	projection := NewTaskProjection("task:session", 100)
	events := []storage.Event{
		{StreamID: "task:session", StreamSeq: 1, EventType: "SessionCreated", Payload: `{"session_id":"task:session","query":"first","workspace":"/tmp/w","workflow_type":"simple"}`},
		{StreamID: "task:session", StreamSeq: 2, EventType: "TurnCreated", Payload: `{"session_id":"task:session","turn_id":"turn:1","query":"first"}`},
		{StreamID: "task:session", StreamSeq: 3, EventType: "RunStarted", Payload: `{"session_id":"task:session","turn_id":"turn:1","run_id":"run:1","workflow_type":"simple"}`},
		{StreamID: "task:session", StreamSeq: 4, EventType: "RunCompleted", Payload: `{"session_id":"task:session","turn_id":"turn:1","run_id":"run:1","final_answer":"answer one"}`},
		{StreamID: "task:session", StreamSeq: 5, EventType: "TurnCreated", Payload: `{"session_id":"task:session","turn_id":"turn:2","query":"second"}`},
		{StreamID: "task:session", StreamSeq: 6, EventType: "RunStarted", Payload: `{"session_id":"task:session","turn_id":"turn:2","run_id":"run:2","workflow_type":"react"}`},
		{StreamID: "task:session", StreamSeq: 7, EventType: "RunCompleted", Payload: `{"session_id":"task:session","turn_id":"turn:2","run_id":"run:2","final_answer":"answer two"}`},
	}
	for _, event := range events {
		if err := projection.Apply(event); err != nil {
			t.Fatalf("apply %s: %v", event.EventType, err)
		}
	}
	view := projection.State()
	if view.SessionID != "task:session" || view.CurrentTurnID != "turn:2" || view.CurrentRunID != "run:2" {
		t.Fatalf("view ids = %+v", view)
	}
	if view.Status != StatusCompleted || view.FinalAnswer != "answer two" {
		t.Fatalf("status/final = %+v", view)
	}
	if len(view.Turns) != 2 || len(view.Runs) != 2 {
		t.Fatalf("turns=%+v runs=%+v", view.Turns, view.Runs)
	}
	if view.Turns[0].Status != StatusCompleted || view.Turns[0].RunID != "run:1" || view.Turns[0].Result != "answer one" {
		t.Fatalf("first turn = %+v", view.Turns[0])
	}
	if view.Runs[1].Status != StatusCompleted || view.Runs[1].TurnID != "turn:2" || view.Runs[1].WorkflowType != "react" {
		t.Fatalf("second run = %+v", view.Runs[1])
	}

	data, err := projection.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	restored := NewTaskProjection("task:session", 100)
	if err := restored.Restore(data); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got := restored.State(); len(got.Turns) != 2 || len(got.Runs) != 2 || got.FinalAnswer != "answer two" {
		t.Fatalf("restored = %+v", got)
	}
}

func TestTaskProjectionRejectsInvalidRunTransition(t *testing.T) {
	projection := NewTaskProjection("task:invalid-run", 100)
	events := []storage.Event{
		{StreamID: "task:invalid-run", StreamSeq: 1, EventType: "RunStarted", Payload: `{"run_id":"run:1","turn_id":"turn:1"}`},
		{StreamID: "task:invalid-run", StreamSeq: 2, EventType: "RunCompleted", Payload: `{"run_id":"run:1","turn_id":"turn:1","final_answer":"done"}`},
	}
	for _, event := range events {
		if err := projection.Apply(event); err != nil {
			t.Fatalf("apply %s: %v", event.EventType, err)
		}
	}
	err := projection.Apply(storage.Event{StreamID: "task:invalid-run", StreamSeq: 3, EventType: "RunStarted", Payload: `{"run_id":"run:1","turn_id":"turn:1"}`})
	if err == nil {
		t.Fatalf("expected invalid transition error")
	}
	if !strings.Contains(err.Error(), "invalid run state transition") {
		t.Fatalf("error = %v", err)
	}
}

func TestTaskProjectionLLMCalls(t *testing.T) {
	projection := NewTaskProjection("task:llm", 100)
	events := []storage.Event{
		{StreamID: "task:llm", StreamSeq: 1, EventType: "LLMCallStarted", Payload: `{"session_id":"task:llm","turn_id":"turn:1","run_id":"run:1","call_id":"llm:1","provider":"deepseek","model":"deepseek-chat","system_prompt_hash":"abc","message_count":3,"tools_count":2,"input_chars":120}`},
		{StreamID: "task:llm", StreamSeq: 2, EventType: "LLMCallCompleted", Payload: `{"call_id":"llm:1","finish_reason":"stop","latency_ms":42,"retry_count":1,"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"cost_usd":0.001}`},
	}
	for _, event := range events {
		if err := projection.Apply(event); err != nil {
			t.Fatalf("apply %s: %v", event.EventType, err)
		}
	}
	view := projection.State()
	if len(view.LLMCalls) != 1 {
		t.Fatalf("llm calls = %+v", view.LLMCalls)
	}
	call := view.LLMCalls[0]
	if call.Status != StatusCompleted || call.Provider != "deepseek" || call.TotalTokens != 15 || call.LatencyMS != 42 {
		t.Fatalf("call = %+v", call)
	}
	if view.Timeline.TotalSteps != 2 {
		t.Fatalf("timeline = %+v", view.Timeline)
	}
}

func TestTaskProjectionTokenBudgetExceeded(t *testing.T) {
	projection := NewTaskProjection("task:budget", 5)
	events := []storage.Event{
		{StreamID: "task:budget", StreamSeq: 1, EventType: "TokenUsed", Payload: `{"total_tokens":4}`},
		{StreamID: "task:budget", StreamSeq: 2, EventType: "TokenBudgetExceeded", Payload: `{"used_tokens":8,"budget_limit":5}`},
	}
	for _, event := range events {
		if err := projection.Apply(event); err != nil {
			t.Fatalf("apply %s: %v", event.EventType, err)
		}
	}
	view := projection.State()
	if !view.Tokens.BudgetExceeded || view.Tokens.TotalTokens != 8 || view.Tokens.BudgetLimit != 5 {
		t.Fatalf("tokens = %+v", view.Tokens)
	}
}

func TestTaskProjectionSummaryMemory(t *testing.T) {
	projection := NewTaskProjection("task:summary", 100)
	events := []storage.Event{
		{StreamID: "task:summary", StreamSeq: 1, EventType: "SessionSummaryCreated", Payload: `{"summary":"session memory"}`},
		{StreamID: "task:summary", StreamSeq: 2, EventType: "WorkspaceSummaryCreated", Payload: `{"summary":"workspace memory"}`},
	}
	for _, event := range events {
		if err := projection.Apply(event); err != nil {
			t.Fatalf("apply %s: %v", event.EventType, err)
		}
	}
	view := projection.State()
	if view.SessionSummary != "session memory" || view.WorkspaceSummary != "workspace memory" {
		t.Fatalf("view = %+v", view)
	}
}

func TestTaskProjectionContexts(t *testing.T) {
	projection := NewTaskProjection("task:context", 100)
	event := storage.Event{
		StreamID:  "task:context",
		StreamSeq: 1,
		EventType: "ContextAssembled",
		Payload:   `{"session_id":"task:context","turn_id":"turn:1","run_id":"run:1","message_count":2,"estimated_tokens":20,"input_chars":80,"token_budget":100,"compacted":true,"omitted_count":1,"included_refs":[{"type":"message","index":1,"role":"user","chars":12}],"omitted_refs":[{"type":"message","index":0,"role":"assistant","chars":30}]}`,
	}
	if err := projection.Apply(event); err != nil {
		t.Fatalf("apply: %v", err)
	}
	view := projection.State()
	if len(view.Contexts) != 1 {
		t.Fatalf("contexts = %+v", view.Contexts)
	}
	ctxState := view.Contexts[0]
	if !ctxState.Compacted || ctxState.OmittedCount != 1 || ctxState.EstimatedTokens != 20 {
		t.Fatalf("context state = %+v", ctxState)
	}
	if len(ctxState.IncludedRefs) != 1 || ctxState.IncludedRefs[0].Role != "user" {
		t.Fatalf("included refs = %+v", ctxState.IncludedRefs)
	}
}

func TestTaskProjectionToolCalls(t *testing.T) {
	projection := NewTaskProjection("task:tool", 100)
	events := []storage.Event{
		{StreamID: "task:tool", StreamSeq: 1, EventType: "ToolCallStarted", Payload: `{"session_id":"task:tool","turn_id":"turn:1","run_id":"run:1","tool_call_id":"call:1","tool_name":"read_file"}`},
		{StreamID: "task:tool", StreamSeq: 2, EventType: "ToolCallFailed", Payload: `{"tool_call_id":"call:1","tool_name":"read_file","error_code":"PATH_ESCAPE","error":"outside workspace","stderr":"outside workspace","exit_code":1,"duration_ms":7,"touched_files":["notes.txt"]}`},
	}
	for _, event := range events {
		if err := projection.Apply(event); err != nil {
			t.Fatalf("apply %s: %v", event.EventType, err)
		}
	}
	view := projection.State()
	if len(view.ToolCalls) != 1 {
		t.Fatalf("tool calls = %+v", view.ToolCalls)
	}
	call := view.ToolCalls[0]
	if call.Status != StatusFailed || call.ErrorCode != "PATH_ESCAPE" || call.ExitCode != 1 || call.DurationMS != 7 {
		t.Fatalf("call = %+v", call)
	}
	if len(call.TouchedFiles) != 1 || call.TouchedFiles[0] != "notes.txt" {
		t.Fatalf("touched files = %+v", call.TouchedFiles)
	}
}

func TestTaskProjectionTimerPauseResume(t *testing.T) {
	projection := NewTaskProjection("task:timer", 10)
	events := []storage.Event{
		{StreamID: "task:timer", StreamSeq: 1, EventType: "TaskStarted", Payload: `{"query":"wait"}`},
		{StreamID: "task:timer", StreamSeq: 2, EventType: "TaskPaused", Payload: `{"reason":"human review"}`},
		{StreamID: "task:timer", StreamSeq: 3, EventType: "TimerScheduled", Payload: `{"timer_id":"resume:1"}`},
		{StreamID: "task:timer", StreamSeq: 4, EventType: "TimerFired", Payload: `{"timer_id":"resume:1"}`},
		{StreamID: "task:timer", StreamSeq: 5, EventType: "TaskResumed", Payload: `{"note":"continue"}`},
	}
	for _, event := range events {
		if err := projection.Apply(event); err != nil {
			t.Fatalf("apply %s: %v", event.EventType, err)
		}
	}
	view := projection.State()
	if view.Status != StatusRunning {
		t.Fatalf("status = %s, want RUNNING", view.Status)
	}
	if view.Timeline.TotalSteps != 4 {
		t.Fatalf("timeline steps = %d, want 4", view.Timeline.TotalSteps)
	}
}

func TestEngineApplyCachesProjection(t *testing.T) {
	store := openProjectionStore(t)
	defer store.Close()
	engine := NewEngine(store, config.Default())
	if err := engine.Apply(storage.Event{StreamID: "task:cached", StreamSeq: 1, EventType: "TaskCreated", Payload: `{"query":"cached"}`}); err != nil {
		t.Fatalf("Apply created: %v", err)
	}
	if err := engine.Apply(storage.Event{StreamID: "task:cached", StreamSeq: 2, EventType: "TaskCompleted", Payload: `{"final_answer":"done","total_steps":1}`}); err != nil {
		t.Fatalf("Apply completed: %v", err)
	}
	view, err := engine.ProjectTask("task:cached")
	if err != nil {
		t.Fatalf("ProjectTask: %v", err)
	}
	if view.Status != StatusCompleted || view.FinalAnswer != "done" {
		t.Fatalf("view = %+v", view)
	}
}

func TestEnginePersistsAndRestoresProjectionSnapshot(t *testing.T) {
	store := openProjectionStore(t)
	defer store.Close()
	ctx := context.Background()
	if _, err := store.AppendEvents(ctx, []storage.AppendEvent{
		{StreamID: "task:persisted", EventType: "TaskStarted", Payload: map[string]any{"query": "persist me"}},
		{StreamID: "task:persisted", EventType: "TimerScheduled", Payload: map[string]any{"timer_id": "t1"}},
		{StreamID: "task:persisted", EventType: "TimerFired", Payload: map[string]any{"timer_id": "t1"}},
		{StreamID: "task:persisted", EventType: "TaskCompleted", Payload: map[string]any{"final_answer": "done", "total_steps": 1}},
	}); err != nil {
		t.Fatalf("append events: %v", err)
	}
	cfg := config.Default()
	cfg.Workflow.SnapshotEventInterval = 2
	engine := NewEngine(store, cfg)
	events, err := store.Read("task:persisted", 1)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	for _, event := range events[:2] {
		if err := engine.Apply(event); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	snapshot, err := store.LatestProjectionSnapshot("task:persisted")
	if err != nil {
		t.Fatalf("LatestProjectionSnapshot: %v", err)
	}
	if snapshot.StreamSeq != 2 {
		t.Fatalf("snapshot seq = %d, want 2", snapshot.StreamSeq)
	}

	restored := NewEngine(store, cfg)
	view, err := restored.ProjectTask("task:persisted")
	if err != nil {
		t.Fatalf("ProjectTask: %v", err)
	}
	if view.Status != StatusCompleted || view.FinalAnswer != "done" {
		t.Fatalf("view = %+v", view)
	}
}

func openProjectionStore(t *testing.T) *storage.SQLiteStore {
	t.Helper()
	store, err := storage.Open(filepath.Join(t.TempDir(), "projection.db"), storage.SQLiteOptions{QueueSize: 16})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}
