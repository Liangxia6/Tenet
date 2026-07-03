package projection

import (
	"context"
	"path/filepath"
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
