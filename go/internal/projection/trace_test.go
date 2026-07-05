package projection

import (
	"context"
	"testing"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/storage"
)

func TestBuildTraceViewCreatesStructuredSpanTree(t *testing.T) {
	now := time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC)
	events := []storage.Event{
		{StreamID: "task:trace", StreamSeq: 1, EventType: "TurnCreated", Payload: `{"session_id":"task:trace","turn_id":"turn:1","query":"fix bug"}`, Timestamp: now},
		{StreamID: "task:trace", StreamSeq: 2, EventType: "RunStarted", Payload: `{"session_id":"task:trace","turn_id":"turn:1","run_id":"run:1","workflow_type":"coding"}`, Timestamp: now.Add(time.Second)},
		{StreamID: "task:trace", StreamSeq: 3, EventType: "CodingPhaseStarted", Payload: `{"phase":"edit","run_id":"run:1"}`, Timestamp: now.Add(2 * time.Second)},
		{StreamID: "task:trace", StreamSeq: 4, EventType: "ContextAssembled", Payload: `{"run_id":"run:1","strategy":"coding_debug","estimated_tokens":20}`, Timestamp: now.Add(3 * time.Second)},
		{StreamID: "task:trace", StreamSeq: 5, EventType: "MemoryRetrievalStarted", Payload: `{"run_id":"run:1","source":"sqlite_fts"}`, Timestamp: now.Add(4 * time.Second)},
		{StreamID: "task:trace", StreamSeq: 6, EventType: "MemoryRetrievalCompleted", Payload: `{"run_id":"run:1","source":"sqlite_fts","memory_count":1}`, Timestamp: now.Add(5 * time.Second)},
		{StreamID: "task:trace", StreamSeq: 7, EventType: "LLMCallStarted", Payload: `{"run_id":"run:1","call_id":"llm:1","model":"deepseek-chat"}`, Timestamp: now.Add(6 * time.Second)},
		{StreamID: "task:trace", StreamSeq: 8, EventType: "LLMCallCompleted", Payload: `{"call_id":"llm:1","total_tokens":12}`, Timestamp: now.Add(7 * time.Second)},
		{StreamID: "task:trace", StreamSeq: 9, EventType: "ToolCallStarted", Payload: `{"run_id":"run:1","tool_call_id":"tool:1","tool_name":"write_file"}`, Timestamp: now.Add(8 * time.Second)},
		{StreamID: "task:trace", StreamSeq: 10, EventType: "ToolCallCompleted", Payload: `{"tool_call_id":"tool:1","tool_name":"write_file","touched_files":["main.go"]}`, Timestamp: now.Add(9 * time.Second)},
		{StreamID: "task:trace", StreamSeq: 11, EventType: "CodingPhaseCompleted", Payload: `{"phase":"edit","run_id":"run:1"}`, Timestamp: now.Add(10 * time.Second)},
		{StreamID: "task:trace", StreamSeq: 12, EventType: "WorkspaceCheckpointCreated", Payload: `{"run_id":"run:1","snapshot_ref":"snapshot:1","checkpoint":"run_completed"}`, Timestamp: now.Add(11 * time.Second)},
		{StreamID: "task:trace", StreamSeq: 13, EventType: "RunCompleted", Payload: `{"turn_id":"turn:1","run_id":"run:1","final_answer":"done"}`, Timestamp: now.Add(12 * time.Second)},
	}
	view, err := BuildTraceView("task:trace", events)
	if err != nil {
		t.Fatalf("BuildTraceView: %v", err)
	}
	if view.RootSpanID == "" || len(view.Spans) == 0 {
		t.Fatalf("view = %+v", view)
	}
	assertTraceSpan(t, view, "run", "coding", StatusCompleted)
	assertTraceSpan(t, view, "workflow_phase", "edit", StatusCompleted)
	assertTraceSpan(t, view, "context_assembly", "coding_debug", StatusCompleted)
	assertTraceSpan(t, view, "memory_retrieval", "sqlite_fts", StatusCompleted)
	assertTraceSpan(t, view, "llm_call", "deepseek-chat", StatusCompleted)
	assertTraceSpan(t, view, "tool_call", "write_file", StatusCompleted)
	if len(view.Checkpoints) != 1 || view.Checkpoints[0].SnapshotRef != "snapshot:1" {
		t.Fatalf("checkpoints = %+v", view.Checkpoints)
	}
	if len(view.Edges) == 0 {
		t.Fatalf("expected parent edges")
	}
}

func TestEngineProjectTrace(t *testing.T) {
	ctx := context.Background()
	store := openProjectionStore(t)
	defer store.Close()
	if _, err := store.AppendEvents(ctx, []storage.AppendEvent{
		{StreamID: "task:engine-trace", EventType: "TurnCreated", Payload: map[string]any{"turn_id": "turn:1"}},
		{StreamID: "task:engine-trace", EventType: "RunStarted", Payload: map[string]any{"turn_id": "turn:1", "run_id": "run:1", "workflow_type": "simple"}},
		{StreamID: "task:engine-trace", EventType: "RunCompleted", Payload: map[string]any{"turn_id": "turn:1", "run_id": "run:1"}},
	}); err != nil {
		t.Fatalf("append events: %v", err)
	}
	view, err := NewEngine(store, config.Default()).ProjectTrace(ctx, "task:engine-trace")
	if err != nil {
		t.Fatalf("ProjectTrace: %v", err)
	}
	assertTraceSpan(t, view, "run", "simple", StatusCompleted)
}

func assertTraceSpan(t *testing.T, view TraceView, spanType string, name string, status TaskStatus) {
	t.Helper()
	for _, span := range view.Spans {
		if span.Type == spanType && span.Name == name {
			if span.Status != status {
				t.Fatalf("span %s/%s status = %s, want %s", spanType, name, span.Status, status)
			}
			if span.ParentID == "" && span.Type != "session" {
				t.Fatalf("span %s/%s has no parent: %+v", spanType, name, span)
			}
			return
		}
	}
	t.Fatalf("missing span type=%s name=%s in %+v", spanType, name, view.Spans)
}
