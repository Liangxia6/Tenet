package workflow

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/storage"
	"github.com/tenet/orchestrator/internal/worker"
)

func TestReplayRunsWorkflowWithoutAppendingEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()

	cfg := config.Default()
	cfg.Workflow.RecordBatchSize = 20
	streamID := "task:replay-full"
	turnID := "turn:1"
	runID := "run:1"
	workspace := t.TempDir()
	if _, err := store.AppendEvents(ctx, []storage.AppendEvent{
		{StreamID: streamID, EventType: "SessionCreated", Payload: map[string]any{"session_id": streamID, "query": "hello", "workspace": workspace, "workflow_type": "simple"}},
		{StreamID: streamID, EventType: "TurnCreated", Payload: map[string]any{"session_id": streamID, "turn_id": turnID, "query": "hello"}},
		{StreamID: streamID, EventType: "TaskCreated", Payload: map[string]any{"session_id": streamID, "turn_id": turnID, "run_id": runID, "query": "hello", "workspace": workspace, "workflow_type": "simple"}},
		{StreamID: streamID, EventType: "ComplexityAnalyzed", Payload: map[string]any{"selected_workflow": "simple"}},
	}); err != nil {
		t.Fatalf("append metadata: %v", err)
	}
	if _, err := Execute(ctx, store, NewRegistry(), &TaskHandle{
		StreamID:     streamID,
		WorkflowType: "simple",
		SessionID:    streamID,
		TurnID:       turnID,
		RunID:        runID,
		Query:        "hello",
		Workspace:    workspace,
		SystemPrompt: "system",
		Config:       cfg,
		Client: worker.StaticClient{Response: worker.GenerateThoughtResponse{
			Thought:      "cached answer",
			IsFinal:      true,
			FinishReason: "stop",
			Usage:        worker.TokenUsage{PromptTokens: 2, CompletionTokens: 3, TotalTokens: 5},
		}},
	}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	beforeSeq, err := store.LatestSeq(streamID)
	if err != nil {
		t.Fatalf("latest seq: %v", err)
	}
	result, err := Replay(ctx, store, NewRegistry(), &TaskHandle{StreamID: streamID, Config: cfg})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	afterSeq, err := store.LatestSeq(streamID)
	if err != nil {
		t.Fatalf("latest seq after: %v", err)
	}
	if afterSeq != beforeSeq {
		t.Fatalf("replay appended events: before=%d after=%d", beforeSeq, afterSeq)
	}
	if result.RunID != runID || result.TurnID != turnID || result.Workflow != "simple" {
		t.Fatalf("result ids = %+v", result)
	}
	if result.Result != "cached answer" {
		t.Fatalf("result = %v, want cached answer", result.Result)
	}
}

func TestReplayFailsWhenWorkflowRecordsUnexpectedEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()

	cfg := config.Default()
	cfg.Workflow.RecordBatchSize = 20
	registry := NewRegistry()
	registry.Register("custom", func(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
		if err := wfctx.Record(ctx, "TaskStarted", map[string]any{"query": task.Query}); err != nil {
			return nil, err
		}
		return "ok", nil
	})
	if _, err := Execute(ctx, store, registry, &TaskHandle{
		StreamID:     "task:replay-drift",
		WorkflowType: "custom",
		Query:        "drift",
		Workspace:    t.TempDir(),
		Config:       cfg,
	}); err != nil {
		t.Fatalf("execute: %v", err)
	}

	drifted := NewRegistry()
	drifted.Register("custom", func(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
		if err := wfctx.Record(ctx, "TaskStarted", map[string]any{"query": task.Query}); err != nil {
			return nil, err
		}
		if err := wfctx.Record(ctx, "UnexpectedNewEvent", map[string]any{}); err != nil {
			return nil, err
		}
		return "ok", nil
	})
	_, err := Replay(ctx, store, drifted, &TaskHandle{StreamID: "task:replay-drift", Config: cfg})
	if err == nil {
		t.Fatalf("expected replay non-determinism")
	}
	if !strings.Contains(err.Error(), "non-determinism") {
		t.Fatalf("error = %v", err)
	}
}
