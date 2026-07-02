package workflow

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/storage"
	_ "modernc.org/sqlite"
)

func testStore(t *testing.T) storage.Store {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "test.db")+"?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := storage.InitSchema(db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	return storage.NewSQLiteStore(db, storage.SQLiteOptions{QueueSize: 16})
}

func testConfig() *config.RuntimeConfig {
	return &config.RuntimeConfig{
		Workflow: config.WorkflowConfig{RecordBatchSize: 20},
	}
}

func TestDecideFlushesRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()

	wfctx, err := NewContext(ctx, store, "task:flush", "", ContextModeExecution, testConfig())
	if err != nil {
		t.Fatalf("new context: %v", err)
	}
	if err := wfctx.Record(ctx, "TaskStarted", map[string]any{"query": "x"}); err != nil {
		t.Fatalf("record: %v", err)
	}
	result, err := wfctx.Decide(ctx, "GenerateThought", func(context.Context) (any, error) {
		return "hello", nil
	})
	if err != nil {
		t.Fatalf("decide: %v", err)
	}
	if result != "hello" {
		t.Fatalf("result = %v, want hello", result)
	}

	events, err := store.Read("task:flush", 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events len = %d, want 2", len(events))
	}
	if events[0].EventType != "TaskStarted" || events[1].EventType != "GenerateThought" {
		t.Fatalf("unexpected event order: %s, %s", events[0].EventType, events[1].EventType)
	}
}

func TestReplaySkipsDecisionFunction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()

	wfctx, err := NewContext(ctx, store, "task:replay", "", ContextModeExecution, testConfig())
	if err != nil {
		t.Fatalf("new execution context: %v", err)
	}
	if _, err := wfctx.Decide(ctx, "GenerateThought", func(context.Context) (any, error) {
		return "cached", nil
	}); err != nil {
		t.Fatalf("decide: %v", err)
	}

	replay, err := NewContext(ctx, store, "task:replay", "", ContextModeReplay, testConfig())
	if err != nil {
		t.Fatalf("new replay context: %v", err)
	}
	called := false
	result, err := replay.Decide(ctx, "GenerateThought", func(context.Context) (any, error) {
		called = true
		return "new", nil
	})
	if err != nil {
		t.Fatalf("replay decide: %v", err)
	}
	if called {
		t.Fatalf("decision function should not be called during replay")
	}
	if result != "cached" {
		t.Fatalf("result = %v, want cached", result)
	}
}
