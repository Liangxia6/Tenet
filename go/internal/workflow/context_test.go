package workflow

import (
	"context"
	"database/sql"
	"errors"
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

func TestGetVersionRecordsAndReplaysMarker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()
	wfctx, err := NewContext(ctx, store, "task:version", "", ContextModeExecution, testConfig())
	if err != nil {
		t.Fatalf("new context: %v", err)
	}
	if got := wfctx.GetVersion("add-feature", 2); got != 2 {
		t.Fatalf("version = %d, want 2", got)
	}
	if err := wfctx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	events, err := store.Read("task:version", 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(events) != 1 || events[0].EventType != "VersionMarker" {
		t.Fatalf("events = %+v", events)
	}
	replay, err := NewContext(ctx, store, "task:version", "", ContextModeReplay, testConfig())
	if err != nil {
		t.Fatalf("new replay: %v", err)
	}
	if got := replay.GetVersion("add-feature", 2); got != 2 {
		t.Fatalf("replay version = %d, want 2", got)
	}
	if replay.HistoryLength() != 0 {
		t.Fatalf("version marker should be removed from replay history")
	}
}

func TestGetVersionDefaultsOldReplayToOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()
	if _, err := store.AppendEvent(ctx, storage.AppendEvent{StreamID: "task:old", EventType: "TaskStarted", Payload: map[string]any{}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	replay, err := NewContext(ctx, store, "task:old", "", ContextModeReplay, testConfig())
	if err != nil {
		t.Fatalf("new replay: %v", err)
	}
	if got := replay.GetVersion("new-change", 3); got != 1 {
		t.Fatalf("old replay version = %d, want 1", got)
	}
}

func TestSleepRecordsTimerAndReplayDoesNotWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()
	wfctx, err := NewContext(ctx, store, "task:timer", "", ContextModeExecution, testConfig())
	if err != nil {
		t.Fatalf("new context: %v", err)
	}
	start := time.Now()
	if err := wfctx.Sleep(ctx, "short", time.Hour); !errors.Is(err, ErrWorkflowSuspended) {
		t.Fatalf("sleep err = %v, want suspended", err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatalf("durable sleep waited too long")
	}
	if err := wfctx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	assertWorkflowEvents(t, store, "task:timer", "TimerScheduled")

	replay, err := NewContext(ctx, store, "task:timer", "", ContextModeReplay, testConfig())
	if err != nil {
		t.Fatalf("new replay: %v", err)
	}
	start = time.Now()
	if err := replay.Sleep(ctx, "short", time.Hour); !errors.Is(err, ErrWorkflowSuspended) {
		t.Fatalf("replay sleep err = %v, want suspended", err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatalf("replay sleep waited too long")
	}
}

func assertWorkflowEvents(t *testing.T, store storage.Store, streamID string, wants ...string) {
	t.Helper()
	events, err := store.Read(streamID, 1)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(events) != len(wants) {
		t.Fatalf("events len = %d, want %d: %+v", len(events), len(wants), events)
	}
	for i, want := range wants {
		if events[i].EventType != want {
			t.Fatalf("event %d = %s, want %s", i, events[i].EventType, want)
		}
	}
}
