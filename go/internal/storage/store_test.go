package storage

import (
    "context"
    "database/sql"
    "path/filepath"
    "testing"
    "time"

    _ "modernc.org/sqlite"
)

func setupDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "test.db")+"?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := InitSchema(db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	return db
}

func TestSQLiteStoreAppendAndRead(t *testing.T) {
    db := setupDB(t)
    store := NewSQLiteStore(db, SQLiteOptions{QueueSize: 8})
    defer store.Close()

    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()

	evt, err := store.AppendEvent(ctx, AppendEvent{
		StreamID:  "task:1",
		EventType: "TaskStarted",
		Payload: map[string]string{
			"query": "hello",
		},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if evt.StreamSeq != 1 {
		t.Fatalf("expected appended seq=1, got %d", evt.StreamSeq)
	}

	events, err := store.Read("task:1", 1)
	if err != nil {
        t.Fatalf("read: %v", err)
    }
    if len(events) != 1 {
        t.Fatalf("expected 1 event, got %d", len(events))
    }
	if events[0].StreamSeq != 1 {
		t.Errorf("expected seq=1, got %d", events[0].StreamSeq)
	}
}

func TestSQLiteStoreAppendEventsAssignsContinuousSeq(t *testing.T) {
	db := setupDB(t)
	store := NewSQLiteStore(db, SQLiteOptions{QueueSize: 8})
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	events, err := store.AppendEvents(ctx, []AppendEvent{
		{StreamID: "task:1", EventType: "TaskStarted", Payload: map[string]any{}},
		{StreamID: "task:1", EventType: "GenerateThought", Payload: map[string]any{"result": "ok"}},
		{StreamID: "task:1", EventType: "TaskCompleted", Payload: map[string]any{}},
	})
	if err != nil {
		t.Fatalf("append events: %v", err)
	}
	for i, evt := range events {
		want := int64(i + 1)
		if evt.StreamSeq != want {
			t.Fatalf("event %d seq = %d, want %d", i, evt.StreamSeq, want)
		}
	}

	latest, err := store.LatestSeq("task:1")
	if err != nil {
		t.Fatalf("latest seq: %v", err)
	}
	if latest != 3 {
		t.Fatalf("latest seq = %d, want 3", latest)
	}
}
