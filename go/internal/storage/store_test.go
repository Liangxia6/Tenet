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

func TestSQLiteStoreForkStreamAndLineage(t *testing.T) {
	db := setupDB(t)
	store := NewSQLiteStore(db, SQLiteOptions{QueueSize: 8})
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := store.AppendEvents(ctx, []AppendEvent{
		{StreamID: "task:parent", EventType: "TaskCreated", Payload: map[string]any{"query": "original"}},
		{StreamID: "task:parent", EventType: "GenerateThought", Payload: map[string]any{"result": "first"}},
		{StreamID: "task:parent", EventType: "TaskCompleted", Payload: map[string]any{"final_answer": "done"}},
	}); err != nil {
		t.Fatalf("append parent events: %v", err)
	}

	childID, err := store.ForkStream(ctx, "task:parent", 2, "try another path")
	if err != nil {
		t.Fatalf("ForkStream: %v", err)
	}
	if childID != "task:parent/fork:2" {
		t.Fatalf("child id = %q", childID)
	}
	childEvents, err := store.Read(childID, 1)
	if err != nil {
		t.Fatalf("read child: %v", err)
	}
	if len(childEvents) != 3 {
		t.Fatalf("child events = %d, want 3", len(childEvents))
	}
	if childEvents[0].EventType != "TaskCreated" || childEvents[1].EventType != "GenerateThought" || childEvents[2].EventType != "ForkCreated" {
		t.Fatalf("child events = %+v", childEvents)
	}
	if childEvents[0].ParentID != "task:parent" {
		t.Fatalf("parent id = %q", childEvents[0].ParentID)
	}

	lineage, err := store.GetLineage(childID)
	if err != nil {
		t.Fatalf("GetLineage: %v", err)
	}
	if len(lineage) != 2 || lineage[0] != "task:parent" || lineage[1] != childID {
		t.Fatalf("lineage = %+v", lineage)
	}
	children, err := store.GetChildStreams("task:parent")
	if err != nil {
		t.Fatalf("GetChildStreams: %v", err)
	}
	if len(children) != 1 || children[0] != childID {
		t.Fatalf("children = %+v", children)
	}
}

func TestSQLiteStoreListStreams(t *testing.T) {
	db := setupDB(t)
	store := NewSQLiteStore(db, SQLiteOptions{QueueSize: 8})
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := store.AppendEvents(ctx, []AppendEvent{
		{StreamID: "task:a", EventType: "TaskCreated", Payload: map[string]any{}},
		{StreamID: "task:a", EventType: "TaskCompleted", Payload: map[string]any{}},
		{StreamID: "task:b", EventType: "TaskCreated", Payload: map[string]any{}},
	}); err != nil {
		t.Fatalf("append events: %v", err)
	}
	streams, err := store.ListStreams(10)
	if err != nil {
		t.Fatalf("ListStreams: %v", err)
	}
	if len(streams) != 2 {
		t.Fatalf("streams = %+v", streams)
	}
	if streams[0].StreamID != "task:b" || streams[0].LatestSeq != 1 {
		t.Fatalf("first stream = %+v", streams[0])
	}
}

func TestSQLiteStoreSaveAndReadLatestSnapshot(t *testing.T) {
	db := setupDB(t)
	store := NewSQLiteStore(db, SQLiteOptions{QueueSize: 8})
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := store.SaveSnapshot(ctx, SnapshotRecord{
		StreamID:  "task:snap",
		StreamSeq: 2,
		Type:      "archive",
		Ref:       "/tmp/one.tar.gz",
		StateBlob: `{"step":2}`,
	}); err != nil {
		t.Fatalf("SaveSnapshot first: %v", err)
	}
	if _, err := store.SaveSnapshot(ctx, SnapshotRecord{
		StreamID:  "task:snap",
		StreamSeq: 4,
		Type:      "archive",
		Ref:       "/tmp/two.tar.gz",
		StateBlob: `{"step":4}`,
	}); err != nil {
		t.Fatalf("SaveSnapshot second: %v", err)
	}
	snapshot, err := store.LatestSnapshot("task:snap", 3)
	if err != nil {
		t.Fatalf("LatestSnapshot: %v", err)
	}
	if snapshot.StreamSeq != 2 || snapshot.Ref != "/tmp/one.tar.gz" {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	snapshot, err = store.LatestSnapshot("task:snap", 0)
	if err != nil {
		t.Fatalf("LatestSnapshot unbounded: %v", err)
	}
	if snapshot.StreamSeq != 4 || snapshot.Ref != "/tmp/two.tar.gz" {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestSQLiteStoreSaveAndReadProjectionSnapshot(t *testing.T) {
	db := setupDB(t)
	store := NewSQLiteStore(db, SQLiteOptions{QueueSize: 8})
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := store.SaveProjectionSnapshot(ctx, ProjectionSnapshot{
		StreamID:  "task:projection",
		StreamSeq: 2,
		StateBlob: `{"stream_id":"task:projection","status":"RUNNING"}`,
	}); err != nil {
		t.Fatalf("SaveProjectionSnapshot first: %v", err)
	}
	if _, err := store.SaveProjectionSnapshot(ctx, ProjectionSnapshot{
		StreamID:  "task:projection",
		StreamSeq: 5,
		StateBlob: `{"stream_id":"task:projection","status":"COMPLETED"}`,
	}); err != nil {
		t.Fatalf("SaveProjectionSnapshot second: %v", err)
	}
	snapshot, err := store.LatestProjectionSnapshot("task:projection")
	if err != nil {
		t.Fatalf("LatestProjectionSnapshot: %v", err)
	}
	if snapshot.StreamSeq != 5 || snapshot.StateBlob == "" {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}
