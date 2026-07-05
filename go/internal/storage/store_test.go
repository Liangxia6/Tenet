package storage

import (
	"context"
	"database/sql"
	"encoding/json"
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

func TestInitSchemaRecordsMigrationVersion(t *testing.T) {
	db := setupDB(t)
	defer db.Close()
	if err := InitSchema(db); err != nil {
		t.Fatalf("InitSchema second run: %v", err)
	}
	versions, err := AppliedSchemaVersions(db)
	if err != nil {
		t.Fatalf("AppliedSchemaVersions: %v", err)
	}
	if len(versions) != 1 || versions[0] != CurrentSchemaVersion {
		t.Fatalf("versions = %+v, want current version %d", versions, CurrentSchemaVersion)
	}
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
	var payload map[string]any
	if err := json.Unmarshal([]byte(events[0].Payload), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload["schema_version"] != float64(1) {
		t.Fatalf("payload = %+v, want schema_version=1", payload)
	}
}

func TestSQLiteStorePreservesExplicitSchemaVersion(t *testing.T) {
	db := setupDB(t)
	store := NewSQLiteStore(db, SQLiteOptions{QueueSize: 8})
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	evt, err := store.AppendEvent(ctx, AppendEvent{
		StreamID:  "task:schema",
		EventType: "TaskStarted",
		Payload:   map[string]any{"schema_version": 2, "query": "hello"},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(evt.Payload), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload["schema_version"] != float64(2) {
		t.Fatalf("payload = %+v, want schema_version=2", payload)
	}
}

func TestSQLiteStoreRedactsSensitivePayloadFields(t *testing.T) {
	db := setupDB(t)
	store := NewSQLiteStore(db, SQLiteOptions{QueueSize: 8})
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	evt, err := store.AppendEvent(ctx, AppendEvent{
		StreamID:  "task:redact",
		EventType: "SecretEvent",
		Payload: map[string]any{
			"api_key": "sk-secret",
			"nested":  map[string]any{"authorization": "Bearer secret"},
			"items":   []any{map[string]any{"password": "pw"}},
			"query":   "keep me",
		},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(evt.Payload), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload["api_key"] != "[REDACTED]" || payload["query"] != "keep me" {
		t.Fatalf("payload = %+v", payload)
	}
	nested := payload["nested"].(map[string]any)
	if nested["authorization"] != "[REDACTED]" {
		t.Fatalf("nested = %+v", nested)
	}
	items := payload["items"].([]any)
	item := items[0].(map[string]any)
	if item["password"] != "[REDACTED]" {
		t.Fatalf("item = %+v", item)
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

func TestSQLiteStoreMemoryFTS(t *testing.T) {
	db := setupDB(t)
	store := NewSQLiteStore(db, SQLiteOptions{QueueSize: 8})
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	entry, err := store.SaveMemoryEntry(ctx, MemoryEntry{
		StreamID:       "task:memory",
		TurnID:         "turn:1",
		RunID:          "run:1",
		Workspace:      "/tmp/workspace",
		Kind:           "session_summary",
		Content:        "Tenet fixed a parser bug and ran tests.",
		SummaryLevel:   1,
		SourceEventSeq: 7,
		Importance:     0.8,
	})
	if err != nil {
		t.Fatalf("SaveMemoryEntry: %v", err)
	}
	if entry.ID == 0 {
		t.Fatalf("entry = %+v", entry)
	}
	results, err := store.SearchMemory(ctx, "parser", 10)
	if err != nil {
		t.Fatalf("SearchMemory: %v", err)
	}
	if len(results) != 1 || results[0].StreamID != "task:memory" || results[0].Kind != "session_summary" {
		t.Fatalf("results = %+v", results)
	}
	if results[0].Workspace != "/tmp/workspace" || results[0].SummaryLevel != 1 || results[0].SourceEventSeq != 7 || results[0].Importance != 0.8 {
		t.Fatalf("memory metadata = %+v", results[0])
	}
	if results[0].TokenEstimate <= 0 {
		t.Fatalf("token estimate = %+v", results[0])
	}
	filtered, err := store.SearchMemoryEntries(ctx, MemorySearchQuery{
		Query:     "parser",
		StreamID:  "task:memory",
		Workspace: "/tmp/workspace",
		Kinds:     []string{"session_summary"},
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("SearchMemoryEntries: %v", err)
	}
	if len(filtered) != 1 || filtered[0].ID != entry.ID {
		t.Fatalf("filtered = %+v", filtered)
	}
	missing, err := store.SearchMemoryEntries(ctx, MemorySearchQuery{
		Query:     "parser",
		StreamID:  "task:memory",
		Workspace: "/tmp/other",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("SearchMemoryEntries missing: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing = %+v", missing)
	}
}
