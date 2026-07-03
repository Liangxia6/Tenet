package workspace

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/guard"
	"github.com/tenet/orchestrator/internal/storage"
	_ "modernc.org/sqlite"
)

func TestManagerInitAndValidatePath(t *testing.T) {
	cfg := config.Default()
	cfg.Workspace.BasePath = t.TempDir()
	manager := NewManager(cfg)
	root, err := manager.Init("session:one")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "findings")); err != nil {
		t.Fatalf("findings dir: %v", err)
	}
	if _, err := manager.ValidatePath(root, "../escape.txt", false); err == nil {
		t.Fatalf("expected escaping path to fail")
	}
	path, err := manager.ValidatePath(root, "notes/todo.md", false)
	if err != nil {
		t.Fatalf("ValidatePath: %v", err)
	}
	if filepath.Base(filepath.Dir(path)) != "notes" {
		t.Fatalf("validated path = %s", path)
	}
}

func TestManagerAnalyzeTextRatio(t *testing.T) {
	cfg := config.Default()
	manager := NewManager(cfg)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package main"), 0644); err != nil {
		t.Fatalf("write text: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "blob.bin"), []byte{0, 1, 2}, 0644); err != nil {
		t.Fatalf("write bin: %v", err)
	}
	ratio, err := manager.AnalyzeTextRatio(root)
	if err != nil {
		t.Fatalf("AnalyzeTextRatio: %v", err)
	}
	if ratio != 0.5 {
		t.Fatalf("ratio = %v, want 0.5", ratio)
	}
}

func TestManagerArchiveSnapshotAndRestore(t *testing.T) {
	cfg := config.Default()
	cfg.Workspace.SnapshotDriver = "archive"
	manager := NewManager(cfg)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("hello"), 0644); err != nil {
		t.Fatalf("write readme: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "nested"), 0755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "file.txt"), []byte("nested"), 0644); err != nil {
		t.Fatalf("write nested: %v", err)
	}

	snapshot, err := manager.Snapshot(context.Background(), root, "session:archive", 3, nil, zeroLease())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snapshot.Type != "archive" {
		t.Fatalf("snapshot = %+v", snapshot)
	}

	dest := t.TempDir()
	if err := manager.Restore(context.Background(), snapshot, dest, nil, zeroLease()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dest, "nested", "file.txt"))
	if err != nil {
		t.Fatalf("read restored: %v", err)
	}
	if string(data) != "nested" {
		t.Fatalf("restored data = %q", string(data))
	}
}

func TestForkWorkspaceRestoresLatestSnapshotBeforeFork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cfg := config.Default()
	cfg.Workspace.BasePath = t.TempDir()
	cfg.Workspace.SnapshotDriver = "archive"
	manager := NewManager(cfg)

	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "test.db")+"?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := storage.InitSchema(db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	store := storage.NewSQLiteStore(db, storage.SQLiteOptions{QueueSize: 8})
	defer store.Close()

	parentRoot, err := manager.Init("task:parent")
	if err != nil {
		t.Fatalf("parent init: %v", err)
	}
	if err := os.WriteFile(filepath.Join(parentRoot, "plan.md"), []byte("before fork"), 0644); err != nil {
		t.Fatalf("write parent: %v", err)
	}
	if _, err := store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  "task:parent",
		EventType: "TaskStarted",
		Payload:   map[string]any{"workspace": parentRoot},
	}); err != nil {
		t.Fatalf("append task started: %v", err)
	}
	capture, err := manager.CaptureSnapshot(ctx, store, "task:parent", parentRoot, "task:parent", 2, map[string]any{"phase": "design"}, nil, zeroLease())
	if err != nil {
		t.Fatalf("CaptureSnapshot: %v", err)
	}
	if capture.Snapshot.StreamSeq != 2 {
		t.Fatalf("snapshot seq = %d, want 2", capture.Snapshot.StreamSeq)
	}
	if _, err := store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  "task:parent",
		EventType: "TaskCompleted",
		Payload:   map[string]any{},
	}); err != nil {
		t.Fatalf("append completed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(parentRoot, "plan.md"), []byte("after fork"), 0644); err != nil {
		t.Fatalf("mutate parent: %v", err)
	}

	fork, err := manager.ForkWorkspace(ctx, store, "task:parent", 2, "try snapshot branch", nil, zeroLease())
	if err != nil {
		t.Fatalf("ForkWorkspace: %v", err)
	}
	if !fork.Restored || fork.Snapshot == nil {
		t.Fatalf("fork = %+v", fork)
	}
	data, err := os.ReadFile(filepath.Join(fork.Workspace, "plan.md"))
	if err != nil {
		t.Fatalf("read fork file: %v", err)
	}
	if string(data) != "before fork" {
		t.Fatalf("fork file = %q, want before fork", string(data))
	}
	events, err := store.Read(fork.StreamID, 1)
	if err != nil {
		t.Fatalf("read fork events: %v", err)
	}
	if events[len(events)-1].EventType != "ForkWorkspaceRestored" {
		t.Fatalf("last event = %s", events[len(events)-1].EventType)
	}
}

func zeroLease() guard.FencingLease {
	return guard.FencingLease{}
}
