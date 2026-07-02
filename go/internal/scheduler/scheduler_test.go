package scheduler

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/storage"
	"github.com/tenet/orchestrator/internal/worker"
	"github.com/tenet/orchestrator/internal/workflow"
	_ "modernc.org/sqlite"
)

func TestSchedulerRunsTask(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "test.db")+"?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := storage.InitSchema(db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	store := storage.NewSQLiteStore(db, storage.SQLiteOptions{QueueSize: 8})
	defer store.Close()

	s := New(store, workflow.NewRegistry(), 1, 4)
	defer s.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = s.Submit(ctx, &workflow.TaskHandle{
		StreamID:     "task:scheduler",
		Mode:         workflow.ContextModeExecution,
		WorkflowType: "simple",
		SessionID:    "task:scheduler",
		Query:        "hello",
		Config:       config.Default(),
		Client:       worker.NewEchoClient(),
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	select {
	case result := <-s.Results():
		if result.Err != nil {
			t.Fatalf("task error: %v", result.Err)
		}
		if result.Result == nil {
			t.Fatalf("expected result")
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for result")
	}
}
