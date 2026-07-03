package scheduler

import (
	"context"
	"database/sql"
	"errors"
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

func TestSchedulerTracksActiveAndQueuedTasks(t *testing.T) {
	store := testSchedulerStore(t)
	defer store.Close()
	release := make(chan struct{})
	client := &blockingClient{started: make(chan struct{}, 2), release: release}
	s := New(store, workflow.NewRegistry(), 1, 4)
	defer s.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, streamID := range []string{"task:one", "task:two"} {
		if err := s.Submit(ctx, &workflow.TaskHandle{
			StreamID:     streamID,
			Mode:         workflow.ContextModeExecution,
			WorkflowType: "simple",
			SessionID:    streamID,
			Query:        "hello",
			Config:       config.Default(),
			Client:       client,
		}); err != nil {
			t.Fatalf("submit %s: %v", streamID, err)
		}
	}
	select {
	case <-client.started:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for first task")
	}
	stats := s.Stats()
	if stats.Active != 1 || stats.Queued != 1 || stats.MaxConcurrent != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	close(release)
	for i := 0; i < 2; i++ {
		select {
		case result := <-s.Results():
			if result.Err != nil {
				t.Fatalf("result error: %v", result.Err)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for result %d", i)
		}
	}
}

func TestSchedulerShutdownRejectsSubmit(t *testing.T) {
	store := testSchedulerStore(t)
	defer store.Close()
	s := New(store, workflow.NewRegistry(), 1, 1)
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	err := s.Submit(context.Background(), &workflow.TaskHandle{StreamID: "task:late"})
	if !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("submit err = %v, want ErrShuttingDown", err)
	}
}

type blockingClient struct {
	started chan struct{}
	release <-chan struct{}
}

func (c *blockingClient) GenerateThought(ctx context.Context, req worker.GenerateThoughtRequest) (worker.GenerateThoughtResponse, error) {
	c.started <- struct{}{}
	select {
	case <-c.release:
	case <-ctx.Done():
		return worker.GenerateThoughtResponse{}, ctx.Err()
	}
	return worker.GenerateThoughtResponse{
		Thought:      "done " + req.TaskID,
		IsFinal:      true,
		FinishReason: "stop",
	}, nil
}

func (c *blockingClient) ExecuteTool(context.Context, worker.ExecuteToolRequest) (worker.ExecuteToolResponse, error) {
	return worker.ExecuteToolResponse{}, nil
}

func (c *blockingClient) HealthCheck(context.Context) (worker.HealthCheckResponse, error) {
	return worker.HealthCheckResponse{Status: "SERVING", WorkerCount: 1}, nil
}

func testSchedulerStore(t *testing.T) storage.Store {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "test.db")+"?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := storage.InitSchema(db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	return storage.NewSQLiteStore(db, storage.SQLiteOptions{QueueSize: 8})
}
