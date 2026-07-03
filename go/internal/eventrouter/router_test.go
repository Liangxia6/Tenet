package eventrouter

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/tenet/orchestrator/internal/storage"
)

func TestRouterPersistsAndPublishesEvent(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	stream := NewMemoryStream()
	router := New(store, stream)
	t.Cleanup(func() {
		_ = router.Close()
	})
	events, cancel := stream.Subscribe("task:router", 1)
	defer cancel()

	appended, err := router.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  "task:router",
		EventType: "TaskStarted",
		Payload:   map[string]any{"query": "hello"},
	})
	if err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if appended.StreamSeq != 1 {
		t.Fatalf("stream seq = %d, want 1", appended.StreamSeq)
	}
	persisted, err := router.Read("task:router", 1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(persisted) != 1 || persisted[0].EventType != "TaskStarted" {
		t.Fatalf("persisted = %+v", persisted)
	}

	select {
	case event := <-events:
		if event.StreamID != "task:router" || event.EventType != "TaskStarted" || event.StreamSeq != 1 {
			t.Fatalf("stream event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for stream event")
	}
}

func TestRouterDoesNotPublishWhenStateFails(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	stream := NewMemoryStream()
	router := New(store, stream)
	t.Cleanup(func() {
		_ = router.Close()
	})
	events, cancel := stream.Subscribe("", 1)
	defer cancel()

	if _, err := router.AppendEvent(ctx, storage.AppendEvent{EventType: "Invalid", Payload: map[string]any{}}); err == nil {
		t.Fatalf("expected append error")
	}
	select {
	case event := <-events:
		t.Fatalf("unexpected stream event: %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
}

func openStore(t *testing.T) *storage.SQLiteStore {
	t.Helper()
	store, err := storage.Open(filepath.Join(t.TempDir(), "tenet.db"), storage.SQLiteOptions{QueueSize: 16})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}
