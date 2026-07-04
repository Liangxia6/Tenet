package timer

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/tenet/orchestrator/internal/storage"
	_ "modernc.org/sqlite"
)

func TestServiceScheduleFiresEvent(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "test.db")+"?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := storage.InitSchema(db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	store := storage.NewSQLiteStore(db, storage.SQLiteOptions{QueueSize: 8})
	defer store.Close()

	service := NewService(store)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done, err := service.Schedule(ctx, ScheduleRequest{
		StreamID: "task:timer-service",
		TimerID:  "resume:1",
		Delay:    5 * time.Millisecond,
		Payload:  map[string]any{"note": "wake"},
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	select {
	case result := <-done:
		if result.Err != nil {
			t.Fatalf("timer result: %v", result.Err)
		}
		if result.Scheduled.EventType != "TimerScheduled" || result.Fired.EventType != "TimerFired" {
			t.Fatalf("result = %+v", result)
		}
	case <-ctx.Done():
		t.Fatalf("timer did not fire: %v", ctx.Err())
	}
	events, err := store.Read("task:timer-service", 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(events) != 2 || events[0].EventType != "TimerScheduled" || events[1].EventType != "TimerFired" {
		t.Fatalf("events = %+v", events)
	}
}

func TestServiceResumeDueTimersAppendsFiredAndResumed(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "test.db")+"?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := storage.InitSchema(db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	store := storage.NewSQLiteStore(db, storage.SQLiteOptions{QueueSize: 8})
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	now := time.Now().UTC()
	if _, err := store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  "task:due",
		EventType: "TimerScheduled",
		Payload: map[string]any{
			"timer_id": "resume:due",
			"due_at":   now.Add(-time.Second).Format(time.RFC3339Nano),
			"note":     "wake up",
			"run_id":   "run:1",
		},
	}); err != nil {
		t.Fatalf("append timer: %v", err)
	}
	service := NewService(store)
	results, err := service.ResumeDueTimers(ctx, now, 100)
	if err != nil {
		t.Fatalf("ResumeDueTimers: %v", err)
	}
	if len(results) != 1 || results[0].Fired.EventType != "TimerFired" || results[0].Resumed.EventType != "TaskResumed" {
		t.Fatalf("results = %+v", results)
	}
	events, err := store.Read("task:due", 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(events) != 3 || events[1].EventType != "TimerFired" || events[2].EventType != "TaskResumed" {
		t.Fatalf("events = %+v", events)
	}

	results, err = service.ResumeDueTimers(ctx, now, 100)
	if err != nil {
		t.Fatalf("ResumeDueTimers again: %v", err)
	}
	if len(results) != 1 || !results[0].Skipped {
		t.Fatalf("second results = %+v", results)
	}
}
