package timer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tenet/orchestrator/internal/storage"
)

type Service struct {
	store  storage.Store
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type ScheduleRequest struct {
	StreamID           string
	TimerID            string
	Delay              time.Duration
	ScheduledEventType string
	FiredEventType     string
	Payload            map[string]any
	ParentID           string
}

type Result struct {
	Scheduled storage.Event
	Fired     storage.Event
	Err       error
}

func NewService(store storage.Store) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{store: store, ctx: ctx, cancel: cancel}
}

func (s *Service) Schedule(ctx context.Context, req ScheduleRequest) (<-chan Result, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("timer store is required")
	}
	if req.StreamID == "" {
		return nil, errors.New("stream_id is required")
	}
	if req.TimerID == "" {
		return nil, errors.New("timer_id is required")
	}
	if req.Delay < 0 {
		return nil, errors.New("delay must be non-negative")
	}
	if req.ScheduledEventType == "" {
		req.ScheduledEventType = "TimerScheduled"
	}
	if req.FiredEventType == "" {
		req.FiredEventType = "TimerFired"
	}
	payload := copyPayload(req.Payload)
	payload["timer_id"] = req.TimerID
	payload["delay_ms"] = req.Delay.Milliseconds()
	payload["due_at"] = time.Now().UTC().Add(req.Delay).Format(time.RFC3339Nano)
	scheduled, err := s.store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  req.StreamID,
		EventType: req.ScheduledEventType,
		Payload:   payload,
		ParentID:  req.ParentID,
	})
	if err != nil {
		return nil, fmt.Errorf("append timer scheduled: %w", err)
	}
	done := make(chan Result, 1)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(done)
		timer := time.NewTimer(req.Delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-s.ctx.Done():
			done <- Result{Scheduled: scheduled, Err: s.ctx.Err()}
			return
		}
		firedPayload := copyPayload(req.Payload)
		firedPayload["timer_id"] = req.TimerID
		firedPayload["delay_ms"] = req.Delay.Milliseconds()
		fired, err := s.store.AppendEvent(context.Background(), storage.AppendEvent{
			StreamID:  req.StreamID,
			EventType: req.FiredEventType,
			Payload:   firedPayload,
			ParentID:  req.ParentID,
		})
		done <- Result{Scheduled: scheduled, Fired: fired, Err: err}
	}()
	return done, nil
}

func (s *Service) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func copyPayload(in map[string]any) map[string]any {
	out := make(map[string]any, len(in)+3)
	for k, v := range in {
		out[k] = v
	}
	return out
}
