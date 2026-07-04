package timer

import (
	"context"
	"encoding/json"
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

type ResumeResult struct {
	StreamID  string
	TimerID   string
	Fired     storage.Event
	Resumed   storage.Event
	Skipped   bool
	SkipCause string
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

func (s *Service) ResumeDueTimers(ctx context.Context, now time.Time, limit int) ([]ResumeResult, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("timer store is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if limit <= 0 {
		limit = 1000
	}
	streams, err := s.store.ListStreams(limit)
	if err != nil {
		return nil, err
	}
	results := []ResumeResult{}
	for _, stream := range streams {
		events, err := s.store.Read(stream.StreamID, 1)
		if err != nil {
			return nil, err
		}
		streamResults, err := s.resumeDueTimersInStream(ctx, stream.StreamID, events, now)
		if err != nil {
			return nil, err
		}
		results = append(results, streamResults...)
	}
	return results, nil
}

func (s *Service) resumeDueTimersInStream(ctx context.Context, streamID string, events []storage.Event, now time.Time) ([]ResumeResult, error) {
	type scheduledTimer struct {
		id      string
		payload map[string]any
		parent  string
	}
	scheduled := map[string]scheduledTimer{}
	fired := map[string]bool{}
	resumed := map[string]bool{}
	for _, event := range events {
		payload := decodeTimerPayload(event.Payload)
		timerID := timerIDFromPayload(payload)
		if timerID == "" {
			continue
		}
		switch event.EventType {
		case "TimerScheduled", "TaskResumeScheduled":
			dueAt, ok := dueAtFromPayload(payload)
			if ok && !dueAt.After(now) {
				scheduled[timerID] = scheduledTimer{id: timerID, payload: payload, parent: event.ParentID}
			}
		case "TimerFired":
			fired[timerID] = true
		case "TaskResumed":
			resumed[timerID] = true
		}
	}
	timerIDs := make([]string, 0, len(scheduled))
	for timerID := range scheduled {
		timerIDs = append(timerIDs, timerID)
	}
	sortStrings(timerIDs)
	results := make([]ResumeResult, 0, len(timerIDs))
	for _, timerID := range timerIDs {
		item := scheduled[timerID]
		if fired[timerID] || resumed[timerID] {
			results = append(results, ResumeResult{StreamID: streamID, TimerID: timerID, Skipped: true, SkipCause: "already fired or resumed"})
			continue
		}
		firedEvent, err := s.store.AppendEvent(ctx, storage.AppendEvent{
			StreamID:  streamID,
			EventType: "TimerFired",
			Payload:   timerResumePayload(item.payload, timerID),
			ParentID:  item.parent,
		})
		if err != nil {
			return nil, err
		}
		resumedEvent, err := s.store.AppendEvent(ctx, storage.AppendEvent{
			StreamID:  streamID,
			EventType: "TaskResumed",
			Payload:   timerResumePayload(item.payload, timerID),
			ParentID:  item.parent,
		})
		if err != nil {
			return nil, err
		}
		results = append(results, ResumeResult{StreamID: streamID, TimerID: timerID, Fired: firedEvent, Resumed: resumedEvent})
	}
	return results, nil
}

func copyPayload(in map[string]any) map[string]any {
	out := make(map[string]any, len(in)+3)
	for k, v := range in {
		out[k] = v
	}
	return out
}

func decodeTimerPayload(raw string) map[string]any {
	payload := map[string]any{}
	_ = json.Unmarshal([]byte(raw), &payload)
	return payload
}

func timerIDFromPayload(payload map[string]any) string {
	if value, ok := payload["timer_id"].(string); ok {
		return value
	}
	return ""
}

func dueAtFromPayload(payload map[string]any) (time.Time, bool) {
	raw, ok := payload["due_at"].(string)
	if !ok || raw == "" {
		return time.Time{}, false
	}
	dueAt, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}
	return dueAt, true
}

func timerResumePayload(source map[string]any, timerID string) map[string]any {
	payload := copyPayload(source)
	payload["timer_id"] = timerID
	if _, ok := payload["note"]; !ok {
		payload["note"] = "timer fired"
	}
	return payload
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
