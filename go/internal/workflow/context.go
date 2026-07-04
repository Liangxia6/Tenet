package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/storage"
)

var ErrWorkflowSuspended = errors.New("workflow suspended")

type SuspensionError struct {
	Reason  string
	TimerID string
}

func (e SuspensionError) Error() string {
	if e.TimerID != "" {
		return fmt.Sprintf("%s: %s", e.Reason, e.TimerID)
	}
	return e.Reason
}

func (e SuspensionError) Unwrap() error {
	return ErrWorkflowSuspended
}

type ContextMode string

const (
	ContextModeExecution ContextMode = "execution"
	ContextModeReplay    ContextMode = "replay"
)

type DecisionFunc func(context.Context) (any, error)

type WorkflowContext struct {
	store           storage.Store
	streamID        string
	parentID        string
	mode            ContextMode
	history         []storage.Event
	historyPos      int
	pendingRecords  []storage.AppendEvent
	recordBatchSize int
	config          *config.RuntimeConfig
	versionMarkers  map[string]bool
	mu              sync.Mutex
}

type DecisionPayload struct {
	Result any    `json:"result"`
	Error  string `json:"error,omitempty"`
}

func NewContext(
	ctx context.Context,
	store storage.Store,
	streamID string,
	parentID string,
	mode ContextMode,
	cfg *config.RuntimeConfig,
) (*WorkflowContext, error) {
	history := []storage.Event{}
	if mode == ContextModeReplay {
		events, err := store.Read(streamID, 1)
		if err != nil {
			return nil, err
		}
		history = events
	}
	_ = ctx
	recordBatchSize := 20
	if cfg != nil && cfg.Workflow.RecordBatchSize > 0 {
		recordBatchSize = cfg.Workflow.RecordBatchSize
	}
	return &WorkflowContext{
		store:           store,
		streamID:        streamID,
		parentID:        parentID,
		mode:            mode,
		history:         history,
		recordBatchSize: recordBatchSize,
		config:          cfg,
		versionMarkers:  map[string]bool{},
	}, nil
}

func (c *WorkflowContext) StreamID() string {
	return c.streamID
}

func (c *WorkflowContext) Record(ctx context.Context, eventType string, payload any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.historyPos < len(c.history) {
		evt := c.history[c.historyPos]
		if evt.EventType != eventType {
			return fmt.Errorf("non-determinism detected: at stream=%s, pos=%d, expected=%s, got=%s", c.streamID, c.historyPos, eventType, evt.EventType)
		}
		c.historyPos++
		return nil
	}
	if c.mode == ContextModeReplay {
		return fmt.Errorf("non-determinism detected: at stream=%s, pos=%d, expected=%s, got=end-of-history", c.streamID, c.historyPos, eventType)
	}

	c.pendingRecords = append(c.pendingRecords, storage.AppendEvent{
		StreamID:  c.streamID,
		EventType: eventType,
		Payload:   payload,
		ParentID:  c.parentID,
	})
	if len(c.pendingRecords) >= c.recordBatchSize {
		return c.flushLocked(ctx)
	}
	return nil
}

func (c *WorkflowContext) Decide(ctx context.Context, decisionType string, fn DecisionFunc) (any, error) {
	c.mu.Lock()
	if err := c.flushLocked(ctx); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if c.historyPos < len(c.history) {
		evt := c.history[c.historyPos]
		if evt.EventType != decisionType {
			c.mu.Unlock()
			return nil, fmt.Errorf("non-determinism detected: at stream=%s, pos=%d, expected=%s, got=%s", c.streamID, c.historyPos, decisionType, evt.EventType)
		}
		c.historyPos++
		c.mu.Unlock()
		var payload DecisionPayload
		if err := json.Unmarshal([]byte(evt.Payload), &payload); err != nil {
			return nil, fmt.Errorf("decode decision payload: %w", err)
		}
		return payload.Result, nil
	}
	if c.mode == ContextModeReplay {
		c.mu.Unlock()
		return nil, fmt.Errorf("non-determinism detected: at stream=%s, pos=%d, expected=%s, got=end-of-history", c.streamID, c.historyPos, decisionType)
	}
	c.mu.Unlock()

	result, err := fn(ctx)
	payload := DecisionPayload{Result: result}
	if err != nil {
		payload.Error = err.Error()
	}
	appended, appendErr := c.store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  c.streamID,
		EventType: decisionType,
		Payload:   payload,
		ParentID:  c.parentID,
	})
	if appendErr != nil {
		return nil, appendErr
	}

	c.mu.Lock()
	c.history = append(c.history, appended)
	c.historyPos++
	c.mu.Unlock()
	return result, err
}

func (c *WorkflowContext) Commit(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushLocked(ctx)
}

func (c *WorkflowContext) Suspend(ctx context.Context, reason string) error {
	if reason == "" {
		reason = "workflow suspended"
	}
	if err := c.Record(ctx, "TaskPaused", map[string]any{"reason": reason}); err != nil {
		return err
	}
	return c.Commit(ctx)
}

func (c *WorkflowContext) Sleep(ctx context.Context, timerID string, delay time.Duration) error {
	if timerID == "" {
		return fmt.Errorf("timer_id is required")
	}
	if delay < 0 {
		return fmt.Errorf("delay must be non-negative")
	}
	delayMS := delay.Milliseconds()
	if err := c.Record(ctx, "TimerScheduled", map[string]any{
		"timer_id": timerID,
		"delay_ms": delayMS,
		"due_at":   time.Now().UTC().Add(delay).Format(time.RFC3339Nano),
	}); err != nil {
		return err
	}
	if c.mode == ContextModeReplay {
		if c.nextHistoryEventType() == "TimerFired" {
			return c.Record(ctx, "TimerFired", map[string]any{"timer_id": timerID, "delay_ms": delayMS})
		}
		return SuspensionError{Reason: "timer scheduled", TimerID: timerID}
	}
	if err := c.Commit(ctx); err != nil {
		return err
	}
	return SuspensionError{Reason: "timer scheduled", TimerID: timerID}
}

func (c *WorkflowContext) GetVersion(changeID string, minVersion int) int {
	if changeID == "" {
		return 1
	}
	if minVersion <= 0 {
		minVersion = 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.versionMarkers[changeID] {
		return minVersion
	}
	for i, evt := range c.history {
		if evt.EventType != "VersionMarker" {
			continue
		}
		var payload struct {
			ChangeID string `json:"change_id"`
			Version  int    `json:"version"`
		}
		if err := json.Unmarshal([]byte(evt.Payload), &payload); err != nil {
			continue
		}
		if payload.ChangeID != changeID {
			continue
		}
		c.history = append(c.history[:i], c.history[i+1:]...)
		if i < c.historyPos {
			c.historyPos--
		}
		c.versionMarkers[changeID] = true
		if payload.Version <= 0 {
			return 1
		}
		return payload.Version
	}
	if c.historyPos < len(c.history) {
		c.versionMarkers[changeID] = true
		return 1
	}
	if c.mode == ContextModeReplay {
		c.versionMarkers[changeID] = true
		return 1
	}
	c.pendingRecords = append(c.pendingRecords, storage.AppendEvent{
		StreamID:  c.streamID,
		EventType: "VersionMarker",
		Payload: map[string]any{
			"change_id": changeID,
			"version":   minVersion,
		},
		ParentID: c.parentID,
	})
	c.versionMarkers[changeID] = true
	return minVersion
}

func (c *WorkflowContext) HistoryPosition() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.historyPos
}

func (c *WorkflowContext) HistoryLength() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.history)
}

func (c *WorkflowContext) nextHistoryEventType() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.historyPos >= len(c.history) {
		return ""
	}
	return c.history[c.historyPos].EventType
}

func (c *WorkflowContext) flushLocked(ctx context.Context) error {
	if len(c.pendingRecords) == 0 {
		return nil
	}
	if c.mode == ContextModeReplay {
		return fmt.Errorf("non-determinism detected: replay attempted to append %d event(s)", len(c.pendingRecords))
	}
	appended, err := c.store.AppendEvents(ctx, c.pendingRecords)
	if err != nil {
		return err
	}
	c.history = append(c.history, appended...)
	c.historyPos += len(appended)
	c.pendingRecords = nil
	return nil
}
