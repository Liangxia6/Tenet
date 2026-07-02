package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/storage"
)

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
	history         []storage.Event
	historyPos      int
	pendingRecords  []storage.AppendEvent
	recordBatchSize int
	config          *config.RuntimeConfig
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
		history:         history,
		recordBatchSize: recordBatchSize,
		config:          cfg,
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

func (c *WorkflowContext) flushLocked(ctx context.Context) error {
	if len(c.pendingRecords) == 0 {
		return nil
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
