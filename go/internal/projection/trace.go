package projection

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tenet/orchestrator/internal/storage"
)

type TraceView struct {
	StreamID    string               `json:"stream_id"`
	RootSpanID  string               `json:"root_span_id,omitempty"`
	Spans       []TraceSpan          `json:"spans"`
	Edges       []TraceEdge          `json:"edges,omitempty"`
	Artifacts   []TraceArtifactRef   `json:"artifacts,omitempty"`
	Checkpoints []TraceCheckpointRef `json:"checkpoints,omitempty"`
}

type TraceSpan struct {
	ID           string         `json:"id"`
	ParentID     string         `json:"parent_id,omitempty"`
	StreamID     string         `json:"stream_id"`
	SessionID    string         `json:"session_id,omitempty"`
	TurnID       string         `json:"turn_id,omitempty"`
	RunID        string         `json:"run_id,omitempty"`
	Type         string         `json:"type"`
	Name         string         `json:"name"`
	Status       TaskStatus     `json:"status"`
	StartedSeq   int64          `json:"started_seq"`
	CompletedSeq int64          `json:"completed_seq,omitempty"`
	StartedAt    time.Time      `json:"started_at"`
	CompletedAt  time.Time      `json:"completed_at,omitempty"`
	Error        string         `json:"error,omitempty"`
	Attributes   map[string]any `json:"attributes,omitempty"`
}

type TraceEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Type string `json:"type"`
}

type TraceArtifactRef struct {
	ArtifactID string `json:"artifact_id"`
	VersionID  string `json:"version_id,omitempty"`
	Path       string `json:"path,omitempty"`
	SpanID     string `json:"span_id,omitempty"`
	EventSeq   int64  `json:"event_seq,omitempty"`
}

type TraceCheckpointRef struct {
	CheckpointID string `json:"checkpoint_id,omitempty"`
	SpanID       string `json:"span_id,omitempty"`
	EventSeq     int64  `json:"event_seq,omitempty"`
	Reason       string `json:"reason,omitempty"`
	SnapshotRef  string `json:"snapshot_ref,omitempty"`
}

type TraceProjection struct {
	view         TraceView
	spans        map[string]int
	turnSpans    map[string]string
	runSpans     map[string]string
	phaseSpans   map[string]string
	llmSpans     map[string]string
	toolSpans    map[string]string
	openMemory   string
	currentTurn  string
	currentRun   string
	currentPhase string
}

func (e *Engine) ProjectTrace(ctx context.Context, streamID string) (TraceView, error) {
	if streamID == "" {
		return TraceView{}, fmt.Errorf("stream_id is required")
	}
	events, err := e.store.Read(streamID, 1)
	if err != nil {
		return TraceView{}, err
	}
	return BuildTraceView(streamID, events)
}

func BuildTraceView(streamID string, events []storage.Event) (TraceView, error) {
	projection := NewTraceProjection(streamID)
	for _, event := range events {
		if err := projection.Apply(event); err != nil {
			return TraceView{}, err
		}
	}
	return projection.State(), nil
}

func NewTraceProjection(streamID string) *TraceProjection {
	rootID := traceID(streamID, "session", "root")
	p := &TraceProjection{
		view: TraceView{
			StreamID:   streamID,
			RootSpanID: rootID,
		},
		spans:      map[string]int{},
		turnSpans:  map[string]string{},
		runSpans:   map[string]string{},
		phaseSpans: map[string]string{},
		llmSpans:   map[string]string{},
		toolSpans:  map[string]string{},
	}
	p.startSpan(TraceSpan{
		ID:         rootID,
		StreamID:   streamID,
		Type:       "session",
		Name:       streamID,
		Status:     StatusRunning,
		StartedSeq: 1,
		Attributes: map[string]any{},
	})
	return p
}

func (p *TraceProjection) Apply(event storage.Event) error {
	payload, err := decodePayload(event.Payload)
	if err != nil {
		return err
	}
	p.ensureRootTimestamp(event)
	switch event.EventType {
	case "SessionCreated", "TaskCreated":
		p.updateRoot(payload)
	case "TurnCreated", "UserMessage":
		turnID := firstNonEmpty(stringValue(payload, "turn_id"), fmt.Sprintf("turn:%d", event.StreamSeq))
		p.currentTurn = turnID
		p.ensureTurnSpan(event, payload, turnID)
	case "RunStarted":
		turnID := firstNonEmpty(stringValue(payload, "turn_id"), p.currentTurn)
		runID := firstNonEmpty(stringValue(payload, "run_id"), fmt.Sprintf("run:%d", event.StreamSeq))
		p.currentTurn = turnID
		p.currentRun = runID
		p.ensureTurnSpan(event, payload, turnID)
		p.startRunSpan(event, payload, turnID, runID)
	case "RunCompleted":
		p.completeRunSpan(event, payload, StatusCompleted)
	case "RunFailed":
		p.completeRunSpan(event, payload, StatusFailed)
	case "RunPaused":
		p.completeRunSpan(event, payload, StatusPaused)
	case "CodingPhaseStarted", "ReasoningPatternStarted", "InteractiveRoundStarted", "DAGWaveStarted":
		p.startPhaseSpan(event, payload)
	case "CodingPhaseCompleted", "ReasoningPatternCompleted", "InteractiveRoundCompleted", "DAGWaveCompleted":
		p.completePhaseSpan(event, payload, StatusCompleted)
	case "ContextAssembled":
		p.instantSpan(event, payload, "context_assembly", firstNonEmpty(stringValue(payload, "strategy"), "context"))
	case "ContextCompressionStarted":
		p.instantSpan(event, payload, "context_compression", "compression_started")
	case "ContextCompressionCompleted":
		p.instantSpan(event, payload, "context_compression", "compression_completed")
	case "MemoryRetrievalStarted":
		p.openMemory = p.startChildSpan(event, payload, "memory_retrieval", firstNonEmpty(stringValue(payload, "source"), "memory"))
	case "MemoryRetrievalCompleted":
		p.completeSpan(firstNonEmpty(p.openMemory, p.latestSpanOfType("memory_retrieval")), event, StatusCompleted, payload)
		p.openMemory = ""
	case "MemoryRetrievalSkipped":
		p.completeSpan(firstNonEmpty(p.openMemory, p.latestSpanOfType("memory_retrieval")), event, StatusCompleted, payload)
		p.openMemory = ""
	case "LLMCallStarted":
		callID := firstNonEmpty(stringValue(payload, "call_id"), fmt.Sprintf("llm:%d", event.StreamSeq))
		spanID := p.startChildSpanWithID(event, payload, traceID(p.view.StreamID, "llm", callID), "llm_call", firstNonEmpty(stringValue(payload, "model"), callID))
		p.llmSpans[callID] = spanID
	case "LLMCallCompleted":
		callID := stringValue(payload, "call_id")
		p.completeSpan(p.llmSpans[callID], event, StatusCompleted, payload)
	case "LLMCallFailed":
		callID := stringValue(payload, "call_id")
		p.completeSpan(p.llmSpans[callID], event, StatusFailed, payload)
	case "ToolCallStarted":
		callID := firstNonEmpty(stringValue(payload, "tool_call_id"), fmt.Sprintf("tool:%d", event.StreamSeq))
		spanID := p.startChildSpanWithID(event, payload, traceID(p.view.StreamID, "tool", callID), "tool_call", firstNonEmpty(stringValue(payload, "tool_name"), callID))
		p.toolSpans[callID] = spanID
	case "ToolCallCompleted":
		callID := stringValue(payload, "tool_call_id")
		p.completeSpan(p.toolSpans[callID], event, StatusCompleted, payload)
	case "ToolCallFailed":
		callID := stringValue(payload, "tool_call_id")
		p.completeSpan(p.toolSpans[callID], event, StatusFailed, payload)
	case "ToolApprovalRequired":
		callID := stringValue(payload, "tool_call_id")
		p.completeSpan(p.toolSpans[callID], event, StatusPaused, payload)
	case "WorkspaceSnapshot", "WorkspaceCheckpointCreated", "AgentCheckpointCreated":
		spanID := p.instantSpan(event, payload, "checkpoint", event.EventType)
		p.view.Checkpoints = append(p.view.Checkpoints, TraceCheckpointRef{
			CheckpointID: stringValue(payload, "checkpoint_id"),
			SpanID:       spanID,
			EventSeq:     event.StreamSeq,
			Reason:       firstNonEmpty(stringValue(payload, "reason"), stringValue(payload, "checkpoint"), event.EventType),
			SnapshotRef:  firstNonEmpty(stringValue(payload, "snapshot_ref"), stringValue(payload, "snapshot_type")),
		})
	case "ArtifactDiscovered", "ArtifactVersionCreated":
		spanID := p.instantSpan(event, payload, "artifact", event.EventType)
		p.view.Artifacts = append(p.view.Artifacts, TraceArtifactRef{
			ArtifactID: firstNonEmpty(stringValue(payload, "artifact_id"), stringValue(payload, "path")),
			VersionID:  stringValue(payload, "version_id"),
			Path:       stringValue(payload, "path"),
			SpanID:     spanID,
			EventSeq:   event.StreamSeq,
		})
	case "ArtifactRollbackStarted", "ArtifactRollbackCompleted", "ArtifactRollbackFailed":
		p.instantSpan(event, payload, "artifact_rollback", event.EventType)
	case "AgentCheckpointRestoreStarted", "AgentCheckpointRestoreCompleted", "AgentCheckpointRestoreFailed":
		p.instantSpan(event, payload, "checkpoint_restore", event.EventType)
	}
	return nil
}

func (p *TraceProjection) State() TraceView {
	return p.view
}

func (p *TraceProjection) updateRoot(payload map[string]any) {
	root := p.findSpan(p.view.RootSpanID)
	if root == nil {
		return
	}
	root.SessionID = firstNonEmpty(stringValue(payload, "session_id"), root.SessionID, p.view.StreamID)
	root.TurnID = firstNonEmpty(stringValue(payload, "turn_id"), root.TurnID)
	root.RunID = firstNonEmpty(stringValue(payload, "run_id"), root.RunID)
	root.Attributes = mergeTraceAttributes(root.Attributes, payload)
}

func (p *TraceProjection) ensureRootTimestamp(event storage.Event) {
	root := p.findSpan(p.view.RootSpanID)
	if root == nil {
		return
	}
	if root.StartedAt.IsZero() {
		root.StartedAt = event.Timestamp
	}
}

func (p *TraceProjection) ensureTurnSpan(event storage.Event, payload map[string]any, turnID string) string {
	if turnID == "" {
		return p.view.RootSpanID
	}
	if spanID := p.turnSpans[turnID]; spanID != "" {
		return spanID
	}
	spanID := traceID(p.view.StreamID, "turn", turnID)
	p.turnSpans[turnID] = spanID
	p.startSpan(TraceSpan{
		ID:         spanID,
		ParentID:   p.view.RootSpanID,
		StreamID:   p.view.StreamID,
		SessionID:  firstNonEmpty(stringValue(payload, "session_id"), p.view.StreamID),
		TurnID:     turnID,
		Type:       "turn",
		Name:       turnID,
		Status:     StatusRunning,
		StartedSeq: event.StreamSeq,
		StartedAt:  event.Timestamp,
		Attributes: copyTraceAttributes(payload),
	})
	return spanID
}

func (p *TraceProjection) startRunSpan(event storage.Event, payload map[string]any, turnID, runID string) string {
	if spanID := p.runSpans[runID]; spanID != "" {
		return spanID
	}
	parentID := p.ensureTurnSpan(event, payload, turnID)
	spanID := traceID(p.view.StreamID, "run", runID)
	p.runSpans[runID] = spanID
	p.startSpan(TraceSpan{
		ID:         spanID,
		ParentID:   parentID,
		StreamID:   p.view.StreamID,
		SessionID:  firstNonEmpty(stringValue(payload, "session_id"), p.view.StreamID),
		TurnID:     turnID,
		RunID:      runID,
		Type:       "run",
		Name:       firstNonEmpty(stringValue(payload, "workflow_type"), runID),
		Status:     StatusRunning,
		StartedSeq: event.StreamSeq,
		StartedAt:  event.Timestamp,
		Attributes: copyTraceAttributes(payload),
	})
	return spanID
}

func (p *TraceProjection) completeRunSpan(event storage.Event, payload map[string]any, status TaskStatus) {
	runID := firstNonEmpty(stringValue(payload, "run_id"), p.currentRun)
	p.completeSpan(p.runSpans[runID], event, status, payload)
	if span := p.findSpan(p.turnSpans[firstNonEmpty(stringValue(payload, "turn_id"), p.currentTurn)]); span != nil {
		span.Status = status
		span.CompletedSeq = event.StreamSeq
		span.CompletedAt = event.Timestamp
	}
}

func (p *TraceProjection) startPhaseSpan(event storage.Event, payload map[string]any) {
	name := firstNonEmpty(stringValue(payload, "phase"), stringValue(payload, "pattern"), stringValue(payload, "round"), stringValue(payload, "wave"), event.EventType)
	key := p.currentRun + ":" + event.EventType + ":" + name
	spanID := p.startChildSpanWithID(event, payload, traceID(p.view.StreamID, "phase", key), "workflow_phase", name)
	p.currentPhase = spanID
	p.phaseSpans[key] = spanID
}

func (p *TraceProjection) completePhaseSpan(event storage.Event, payload map[string]any, status TaskStatus) {
	name := firstNonEmpty(stringValue(payload, "phase"), stringValue(payload, "pattern"), stringValue(payload, "round"), stringValue(payload, "wave"), event.EventType)
	key := p.currentRun + ":" + strings.ReplaceAll(event.EventType, "Completed", "Started") + ":" + name
	spanID := firstNonEmpty(p.phaseSpans[key], p.currentPhase)
	p.completeSpan(spanID, event, status, payload)
	if spanID == p.currentPhase {
		p.currentPhase = ""
	}
}

func (p *TraceProjection) instantSpan(event storage.Event, payload map[string]any, spanType, name string) string {
	spanID := traceID(p.view.StreamID, spanType, fmt.Sprintf("%d", event.StreamSeq))
	p.startChildSpanWithID(event, payload, spanID, spanType, name)
	p.completeSpan(spanID, event, StatusCompleted, payload)
	return spanID
}

func (p *TraceProjection) startChildSpan(event storage.Event, payload map[string]any, spanType, name string) string {
	return p.startChildSpanWithID(event, payload, traceID(p.view.StreamID, spanType, fmt.Sprintf("%d", event.StreamSeq)), spanType, name)
}

func (p *TraceProjection) startChildSpanWithID(event storage.Event, payload map[string]any, spanID, spanType, name string) string {
	if spanID == "" {
		spanID = traceID(p.view.StreamID, spanType, fmt.Sprintf("%d", event.StreamSeq))
	}
	parentID := p.activeParentID()
	span := TraceSpan{
		ID:         spanID,
		ParentID:   parentID,
		StreamID:   p.view.StreamID,
		SessionID:  firstNonEmpty(stringValue(payload, "session_id"), p.view.StreamID),
		TurnID:     firstNonEmpty(stringValue(payload, "turn_id"), p.currentTurn),
		RunID:      firstNonEmpty(stringValue(payload, "run_id"), p.currentRun),
		Type:       spanType,
		Name:       name,
		Status:     StatusRunning,
		StartedSeq: event.StreamSeq,
		StartedAt:  event.Timestamp,
		Attributes: copyTraceAttributes(payload),
	}
	p.startSpan(span)
	return spanID
}

func (p *TraceProjection) startSpan(span TraceSpan) {
	if span.ID == "" {
		return
	}
	if span.StartedAt.IsZero() {
		span.StartedAt = time.Now().UTC()
	}
	if span.Status == "" {
		span.Status = StatusRunning
	}
	if idx, ok := p.spans[span.ID]; ok {
		current := p.view.Spans[idx]
		current.Attributes = mergeTraceAttributes(current.Attributes, span.Attributes)
		if current.ParentID == "" {
			current.ParentID = span.ParentID
		}
		p.view.Spans[idx] = current
		return
	}
	p.spans[span.ID] = len(p.view.Spans)
	p.view.Spans = append(p.view.Spans, span)
	if span.ParentID != "" {
		p.view.Edges = append(p.view.Edges, TraceEdge{From: span.ParentID, To: span.ID, Type: "parent"})
	}
}

func (p *TraceProjection) completeSpan(spanID string, event storage.Event, status TaskStatus, payload map[string]any) {
	span := p.findSpan(spanID)
	if span == nil {
		return
	}
	span.Status = status
	span.CompletedSeq = event.StreamSeq
	span.CompletedAt = event.Timestamp
	span.Attributes = mergeTraceAttributes(span.Attributes, payload)
	span.Error = firstNonEmpty(stringValue(payload, "error"), stringValue(payload, "reason"), span.Error)
}

func (p *TraceProjection) activeParentID() string {
	if p.currentPhase != "" {
		return p.currentPhase
	}
	if p.currentRun != "" {
		if spanID := p.runSpans[p.currentRun]; spanID != "" {
			return spanID
		}
	}
	if p.currentTurn != "" {
		if spanID := p.turnSpans[p.currentTurn]; spanID != "" {
			return spanID
		}
	}
	return p.view.RootSpanID
}

func (p *TraceProjection) latestSpanOfType(spanType string) string {
	for i := len(p.view.Spans) - 1; i >= 0; i-- {
		if p.view.Spans[i].Type == spanType {
			return p.view.Spans[i].ID
		}
	}
	return ""
}

func (p *TraceProjection) findSpan(spanID string) *TraceSpan {
	if spanID == "" {
		return nil
	}
	idx, ok := p.spans[spanID]
	if !ok {
		return nil
	}
	return &p.view.Spans[idx]
}

func traceID(streamID, kind, key string) string {
	key = strings.NewReplacer(" ", "_", "/", "_", ":", "_").Replace(key)
	return "span:" + streamID + ":" + kind + ":" + key
}

func copyTraceAttributes(payload map[string]any) map[string]any {
	if len(payload) == 0 {
		return nil
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		out[key] = value
	}
	return out
}

func mergeTraceAttributes(base map[string]any, next map[string]any) map[string]any {
	if len(base) == 0 {
		return copyTraceAttributes(next)
	}
	for key, value := range next {
		base[key] = value
	}
	return base
}
