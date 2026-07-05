package eventrouter

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/tenet/orchestrator/internal/storage"
)

type StreamEvent struct {
	ID        int64     `json:"id"`
	StreamID  string    `json:"stream_id"`
	StreamSeq int64     `json:"stream_seq"`
	EventType string    `json:"event_type"`
	Payload   string    `json:"payload"`
	ParentID  string    `json:"parent_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type StreamChannel interface {
	Publish(context.Context, StreamEvent) error
}

type Router struct {
	state  storage.Store
	stream StreamChannel
}

func New(state storage.Store, stream StreamChannel) *Router {
	return &Router{state: state, stream: stream}
}

func (r *Router) AppendEvent(ctx context.Context, event storage.AppendEvent) (storage.Event, error) {
	events, err := r.AppendEvents(ctx, []storage.AppendEvent{event})
	if err != nil {
		return storage.Event{}, err
	}
	if len(events) == 0 {
		return storage.Event{}, nil
	}
	return events[0], nil
}

func (r *Router) AppendEvents(ctx context.Context, events []storage.AppendEvent) ([]storage.Event, error) {
	appended, err := r.state.AppendEvents(ctx, events)
	if err != nil {
		return nil, err
	}
	r.publishAsync(appended)
	return appended, nil
}

func (r *Router) Append(ctx context.Context, req *storage.WriteRequest) (*storage.WriteResult, error) {
	return r.state.Append(ctx, req)
}

func (r *Router) SaveSnapshot(ctx context.Context, snapshot storage.SnapshotRecord) (storage.SnapshotRecord, error) {
	return r.state.SaveSnapshot(ctx, snapshot)
}

func (r *Router) LatestSnapshot(streamID string, maxSeq int64) (storage.SnapshotRecord, error) {
	return r.state.LatestSnapshot(streamID, maxSeq)
}

func (r *Router) SaveProjectionSnapshot(ctx context.Context, snapshot storage.ProjectionSnapshot) (storage.ProjectionSnapshot, error) {
	return r.state.SaveProjectionSnapshot(ctx, snapshot)
}

func (r *Router) LatestProjectionSnapshot(streamID string) (storage.ProjectionSnapshot, error) {
	return r.state.LatestProjectionSnapshot(streamID)
}

func (r *Router) SaveAgentCheckpoint(ctx context.Context, checkpoint storage.AgentCheckpoint) (storage.AgentCheckpoint, error) {
	return r.state.SaveAgentCheckpoint(ctx, checkpoint)
}

func (r *Router) GetAgentCheckpoint(ctx context.Context, id string) (storage.AgentCheckpoint, error) {
	return r.state.GetAgentCheckpoint(ctx, id)
}

func (r *Router) ListAgentCheckpoints(ctx context.Context, streamID string, limit int) ([]storage.AgentCheckpoint, error) {
	return r.state.ListAgentCheckpoints(ctx, streamID, limit)
}

func (r *Router) RecordArtifactVersion(ctx context.Context, version storage.ArtifactVersion) (storage.ArtifactVersion, error) {
	return r.state.RecordArtifactVersion(ctx, version)
}

func (r *Router) ListArtifacts(ctx context.Context, streamID string) ([]storage.Artifact, error) {
	return r.state.ListArtifacts(ctx, streamID)
}

func (r *Router) ListArtifactVersions(ctx context.Context, streamID string, path string) ([]storage.ArtifactVersion, error) {
	return r.state.ListArtifactVersions(ctx, streamID, path)
}

func (r *Router) GetArtifactVersion(ctx context.Context, streamID string, path string, version int) (storage.ArtifactVersion, error) {
	return r.state.GetArtifactVersion(ctx, streamID, path, version)
}

func (r *Router) SaveMemoryEntry(ctx context.Context, entry storage.MemoryEntry) (storage.MemoryEntry, error) {
	return r.state.SaveMemoryEntry(ctx, entry)
}

func (r *Router) SearchMemory(ctx context.Context, query string, limit int) ([]storage.MemoryEntry, error) {
	return r.state.SearchMemory(ctx, query, limit)
}

func (r *Router) SearchMemoryEntries(ctx context.Context, query storage.MemorySearchQuery) ([]storage.MemoryEntry, error) {
	return r.state.SearchMemoryEntries(ctx, query)
}

func (r *Router) ForkStream(ctx context.Context, parentStreamID string, forkFromSeq int64, newQuery string) (string, error) {
	return r.state.ForkStream(ctx, parentStreamID, forkFromSeq, newQuery)
}

func (r *Router) GetLineage(streamID string) ([]string, error) {
	return r.state.GetLineage(streamID)
}

func (r *Router) GetChildStreams(streamID string) ([]string, error) {
	return r.state.GetChildStreams(streamID)
}

func (r *Router) ListStreams(limit int) ([]storage.StreamSummary, error) {
	return r.state.ListStreams(limit)
}

func (r *Router) Read(streamID string, fromSeq int64) ([]storage.Event, error) {
	return r.state.Read(streamID, fromSeq)
}

func (r *Router) LatestSeq(streamID string) (int64, error) {
	return r.state.LatestSeq(streamID)
}

func (r *Router) Close() error {
	if closer, ok := r.stream.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
	return r.state.Close()
}

func (r *Router) publishAsync(events []storage.Event) {
	if r.stream == nil {
		return
	}
	for _, event := range events {
		streamEvent := FromStorageEvent(event)
		go func() {
			_ = r.stream.Publish(context.Background(), streamEvent)
		}()
	}
}

func FromStorageEvent(event storage.Event) StreamEvent {
	return StreamEvent{
		ID:        event.ID,
		StreamID:  event.StreamID,
		StreamSeq: event.StreamSeq,
		EventType: event.EventType,
		Payload:   event.Payload,
		ParentID:  event.ParentID,
		Timestamp: event.Timestamp,
	}
}

func Encode(event StreamEvent) ([]byte, error) {
	return json.Marshal(event)
}

type MemoryStream struct {
	mu          sync.Mutex
	subscribers map[string]map[chan StreamEvent]struct{}
}

func NewMemoryStream() *MemoryStream {
	return &MemoryStream{subscribers: map[string]map[chan StreamEvent]struct{}{}}
}

func (s *MemoryStream) Publish(_ context.Context, event StreamEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for subscriber := range s.subscribers[event.StreamID] {
		select {
		case subscriber <- event:
		default:
		}
	}
	return nil
}

func (s *MemoryStream) Subscribe(streamID string, buffer int) (<-chan StreamEvent, func()) {
	if buffer <= 0 {
		buffer = 64
	}
	ch := make(chan StreamEvent, buffer)
	s.mu.Lock()
	if s.subscribers[streamID] == nil {
		s.subscribers[streamID] = map[chan StreamEvent]struct{}{}
	}
	s.subscribers[streamID][ch] = struct{}{}
	s.mu.Unlock()
	cancel := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if subscribers := s.subscribers[streamID]; subscribers != nil {
			delete(subscribers, ch)
			close(ch)
			if len(subscribers) == 0 {
				delete(s.subscribers, streamID)
			}
		}
	}
	return ch, cancel
}
