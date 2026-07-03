package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tenet/orchestrator/internal/guard"
	"github.com/tenet/orchestrator/internal/storage"
)

type CaptureResult struct {
	Snapshot storage.SnapshotRecord `json:"snapshot"`
	Event    storage.Event          `json:"event"`
}

type ForkResult struct {
	StreamID   string                  `json:"stream_id"`
	ParentID   string                  `json:"parent_id"`
	ForkAtSeq  int64                   `json:"fork_at_seq"`
	Workspace  string                  `json:"workspace"`
	Snapshot   *storage.SnapshotRecord `json:"snapshot,omitempty"`
	Restored   bool                    `json:"restored"`
	RestoreErr string                  `json:"restore_error,omitempty"`
}

func (m *Manager) CaptureSnapshot(
	ctx context.Context,
	store storage.Store,
	streamID string,
	root string,
	sessionID string,
	seqHint int64,
	state any,
	lock guard.LockManager,
	lease guard.FencingLease,
) (CaptureResult, error) {
	if store == nil {
		return CaptureResult{}, errors.New("store is required")
	}
	snapshot, err := m.Snapshot(ctx, root, sessionID, seqHint, lock, lease)
	if err != nil {
		return CaptureResult{}, err
	}
	payload := map[string]any{
		"snapshot_type": snapshot.Type,
		"snapshot_ref":  snapshot.Ref,
		"workspace":     root,
	}
	if state != nil {
		payload["state"] = state
	}
	event, err := store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  streamID,
		EventType: "WorkspaceSnapshot",
		Payload:   payload,
	})
	if err != nil {
		return CaptureResult{}, err
	}
	stateBlob := "{}"
	if state != nil {
		data, err := json.Marshal(state)
		if err != nil {
			return CaptureResult{}, fmt.Errorf("marshal snapshot state: %w", err)
		}
		stateBlob = string(data)
	}
	record, err := store.SaveSnapshot(ctx, storage.SnapshotRecord{
		StreamID:  streamID,
		StreamSeq: event.StreamSeq,
		Type:      snapshot.Type,
		Ref:       snapshot.Ref,
		StateBlob: stateBlob,
	})
	if err != nil {
		return CaptureResult{}, err
	}
	return CaptureResult{Snapshot: record, Event: event}, nil
}

func (m *Manager) ForkWorkspace(
	ctx context.Context,
	store storage.Store,
	parentStreamID string,
	forkFromSeq int64,
	newQuery string,
	lock guard.LockManager,
	lease guard.FencingLease,
) (ForkResult, error) {
	if store == nil {
		return ForkResult{}, errors.New("store is required")
	}
	childID, err := store.ForkStream(ctx, parentStreamID, forkFromSeq, newQuery)
	if err != nil {
		return ForkResult{}, err
	}
	result := ForkResult{
		StreamID:  childID,
		ParentID:  parentStreamID,
		ForkAtSeq: forkFromSeq,
	}
	root, err := m.Init(childID)
	if err != nil {
		return result, err
	}
	result.Workspace = root
	snapshot, err := store.LatestSnapshot(parentStreamID, forkFromSeq)
	if errors.Is(err, sql.ErrNoRows) {
		_, appendErr := store.AppendEvent(ctx, storage.AppendEvent{
			StreamID:  childID,
			EventType: "ForkWorkspaceInitialized",
			Payload: map[string]any{
				"workspace": root,
				"restored":  false,
				"reason":    "no_snapshot_before_fork",
			},
			ParentID: parentStreamID,
		})
		return result, appendErr
	}
	if err != nil {
		return result, err
	}
	result.Snapshot = &snapshot
	restoreSnapshot := Snapshot{Type: snapshot.Type, Ref: snapshot.Ref}
	if err := m.Restore(ctx, restoreSnapshot, root, lock, lease); err != nil {
		result.RestoreErr = err.Error()
		_, appendErr := store.AppendEvent(ctx, storage.AppendEvent{
			StreamID:  childID,
			EventType: "ForkWorkspaceRestoreFailed",
			Payload: map[string]any{
				"workspace":      root,
				"snapshot_type":  snapshot.Type,
				"snapshot_ref":   snapshot.Ref,
				"snapshot_seq":   snapshot.StreamSeq,
				"restore_error":  err.Error(),
				"parent_stream":  parentStreamID,
				"parent_fork_at": forkFromSeq,
			},
			ParentID: parentStreamID,
		})
		if appendErr != nil {
			return result, appendErr
		}
		return result, err
	}
	result.Restored = true
	_, err = store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  childID,
		EventType: "ForkWorkspaceRestored",
		Payload: map[string]any{
			"workspace":      root,
			"snapshot_type":  snapshot.Type,
			"snapshot_ref":   snapshot.Ref,
			"snapshot_seq":   snapshot.StreamSeq,
			"parent_stream":  parentStreamID,
			"parent_fork_at": forkFromSeq,
		},
		ParentID: parentStreamID,
	})
	return result, err
}
