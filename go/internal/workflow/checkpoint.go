package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tenet/orchestrator/internal/storage"
)

func recordAgentCheckpoint(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, reason string, state map[string]any) error {
	if wfctx == nil || task == nil || strings.TrimSpace(reason) == "" {
		return nil
	}
	if wfctx.mode == ContextModeReplay {
		if wfctx.nextHistoryEventType() == "AgentCheckpointCreated" {
			return wfctx.Record(ctx, "AgentCheckpointCreated", map[string]any{})
		}
		return nil
	}
	if err := wfctx.Commit(ctx); err != nil {
		return err
	}
	seq, err := wfctx.store.LatestSeq(task.StreamID)
	if err != nil {
		return err
	}
	checkpoint := storage.AgentCheckpoint{
		StreamID:           task.StreamID,
		TurnID:             task.TurnID,
		RunID:              task.RunID,
		EventSeq:           seq,
		WorkflowType:       task.WorkflowType,
		WorkflowPhase:      stringValueFromMap(state, "workflow_phase"),
		Reason:             reason,
		ContextStateJSON:   jsonObject(state["context"]),
		MemoryStateJSON:    jsonObject(state["memory"]),
		TokenStateJSON:     jsonObject(state["tokens"]),
		ToolStateJSON:      jsonObject(state["tool"]),
		ArtifactManifestID: stringValueFromMap(state, "artifact_manifest_id"),
	}
	if rawID, ok := state["workspace_snapshot_id"].(int64); ok {
		checkpoint.WorkspaceSnapshotID = rawID
	}
	saved, err := wfctx.store.SaveAgentCheckpoint(ctx, checkpoint)
	if err != nil {
		return err
	}
	return wfctx.Record(ctx, "AgentCheckpointCreated", map[string]any{
		"checkpoint_id":         saved.ID,
		"stream_id":             saved.StreamID,
		"session_id":            task.SessionID,
		"turn_id":               task.TurnID,
		"run_id":                task.RunID,
		"event_seq":             saved.EventSeq,
		"workflow_type":         saved.WorkflowType,
		"workflow_phase":        saved.WorkflowPhase,
		"reason":                saved.Reason,
		"workspace_snapshot_id": saved.WorkspaceSnapshotID,
		"artifact_manifest_id":  saved.ArtifactManifestID,
	})
}

func jsonObject(value any) string {
	if value == nil {
		return "{}"
	}
	data, err := json.Marshal(value)
	if err != nil || len(data) == 0 {
		return "{}"
	}
	return string(data)
}

func stringValueFromMap(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	switch value := values[key].(type) {
	case string:
		return value
	case fmt.Stringer:
		return value.String()
	default:
		return ""
	}
}
