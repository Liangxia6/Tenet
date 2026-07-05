package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"

	"github.com/tenet/orchestrator/internal/storage"
)

func recordToolArtifactVersions(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, toolCallID string, touchedFiles []string) error {
	if wfctx == nil || task == nil || len(touchedFiles) == 0 {
		return nil
	}
	if wfctx.mode == ContextModeReplay {
		for wfctx.nextHistoryEventType() == "ArtifactVersionCreated" {
			if err := wfctx.Record(ctx, "ArtifactVersionCreated", map[string]any{}); err != nil {
				return err
			}
		}
		return nil
	}
	if err := wfctx.Commit(ctx); err != nil {
		return err
	}
	baseSeq, err := wfctx.store.LatestSeq(task.StreamID)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, touched := range touchedFiles {
		rel, abs, ok := artifactPath(task.Workspace, touched)
		if !ok || seen[rel] {
			continue
		}
		seen[rel] = true
		info, err := os.Stat(abs)
		if err != nil || info.IsDir() {
			continue
		}
		data, err := os.ReadFile(abs)
		if err != nil {
			continue
		}
		sum := sha256.Sum256(data)
		version, err := wfctx.store.RecordArtifactVersion(ctx, storage.ArtifactVersion{
			StreamID:           task.StreamID,
			TurnID:             task.TurnID,
			RunID:              task.RunID,
			Workspace:          task.Workspace,
			Path:               rel,
			ArtifactType:       artifactTypeForPath(rel),
			EventSeq:           baseSeq,
			ProducerToolCallID: toolCallID,
			ContentHash:        "sha256:" + hex.EncodeToString(sum[:]),
			ContentBlob:        string(data),
			SizeBytes:          info.Size(),
			Summary:            "updated by tool " + toolCallID,
		})
		if err != nil {
			return err
		}
		if err := wfctx.Record(ctx, "ArtifactVersionCreated", map[string]any{
			"artifact_id":           version.ArtifactID,
			"version_id":            version.ID,
			"version":               version.Version,
			"path":                  version.Path,
			"artifact_type":         version.ArtifactType,
			"content_hash":          version.ContentHash,
			"size_bytes":            version.SizeBytes,
			"producer_tool_call_id": toolCallID,
			"turn_id":               task.TurnID,
			"run_id":                task.RunID,
			"event_seq":             version.EventSeq,
		}); err != nil {
			return err
		}
	}
	return nil
}

func artifactPath(workspace string, touched string) (string, string, bool) {
	touched = strings.TrimSpace(touched)
	if touched == "" || filepath.IsAbs(touched) {
		return "", "", false
	}
	rel := filepath.Clean(touched)
	if rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return "", "", false
	}
	abs := filepath.Join(workspace, rel)
	return filepath.ToSlash(rel), abs, true
}

func artifactTypeForPath(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".go", ".py", ".ts", ".tsx", ".js", ".jsx", ".java", ".rs", ".cpp", ".c", ".h":
		return "code"
	case ".md", ".txt", ".docx":
		return "document"
	case ".yaml", ".yml", ".json", ".toml", ".ini":
		return "config"
	case ".log", ".out":
		return "log"
	default:
		return "file"
	}
}
