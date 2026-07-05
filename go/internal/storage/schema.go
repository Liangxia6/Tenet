package storage

import (
	"database/sql"
	"fmt"
)

const CurrentSchemaVersion = 1

func InitSchema(db *sql.DB) error {
	statements := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
		`CREATE TABLE IF NOT EXISTS sessions (
			id TEXT PRIMARY KEY NOT NULL,
			workspace_path TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'RUNNING'
				CHECK (status IN ('RUNNING','PAUSED','COMPLETED','FAILED')),
			agent_config TEXT,
			created_at TEXT NOT NULL DEFAULT (datetime('now')),
			updated_at TEXT NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS event_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			stream_id TEXT NOT NULL,
			stream_seq INTEGER NOT NULL,
			event_type TEXT NOT NULL,
			payload TEXT NOT NULL,
			parent_id TEXT,
			timestamp TEXT NOT NULL DEFAULT (datetime('now')),
			CONSTRAINT uq_stream_seq UNIQUE (stream_id, stream_seq)
		)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			stream_id TEXT NOT NULL,
			stream_seq INTEGER NOT NULL,
			snapshot_type TEXT NOT NULL CHECK (snapshot_type IN ('git','archive')),
			snapshot_ref TEXT NOT NULL,
			state_blob TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS projection_snapshots (
			stream_id TEXT PRIMARY KEY NOT NULL,
			stream_seq INTEGER NOT NULL,
			state_blob TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (datetime('now')),
			updated_at TEXT NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS token_telemetry (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT NOT NULL,
			task_id TEXT NOT NULL,
			agent_name TEXT NOT NULL,
			model TEXT NOT NULL,
			prompt_tokens INTEGER NOT NULL CHECK (prompt_tokens >= 0),
			completion_tokens INTEGER NOT NULL CHECK (completion_tokens >= 0),
			cost_usd REAL NOT NULL DEFAULT 0.0 CHECK (cost_usd >= 0.0),
			event_id INTEGER NOT NULL,
			recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY (event_id) REFERENCES event_log(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS memory_entries (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			stream_id TEXT NOT NULL,
			turn_id TEXT,
			run_id TEXT,
			workspace TEXT,
			kind TEXT NOT NULL,
			content TEXT NOT NULL,
			summary_level INTEGER NOT NULL DEFAULT 0,
			source_event_seq INTEGER NOT NULL DEFAULT 0,
			importance REAL NOT NULL DEFAULT 0,
			token_estimate INTEGER NOT NULL DEFAULT 0,
			expires_at TEXT,
			created_at TEXT NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS agent_checkpoints (
			id TEXT PRIMARY KEY,
			stream_id TEXT NOT NULL,
			turn_id TEXT,
			run_id TEXT,
			event_seq INTEGER NOT NULL,
			workflow_type TEXT,
			workflow_phase TEXT,
			reason TEXT NOT NULL,
			context_state_json TEXT NOT NULL DEFAULT '{}',
			memory_state_json TEXT NOT NULL DEFAULT '{}',
			token_state_json TEXT NOT NULL DEFAULT '{}',
			tool_state_json TEXT NOT NULL DEFAULT '{}',
			workspace_snapshot_id INTEGER,
			artifact_manifest_id TEXT,
			created_at TEXT NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS artifacts (
			id TEXT PRIMARY KEY,
			stream_id TEXT NOT NULL,
			workspace TEXT NOT NULL,
			path TEXT NOT NULL,
			artifact_type TEXT NOT NULL,
			current_version_id TEXT,
			created_by_event_seq INTEGER NOT NULL,
			created_by_span_id TEXT,
			created_at TEXT NOT NULL DEFAULT (datetime('now')),
			updated_at TEXT NOT NULL DEFAULT (datetime('now')),
			UNIQUE(stream_id, workspace, path)
		)`,
		`CREATE TABLE IF NOT EXISTS artifact_versions (
			id TEXT PRIMARY KEY,
			artifact_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			stream_id TEXT NOT NULL,
			turn_id TEXT,
			run_id TEXT,
			event_seq INTEGER NOT NULL,
			producer_span_id TEXT,
			producer_llm_call_id TEXT,
			producer_tool_call_id TEXT,
			content_hash TEXT NOT NULL,
			content_blob TEXT,
			size_bytes INTEGER NOT NULL DEFAULT 0,
			snapshot_ref TEXT,
			diff_ref TEXT,
			summary TEXT,
			created_at TEXT NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY(artifact_id) REFERENCES artifacts(id)
		)`,
		`CREATE TABLE IF NOT EXISTS artifact_diffs (
			id TEXT PRIMARY KEY,
			stream_id TEXT NOT NULL,
			artifact_id TEXT NOT NULL,
			before_version_id TEXT,
			after_version_id TEXT NOT NULL,
			diff_format TEXT NOT NULL,
			diff_text TEXT NOT NULL,
			reversible INTEGER NOT NULL DEFAULT 1,
			created_at TEXT NOT NULL DEFAULT (datetime('now'))
		)`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS memory_entries_fts USING fts5(
			content,
			kind UNINDEXED,
			stream_id UNINDEXED,
			content='memory_entries',
			content_rowid='id'
		)`,
		"CREATE INDEX IF NOT EXISTS idx_event_log_stream_id ON event_log(stream_id)",
		"CREATE INDEX IF NOT EXISTS idx_event_log_stream_id_seq ON event_log(stream_id, stream_seq)",
		"CREATE INDEX IF NOT EXISTS idx_event_log_type ON event_log(event_type)",
		"CREATE INDEX IF NOT EXISTS idx_event_log_timestamp ON event_log(timestamp)",
		"CREATE INDEX IF NOT EXISTS idx_event_log_parent_id ON event_log(parent_id)",
		"CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status)",
		"CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at)",
		"CREATE INDEX IF NOT EXISTS idx_snapshots_stream_id_seq ON snapshots(stream_id, stream_seq DESC)",
		"CREATE INDEX IF NOT EXISTS idx_projection_snapshots_seq ON projection_snapshots(stream_id, stream_seq DESC)",
		"CREATE INDEX IF NOT EXISTS idx_token_telemetry_task ON token_telemetry(task_id)",
		"CREATE INDEX IF NOT EXISTS idx_memory_entries_stream ON memory_entries(stream_id, created_at DESC)",
		"CREATE INDEX IF NOT EXISTS idx_memory_entries_kind ON memory_entries(kind, created_at DESC)",
		"CREATE INDEX IF NOT EXISTS idx_memory_entries_workspace ON memory_entries(workspace, created_at DESC)",
		"CREATE INDEX IF NOT EXISTS idx_agent_checkpoints_stream ON agent_checkpoints(stream_id, event_seq DESC)",
		"CREATE INDEX IF NOT EXISTS idx_agent_checkpoints_run ON agent_checkpoints(run_id, event_seq DESC)",
		"CREATE INDEX IF NOT EXISTS idx_artifacts_stream ON artifacts(stream_id, path)",
		"CREATE INDEX IF NOT EXISTS idx_artifact_versions_artifact ON artifact_versions(artifact_id, version DESC)",
		"CREATE INDEX IF NOT EXISTS idx_artifact_versions_stream ON artifact_versions(stream_id, event_seq DESC)",
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	if err := ensureMemoryEntryColumns(db); err != nil {
		return err
	}
	if err := ensureArtifactVersionColumns(db); err != nil {
		return err
	}
	if _, err := db.Exec("INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)", CurrentSchemaVersion); err != nil {
		return fmt.Errorf("record schema migration: %w", err)
	}
	return nil
}

func ensureArtifactVersionColumns(db *sql.DB) error {
	columns, err := tableColumns(db, "artifact_versions")
	if err != nil {
		return err
	}
	if !columns["content_blob"] {
		if _, err := db.Exec("ALTER TABLE artifact_versions ADD COLUMN content_blob TEXT"); err != nil {
			return fmt.Errorf("add artifact_versions.content_blob: %w", err)
		}
	}
	return nil
}

func ensureMemoryEntryColumns(db *sql.DB) error {
	columns, err := tableColumns(db, "memory_entries")
	if err != nil {
		return err
	}
	definitions := map[string]string{
		"workspace":        "ALTER TABLE memory_entries ADD COLUMN workspace TEXT",
		"summary_level":    "ALTER TABLE memory_entries ADD COLUMN summary_level INTEGER NOT NULL DEFAULT 0",
		"source_event_seq": "ALTER TABLE memory_entries ADD COLUMN source_event_seq INTEGER NOT NULL DEFAULT 0",
		"importance":       "ALTER TABLE memory_entries ADD COLUMN importance REAL NOT NULL DEFAULT 0",
		"token_estimate":   "ALTER TABLE memory_entries ADD COLUMN token_estimate INTEGER NOT NULL DEFAULT 0",
		"expires_at":       "ALTER TABLE memory_entries ADD COLUMN expires_at TEXT",
	}
	for column, statement := range definitions {
		if columns[column] {
			continue
		}
		if _, err := db.Exec(statement); err != nil {
			return fmt.Errorf("add memory_entries.%s: %w", column, err)
		}
	}
	return nil
}

func tableColumns(db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return nil, fmt.Errorf("inspect table %s: %w", table, err)
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return nil, err
		}
		columns[name] = true
	}
	return columns, rows.Err()
}

func AppliedSchemaVersions(db *sql.DB) ([]int, error) {
	rows, err := db.Query("SELECT version FROM schema_migrations ORDER BY version ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}
	return versions, rows.Err()
}
