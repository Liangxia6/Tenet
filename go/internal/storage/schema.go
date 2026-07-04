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
			kind TEXT NOT NULL,
			content TEXT NOT NULL,
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
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	if _, err := db.Exec("INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)", CurrentSchemaVersion); err != nil {
		return fmt.Errorf("record schema migration: %w", err)
	}
	return nil
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
