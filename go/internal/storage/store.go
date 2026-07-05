package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Event struct {
	ID        int64
	StreamID  string
	StreamSeq int64
	EventType string
	Payload   string
	ParentID  string
	Timestamp time.Time
}

type Store interface {
	AppendEvent(ctx context.Context, event AppendEvent) (Event, error)
	AppendEvents(ctx context.Context, events []AppendEvent) ([]Event, error)
	Append(ctx context.Context, req *WriteRequest) (*WriteResult, error)
	SaveSnapshot(ctx context.Context, snapshot SnapshotRecord) (SnapshotRecord, error)
	LatestSnapshot(streamID string, maxSeq int64) (SnapshotRecord, error)
	SaveProjectionSnapshot(ctx context.Context, snapshot ProjectionSnapshot) (ProjectionSnapshot, error)
	LatestProjectionSnapshot(streamID string) (ProjectionSnapshot, error)
	SaveAgentCheckpoint(ctx context.Context, checkpoint AgentCheckpoint) (AgentCheckpoint, error)
	GetAgentCheckpoint(ctx context.Context, id string) (AgentCheckpoint, error)
	ListAgentCheckpoints(ctx context.Context, streamID string, limit int) ([]AgentCheckpoint, error)
	RecordArtifactVersion(ctx context.Context, version ArtifactVersion) (ArtifactVersion, error)
	ListArtifacts(ctx context.Context, streamID string) ([]Artifact, error)
	ListArtifactVersions(ctx context.Context, streamID string, path string) ([]ArtifactVersion, error)
	GetArtifactVersion(ctx context.Context, streamID string, path string, version int) (ArtifactVersion, error)
	SaveMemoryEntry(ctx context.Context, entry MemoryEntry) (MemoryEntry, error)
	SearchMemory(ctx context.Context, query string, limit int) ([]MemoryEntry, error)
	SearchMemoryEntries(ctx context.Context, query MemorySearchQuery) ([]MemoryEntry, error)
	ForkStream(ctx context.Context, parentStreamID string, forkFromSeq int64, newQuery string) (string, error)
	GetLineage(streamID string) ([]string, error)
	GetChildStreams(streamID string) ([]string, error)
	ListStreams(limit int) ([]StreamSummary, error)
	Read(streamID string, fromSeq int64) ([]Event, error)
	LatestSeq(streamID string) (int64, error)
	Close() error
}

type StreamSummary struct {
	StreamID  string
	LatestSeq int64
	EventType string
	Timestamp time.Time
}

type SnapshotRecord struct {
	ID        int64
	StreamID  string
	StreamSeq int64
	Type      string
	Ref       string
	StateBlob string
	CreatedAt time.Time
}

type ProjectionSnapshot struct {
	StreamID  string
	StreamSeq int64
	StateBlob string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type MemoryEntry struct {
	ID             int64
	StreamID       string
	TurnID         string
	RunID          string
	Workspace      string
	Kind           string
	Content        string
	SummaryLevel   int
	SourceEventSeq int64
	Importance     float64
	TokenEstimate  int
	ExpiresAt      time.Time
	CreatedAt      time.Time
}

type MemorySearchQuery struct {
	Query          string
	Limit          int
	StreamID       string
	Workspace      string
	Kinds          []string
	IncludeExpired bool
}

type AgentCheckpoint struct {
	ID                  string
	StreamID            string
	TurnID              string
	RunID               string
	EventSeq            int64
	WorkflowType        string
	WorkflowPhase       string
	Reason              string
	ContextStateJSON    string
	MemoryStateJSON     string
	TokenStateJSON      string
	ToolStateJSON       string
	WorkspaceSnapshotID int64
	ArtifactManifestID  string
	CreatedAt           time.Time
}

type Artifact struct {
	ID                string
	StreamID          string
	Workspace         string
	Path              string
	ArtifactType      string
	CurrentVersionID  string
	CreatedByEventSeq int64
	CreatedBySpanID   string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type ArtifactVersion struct {
	ID                 string
	ArtifactID         string
	Version            int
	StreamID           string
	TurnID             string
	RunID              string
	Workspace          string
	Path               string
	ArtifactType       string
	EventSeq           int64
	ProducerSpanID     string
	ProducerLLMCallID  string
	ProducerToolCallID string
	ContentHash        string
	ContentBlob        string
	SizeBytes          int64
	SnapshotRef        string
	DiffRef            string
	Summary            string
	CreatedAt          time.Time
}

type ArtifactDiff struct {
	ID              string
	StreamID        string
	ArtifactID      string
	BeforeVersionID string
	AfterVersionID  string
	DiffFormat      string
	DiffText        string
	Reversible      bool
	CreatedAt       time.Time
}

type AppendEvent struct {
	StreamID  string
	EventType string
	Payload   any
	ParentID  string
}

type WriteRequest struct {
	Statements []WriteStatement
}

type WriteStatement struct {
	SQL  string
	Args []any
}

type WriteResult struct {
	LastInsertID int64
	RowsAffected int64
}

type SQLiteStore struct {
	db        *sql.DB
	writeCh   chan *writeJob
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// SQLiteStore 是当前 MVP 的持久化事件库。
// 所有写入通过单写队列串行化，避免 SQLite 多写竞争。
// AppendEvents 会统一补 schema_version 并做 secret redaction，
// 上层 workflow 不需要在每个事件里重复处理这些横切逻辑。
type writeJob struct {
	req    *WriteRequest
	append *appendJob
	fork   *forkJob
	result chan resultWithErr
}

type resultWithErr struct {
	result *WriteResult
	err    error
}

type SQLiteOptions struct {
	QueueSize int
}

func NewSQLiteStore(db *sql.DB, opts SQLiteOptions) *SQLiteStore {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 1024
	}
	s := &SQLiteStore{
		db:      db,
		writeCh: make(chan *writeJob, opts.QueueSize),
	}
	s.wg.Add(1)
	go s.loop()
	return s
}

func Open(path string, opts SQLiteOptions) (*SQLiteStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("create database dir: %w", err)
	}
	db, err := sql.Open("sqlite", "file:"+path+"?cache=shared&mode=rwc&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := InitSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return NewSQLiteStore(db, opts), nil
}

func (s *SQLiteStore) loop() {
	defer s.wg.Done()
	for job := range s.writeCh {
		if job.append != nil {
			events, err := s.execAppend(job.append)
			job.append.result <- appendResult{events: events, err: err}
			close(job.append.result)
			continue
		}
		if job.fork != nil {
			streamID, err := s.execFork(job.fork)
			job.fork.result <- forkResult{streamID: streamID, err: err}
			close(job.fork.result)
			continue
		}
		res, err := s.exec(job.req)
		job.result <- resultWithErr{result: res, err: err}
		close(job.result)
	}
}

func (s *SQLiteStore) exec(req *WriteRequest) (*WriteResult, error) {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	var lastResult sql.Result
	for _, stmt := range req.Statements {
		lastResult, err = tx.Exec(stmt.SQL, stmt.Args...)
		if err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	lastInsertID, _ := lastResult.LastInsertId()
	rowsAffected, _ := lastResult.RowsAffected()
	return &WriteResult{LastInsertID: lastInsertID, RowsAffected: rowsAffected}, nil
}

func (s *SQLiteStore) AppendEvent(ctx context.Context, event AppendEvent) (Event, error) {
	events, err := s.AppendEvents(ctx, []AppendEvent{event})
	if err != nil {
		return Event{}, err
	}
	return events[0], nil
}

func (s *SQLiteStore) AppendEvents(ctx context.Context, events []AppendEvent) ([]Event, error) {
	if len(events) == 0 {
		return nil, nil
	}
	job := &appendJob{
		events: events,
		result: make(chan appendResult, 1),
	}
	select {
	case s.writeCh <- &writeJob{append: job}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case out := <-job.result:
		return out.events, out.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *SQLiteStore) Append(ctx context.Context, req *WriteRequest) (*WriteResult, error) {
	job := &writeJob{
		req:    req,
		result: make(chan resultWithErr, 1),
	}
	select {
	case s.writeCh <- job:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case out := <-job.result:
		return out.result, out.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *SQLiteStore) SaveSnapshot(ctx context.Context, snapshot SnapshotRecord) (SnapshotRecord, error) {
	if snapshot.StreamID == "" {
		return SnapshotRecord{}, fmt.Errorf("stream_id is required")
	}
	if snapshot.StreamSeq <= 0 {
		return SnapshotRecord{}, fmt.Errorf("stream_seq must be positive")
	}
	if snapshot.Type != "git" && snapshot.Type != "archive" {
		return SnapshotRecord{}, fmt.Errorf("unsupported snapshot type %q", snapshot.Type)
	}
	if snapshot.Ref == "" {
		return SnapshotRecord{}, fmt.Errorf("snapshot_ref is required")
	}
	if snapshot.StateBlob == "" {
		snapshot.StateBlob = "{}"
	}
	result, err := s.Append(ctx, &WriteRequest{Statements: []WriteStatement{{
		SQL: `INSERT INTO snapshots(stream_id, stream_seq, snapshot_type, snapshot_ref, state_blob)
			VALUES(?,?,?,?,?)`,
		Args: []any{snapshot.StreamID, snapshot.StreamSeq, snapshot.Type, snapshot.Ref, snapshot.StateBlob},
	}}})
	if err != nil {
		return SnapshotRecord{}, err
	}
	snapshot.ID = result.LastInsertID
	snapshot.CreatedAt = time.Now().UTC()
	return snapshot, nil
}

func (s *SQLiteStore) LatestSnapshot(streamID string, maxSeq int64) (SnapshotRecord, error) {
	if streamID == "" {
		return SnapshotRecord{}, fmt.Errorf("stream_id is required")
	}
	if maxSeq <= 0 {
		maxSeq = 1<<63 - 1
	}
	var snapshot SnapshotRecord
	var createdAt string
	err := s.db.QueryRow(`
		SELECT id, stream_id, stream_seq, snapshot_type, snapshot_ref, state_blob, created_at
		FROM snapshots
		WHERE stream_id = ? AND stream_seq <= ?
		ORDER BY stream_seq DESC, id DESC
		LIMIT 1
	`, streamID, maxSeq).Scan(
		&snapshot.ID,
		&snapshot.StreamID,
		&snapshot.StreamSeq,
		&snapshot.Type,
		&snapshot.Ref,
		&snapshot.StateBlob,
		&createdAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SnapshotRecord{}, sql.ErrNoRows
	}
	if err != nil {
		return SnapshotRecord{}, err
	}
	if createdAt != "" {
		if parsed, err := time.Parse(time.RFC3339, createdAt); err == nil {
			snapshot.CreatedAt = parsed
		} else if parsed, err := time.Parse("2006-01-02 15:04:05", createdAt); err == nil {
			snapshot.CreatedAt = parsed
		}
	}
	return snapshot, nil
}

func (s *SQLiteStore) SaveProjectionSnapshot(ctx context.Context, snapshot ProjectionSnapshot) (ProjectionSnapshot, error) {
	if snapshot.StreamID == "" {
		return ProjectionSnapshot{}, fmt.Errorf("stream_id is required")
	}
	if snapshot.StreamSeq <= 0 {
		return ProjectionSnapshot{}, fmt.Errorf("stream_seq must be positive")
	}
	if snapshot.StateBlob == "" {
		return ProjectionSnapshot{}, fmt.Errorf("state_blob is required")
	}
	_, err := s.Append(ctx, &WriteRequest{Statements: []WriteStatement{{
		SQL: `INSERT INTO projection_snapshots(stream_id, stream_seq, state_blob)
			VALUES(?,?,?)
			ON CONFLICT(stream_id) DO UPDATE SET
				stream_seq = excluded.stream_seq,
				state_blob = excluded.state_blob,
				updated_at = datetime('now')`,
		Args: []any{snapshot.StreamID, snapshot.StreamSeq, snapshot.StateBlob},
	}}})
	if err != nil {
		return ProjectionSnapshot{}, err
	}
	now := time.Now().UTC()
	snapshot.CreatedAt = now
	snapshot.UpdatedAt = now
	return snapshot, nil
}

func (s *SQLiteStore) LatestProjectionSnapshot(streamID string) (ProjectionSnapshot, error) {
	if streamID == "" {
		return ProjectionSnapshot{}, fmt.Errorf("stream_id is required")
	}
	var snapshot ProjectionSnapshot
	var createdAt, updatedAt string
	err := s.db.QueryRow(`
		SELECT stream_id, stream_seq, state_blob, created_at, updated_at
		FROM projection_snapshots
		WHERE stream_id = ?
		LIMIT 1
	`, streamID).Scan(&snapshot.StreamID, &snapshot.StreamSeq, &snapshot.StateBlob, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ProjectionSnapshot{}, sql.ErrNoRows
	}
	if err != nil {
		return ProjectionSnapshot{}, err
	}
	snapshot.CreatedAt = parseSQLiteTime(createdAt)
	snapshot.UpdatedAt = parseSQLiteTime(updatedAt)
	return snapshot, nil
}

func (s *SQLiteStore) SaveAgentCheckpoint(ctx context.Context, checkpoint AgentCheckpoint) (AgentCheckpoint, error) {
	if checkpoint.StreamID == "" {
		return AgentCheckpoint{}, fmt.Errorf("stream_id is required")
	}
	if checkpoint.EventSeq <= 0 {
		return AgentCheckpoint{}, fmt.Errorf("event_seq must be positive")
	}
	if strings.TrimSpace(checkpoint.Reason) == "" {
		return AgentCheckpoint{}, fmt.Errorf("reason is required")
	}
	if checkpoint.ID == "" {
		checkpoint.ID = fmt.Sprintf("ckpt:%s:%d:%s", checkpoint.StreamID, checkpoint.EventSeq, sanitizeIDPart(checkpoint.Reason))
	}
	checkpoint.ContextStateJSON = defaultJSON(checkpoint.ContextStateJSON)
	checkpoint.MemoryStateJSON = defaultJSON(checkpoint.MemoryStateJSON)
	checkpoint.TokenStateJSON = defaultJSON(checkpoint.TokenStateJSON)
	checkpoint.ToolStateJSON = defaultJSON(checkpoint.ToolStateJSON)
	_, err := s.Append(ctx, &WriteRequest{Statements: []WriteStatement{{
		SQL: `INSERT INTO agent_checkpoints(
				id, stream_id, turn_id, run_id, event_seq, workflow_type, workflow_phase, reason,
				context_state_json, memory_state_json, token_state_json, tool_state_json,
				workspace_snapshot_id, artifact_manifest_id
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(id) DO UPDATE SET
				context_state_json = excluded.context_state_json,
				memory_state_json = excluded.memory_state_json,
				token_state_json = excluded.token_state_json,
				tool_state_json = excluded.tool_state_json,
				workspace_snapshot_id = excluded.workspace_snapshot_id,
				artifact_manifest_id = excluded.artifact_manifest_id`,
		Args: []any{
			checkpoint.ID,
			checkpoint.StreamID,
			checkpoint.TurnID,
			checkpoint.RunID,
			checkpoint.EventSeq,
			checkpoint.WorkflowType,
			checkpoint.WorkflowPhase,
			checkpoint.Reason,
			checkpoint.ContextStateJSON,
			checkpoint.MemoryStateJSON,
			checkpoint.TokenStateJSON,
			checkpoint.ToolStateJSON,
			nullableInt64(checkpoint.WorkspaceSnapshotID),
			checkpoint.ArtifactManifestID,
		},
	}}})
	if err != nil {
		return AgentCheckpoint{}, err
	}
	checkpoint.CreatedAt = time.Now().UTC()
	return checkpoint, nil
}

func (s *SQLiteStore) GetAgentCheckpoint(ctx context.Context, id string) (AgentCheckpoint, error) {
	if strings.TrimSpace(id) == "" {
		return AgentCheckpoint{}, fmt.Errorf("checkpoint id is required")
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT id, stream_id, turn_id, run_id, event_seq, workflow_type, workflow_phase, reason,
		       context_state_json, memory_state_json, token_state_json, tool_state_json,
		       workspace_snapshot_id, artifact_manifest_id, created_at
		FROM agent_checkpoints
		WHERE id = ?
	`, id)
	return scanAgentCheckpoint(row)
}

func (s *SQLiteStore) ListAgentCheckpoints(ctx context.Context, streamID string, limit int) ([]AgentCheckpoint, error) {
	if strings.TrimSpace(streamID) == "" {
		return nil, fmt.Errorf("stream_id is required")
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, stream_id, turn_id, run_id, event_seq, workflow_type, workflow_phase, reason,
		       context_state_json, memory_state_json, token_state_json, tool_state_json,
		       workspace_snapshot_id, artifact_manifest_id, created_at
		FROM agent_checkpoints
		WHERE stream_id = ?
		ORDER BY event_seq DESC
		LIMIT ?
	`, streamID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	checkpoints := []AgentCheckpoint{}
	for rows.Next() {
		checkpoint, err := scanAgentCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, checkpoint)
	}
	return checkpoints, rows.Err()
}

func (s *SQLiteStore) RecordArtifactVersion(ctx context.Context, version ArtifactVersion) (ArtifactVersion, error) {
	if version.StreamID == "" {
		return ArtifactVersion{}, fmt.Errorf("stream_id is required")
	}
	if strings.TrimSpace(version.Workspace) == "" {
		return ArtifactVersion{}, fmt.Errorf("workspace is required")
	}
	if strings.TrimSpace(version.Path) == "" {
		return ArtifactVersion{}, fmt.Errorf("path is required")
	}
	if strings.TrimSpace(version.ContentHash) == "" {
		return ArtifactVersion{}, fmt.Errorf("content_hash is required")
	}
	if version.EventSeq <= 0 {
		return ArtifactVersion{}, fmt.Errorf("event_seq must be positive")
	}
	if version.ArtifactType == "" {
		version.ArtifactType = "file"
	}
	if version.ArtifactID == "" {
		version.ArtifactID = fmt.Sprintf("artifact:%s:%s", version.StreamID, sanitizeIDPart(version.Path))
	}
	var currentMax sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT MAX(version)
		FROM artifact_versions
		WHERE artifact_id = ?
	`, version.ArtifactID).Scan(&currentMax)
	if err != nil {
		return ArtifactVersion{}, err
	}
	version.Version = int(currentMax.Int64) + 1
	if version.ID == "" {
		version.ID = fmt.Sprintf("%s:v%d", version.ArtifactID, version.Version)
	}
	var previousID, previousContent sql.NullString
	_ = s.db.QueryRowContext(ctx, `
		SELECT id, content_blob
		FROM artifact_versions
		WHERE artifact_id = ?
		ORDER BY version DESC
		LIMIT 1
	`, version.ArtifactID).Scan(&previousID, &previousContent)
	statements := []WriteStatement{
		{
			SQL: `INSERT INTO artifacts(id, stream_id, workspace, path, artifact_type, current_version_id, created_by_event_seq, created_by_span_id)
				VALUES(?,?,?,?,?,?,?,?)
				ON CONFLICT(stream_id, workspace, path) DO UPDATE SET
					artifact_type = excluded.artifact_type,
					current_version_id = excluded.current_version_id,
					updated_at = datetime('now')`,
			Args: []any{version.ArtifactID, version.StreamID, version.Workspace, version.Path, version.ArtifactType, version.ID, version.EventSeq, version.ProducerSpanID},
		},
		{
			SQL: `INSERT INTO artifact_versions(
					id, artifact_id, version, stream_id, turn_id, run_id, event_seq,
					producer_span_id, producer_llm_call_id, producer_tool_call_id,
					content_hash, content_blob, size_bytes, snapshot_ref, diff_ref, summary
				) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			Args: []any{
				version.ID,
				version.ArtifactID,
				version.Version,
				version.StreamID,
				version.TurnID,
				version.RunID,
				version.EventSeq,
				version.ProducerSpanID,
				version.ProducerLLMCallID,
				version.ProducerToolCallID,
				version.ContentHash,
				version.ContentBlob,
				version.SizeBytes,
				version.SnapshotRef,
				version.DiffRef,
				version.Summary,
			},
		},
	}
	if previousID.Valid && previousContent.Valid && previousContent.String != version.ContentBlob {
		diffID := fmt.Sprintf("diff:%s:v%d", version.ArtifactID, version.Version)
		version.DiffRef = diffID
		statements[1].Args[14] = version.DiffRef
		statements = append(statements, WriteStatement{
			SQL: `INSERT INTO artifact_diffs(id, stream_id, artifact_id, before_version_id, after_version_id, diff_format, diff_text, reversible)
				VALUES(?,?,?,?,?,?,?,?)`,
			Args: []any{diffID, version.StreamID, version.ArtifactID, previousID.String, version.ID, "simple", simpleTextDiff(previousContent.String, version.ContentBlob), 1},
		})
	}
	_, err = s.Append(ctx, &WriteRequest{Statements: statements})
	if err != nil {
		return ArtifactVersion{}, err
	}
	version.CreatedAt = time.Now().UTC()
	return version, nil
}

func (s *SQLiteStore) ListArtifacts(ctx context.Context, streamID string) ([]Artifact, error) {
	if strings.TrimSpace(streamID) == "" {
		return nil, fmt.Errorf("stream_id is required")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, stream_id, workspace, path, artifact_type, current_version_id, created_by_event_seq, created_by_span_id, created_at, updated_at
		FROM artifacts
		WHERE stream_id = ?
		ORDER BY path ASC
	`, streamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	artifacts := []Artifact{}
	for rows.Next() {
		artifact, err := scanArtifact(rows)
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, rows.Err()
}

func (s *SQLiteStore) ListArtifactVersions(ctx context.Context, streamID string, path string) ([]ArtifactVersion, error) {
	if strings.TrimSpace(streamID) == "" {
		return nil, fmt.Errorf("stream_id is required")
	}
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("path is required")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT v.id, v.artifact_id, v.version, v.stream_id, v.turn_id, v.run_id,
		       a.workspace, a.path, a.artifact_type, v.event_seq,
		       v.producer_span_id, v.producer_llm_call_id, v.producer_tool_call_id,
		       v.content_hash, v.content_blob, v.size_bytes, v.snapshot_ref, v.diff_ref, v.summary, v.created_at
		FROM artifact_versions v
		JOIN artifacts a ON a.id = v.artifact_id
		WHERE v.stream_id = ? AND a.path = ?
		ORDER BY v.version DESC
	`, streamID, path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	versions := []ArtifactVersion{}
	for rows.Next() {
		version, err := scanArtifactVersion(rows)
		if err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}
	return versions, rows.Err()
}

func (s *SQLiteStore) GetArtifactVersion(ctx context.Context, streamID string, path string, versionNumber int) (ArtifactVersion, error) {
	if strings.TrimSpace(streamID) == "" {
		return ArtifactVersion{}, fmt.Errorf("stream_id is required")
	}
	if strings.TrimSpace(path) == "" {
		return ArtifactVersion{}, fmt.Errorf("path is required")
	}
	if versionNumber <= 0 {
		return ArtifactVersion{}, fmt.Errorf("version must be positive")
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT v.id, v.artifact_id, v.version, v.stream_id, v.turn_id, v.run_id,
		       a.workspace, a.path, a.artifact_type, v.event_seq,
		       v.producer_span_id, v.producer_llm_call_id, v.producer_tool_call_id,
		       v.content_hash, v.content_blob, v.size_bytes, v.snapshot_ref, v.diff_ref, v.summary, v.created_at
		FROM artifact_versions v
		JOIN artifacts a ON a.id = v.artifact_id
		WHERE v.stream_id = ? AND a.path = ? AND v.version = ?
	`, streamID, path, versionNumber)
	return scanArtifactVersion(row)
}

func (s *SQLiteStore) SaveMemoryEntry(ctx context.Context, entry MemoryEntry) (MemoryEntry, error) {
	if entry.StreamID == "" {
		return MemoryEntry{}, fmt.Errorf("stream_id is required")
	}
	if entry.Kind == "" {
		return MemoryEntry{}, fmt.Errorf("memory kind is required")
	}
	if strings.TrimSpace(entry.Content) == "" {
		return MemoryEntry{}, fmt.Errorf("memory content is required")
	}
	if entry.TokenEstimate <= 0 {
		entry.TokenEstimate = estimateMemoryTokens(entry.Content)
	}
	result, err := s.Append(ctx, &WriteRequest{Statements: []WriteStatement{{
		SQL: `INSERT INTO memory_entries(stream_id, turn_id, run_id, workspace, kind, content, summary_level, source_event_seq, importance, token_estimate, expires_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		Args: []any{entry.StreamID, entry.TurnID, entry.RunID, entry.Workspace, entry.Kind, entry.Content, entry.SummaryLevel, entry.SourceEventSeq, entry.Importance, entry.TokenEstimate, sqliteTimeOrNil(entry.ExpiresAt)},
	}}})
	if err != nil {
		return MemoryEntry{}, err
	}
	entry.ID = result.LastInsertID
	entry.CreatedAt = time.Now().UTC()
	if _, err := s.Append(ctx, &WriteRequest{Statements: []WriteStatement{{
		SQL:  `INSERT INTO memory_entries_fts(rowid, content, kind, stream_id) VALUES(?,?,?,?)`,
		Args: []any{entry.ID, entry.Content, entry.Kind, entry.StreamID},
	}}}); err != nil {
		return MemoryEntry{}, err
	}
	return entry, nil
}

func (s *SQLiteStore) SearchMemory(ctx context.Context, query string, limit int) ([]MemoryEntry, error) {
	return s.SearchMemoryEntries(ctx, MemorySearchQuery{Query: query, Limit: limit})
}

func (s *SQLiteStore) SearchMemoryEntries(ctx context.Context, query MemorySearchQuery) ([]MemoryEntry, error) {
	if strings.TrimSpace(query.Query) == "" {
		return nil, fmt.Errorf("query is required")
	}
	limit := query.Limit
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	where := []string{"memory_entries_fts MATCH ?"}
	args := []any{query.Query}
	if query.StreamID != "" {
		where = append(where, "m.stream_id = ?")
		args = append(args, query.StreamID)
	}
	if query.Workspace != "" {
		where = append(where, "m.workspace = ?")
		args = append(args, query.Workspace)
	}
	kinds := compactStrings(query.Kinds)
	if len(kinds) > 0 {
		placeholders := make([]string, len(kinds))
		for i, kind := range kinds {
			placeholders[i] = "?"
			args = append(args, kind)
		}
		where = append(where, "m.kind IN ("+strings.Join(placeholders, ",")+")")
	}
	if !query.IncludeExpired {
		where = append(where, "(m.expires_at IS NULL OR m.expires_at = '' OR m.expires_at > datetime('now'))")
	}
	args = append(args, limit)
	sqlText := `
		SELECT m.id, m.stream_id, m.turn_id, m.run_id, m.workspace, m.kind, m.content,
		       m.summary_level, m.source_event_seq, m.importance, m.token_estimate, m.expires_at, m.created_at
		FROM memory_entries_fts f
		JOIN memory_entries m ON m.id = f.rowid
		WHERE ` + strings.Join(where, " AND ") + `
		ORDER BY rank
		LIMIT ?`
	rows, err := s.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := []MemoryEntry{}
	for rows.Next() {
		var entry MemoryEntry
		var createdAt string
		var expiresAt sql.NullString
		if err := rows.Scan(&entry.ID, &entry.StreamID, &entry.TurnID, &entry.RunID, &entry.Workspace, &entry.Kind, &entry.Content, &entry.SummaryLevel, &entry.SourceEventSeq, &entry.Importance, &entry.TokenEstimate, &expiresAt, &createdAt); err != nil {
			return nil, err
		}
		if expiresAt.Valid {
			entry.ExpiresAt = parseSQLiteTime(expiresAt.String)
		}
		entry.CreatedAt = parseSQLiteTime(createdAt)
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func compactStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

type checkpointScanner interface {
	Scan(dest ...any) error
}

func scanAgentCheckpoint(scanner checkpointScanner) (AgentCheckpoint, error) {
	var checkpoint AgentCheckpoint
	var workspaceSnapshotID sql.NullInt64
	var createdAt string
	err := scanner.Scan(
		&checkpoint.ID,
		&checkpoint.StreamID,
		&checkpoint.TurnID,
		&checkpoint.RunID,
		&checkpoint.EventSeq,
		&checkpoint.WorkflowType,
		&checkpoint.WorkflowPhase,
		&checkpoint.Reason,
		&checkpoint.ContextStateJSON,
		&checkpoint.MemoryStateJSON,
		&checkpoint.TokenStateJSON,
		&checkpoint.ToolStateJSON,
		&workspaceSnapshotID,
		&checkpoint.ArtifactManifestID,
		&createdAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentCheckpoint{}, sql.ErrNoRows
	}
	if err != nil {
		return AgentCheckpoint{}, err
	}
	if workspaceSnapshotID.Valid {
		checkpoint.WorkspaceSnapshotID = workspaceSnapshotID.Int64
	}
	checkpoint.CreatedAt = parseSQLiteTime(createdAt)
	return checkpoint, nil
}

func scanArtifact(scanner checkpointScanner) (Artifact, error) {
	var artifact Artifact
	var currentVersionID sql.NullString
	var createdBySpanID sql.NullString
	var createdAt, updatedAt string
	err := scanner.Scan(
		&artifact.ID,
		&artifact.StreamID,
		&artifact.Workspace,
		&artifact.Path,
		&artifact.ArtifactType,
		&currentVersionID,
		&artifact.CreatedByEventSeq,
		&createdBySpanID,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		return Artifact{}, err
	}
	artifact.CurrentVersionID = currentVersionID.String
	artifact.CreatedBySpanID = createdBySpanID.String
	artifact.CreatedAt = parseSQLiteTime(createdAt)
	artifact.UpdatedAt = parseSQLiteTime(updatedAt)
	return artifact, nil
}

func scanArtifactVersion(scanner checkpointScanner) (ArtifactVersion, error) {
	var version ArtifactVersion
	var producerSpanID, producerLLMCallID, producerToolCallID sql.NullString
	var contentBlob, snapshotRef, diffRef, summary sql.NullString
	var createdAt string
	err := scanner.Scan(
		&version.ID,
		&version.ArtifactID,
		&version.Version,
		&version.StreamID,
		&version.TurnID,
		&version.RunID,
		&version.Workspace,
		&version.Path,
		&version.ArtifactType,
		&version.EventSeq,
		&producerSpanID,
		&producerLLMCallID,
		&producerToolCallID,
		&version.ContentHash,
		&contentBlob,
		&version.SizeBytes,
		&snapshotRef,
		&diffRef,
		&summary,
		&createdAt,
	)
	if err != nil {
		return ArtifactVersion{}, err
	}
	version.ProducerSpanID = producerSpanID.String
	version.ProducerLLMCallID = producerLLMCallID.String
	version.ProducerToolCallID = producerToolCallID.String
	version.ContentBlob = contentBlob.String
	version.SnapshotRef = snapshotRef.String
	version.DiffRef = diffRef.String
	version.Summary = summary.String
	version.CreatedAt = parseSQLiteTime(createdAt)
	return version, nil
}

func defaultJSON(value string) string {
	if strings.TrimSpace(value) == "" {
		return "{}"
	}
	return value
}

func sanitizeIDPart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "checkpoint"
	}
	replacer := strings.NewReplacer(" ", "_", "/", "_", ":", "_", "\t", "_", "\n", "_")
	return replacer.Replace(value)
}

func nullableInt64(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}

func simpleTextDiff(before, after string) string {
	if before == after {
		return ""
	}
	return "--- before\n" + before + "\n+++ after\n" + after
}

func estimateMemoryTokens(content string) int {
	content = strings.TrimSpace(content)
	if content == "" {
		return 0
	}
	return (len(content) + 3) / 4
}

func sqliteTimeOrNil(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func (s *SQLiteStore) ForkStream(ctx context.Context, parentStreamID string, forkFromSeq int64, newQuery string) (string, error) {
	job := &forkJob{
		parentStreamID: parentStreamID,
		forkFromSeq:    forkFromSeq,
		newQuery:       newQuery,
		result:         make(chan forkResult, 1),
	}
	select {
	case s.writeCh <- &writeJob{fork: job}:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	select {
	case out := <-job.result:
		return out.streamID, out.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (s *SQLiteStore) GetLineage(streamID string) ([]string, error) {
	lineage := []string{}
	current := streamID
	for depth := 0; depth < 50 && current != ""; depth++ {
		lineage = append([]string{current}, lineage...)
		var parent sql.NullString
		err := s.db.QueryRow(`
			SELECT parent_id
			FROM event_log
			WHERE stream_id = ?
			ORDER BY stream_seq ASC
			LIMIT 1
		`, current).Scan(&parent)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return nil, err
		}
		if !parent.Valid || parent.String == "" {
			break
		}
		current = parent.String
	}
	return lineage, nil
}

func (s *SQLiteStore) GetChildStreams(streamID string) ([]string, error) {
	rows, err := s.db.Query(`
		SELECT DISTINCT stream_id
		FROM event_log
		WHERE parent_id = ? AND stream_id != ?
		ORDER BY stream_id
	`, streamID, streamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var children []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		children = append(children, id)
	}
	return children, rows.Err()
}

func (s *SQLiteStore) ListStreams(limit int) ([]StreamSummary, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.Query(`
		WITH latest AS (
			SELECT stream_id, MAX(stream_seq) AS latest_seq
			FROM event_log
			GROUP BY stream_id
		)
		SELECT e.stream_id, e.stream_seq, e.event_type, e.timestamp
		FROM event_log e
		JOIN latest l ON e.stream_id = l.stream_id AND e.stream_seq = l.latest_seq
		ORDER BY e.id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var summaries []StreamSummary
	for rows.Next() {
		var summary StreamSummary
		var ts string
		if err := rows.Scan(&summary.StreamID, &summary.LatestSeq, &summary.EventType, &ts); err != nil {
			return nil, err
		}
		if ts != "" {
			if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
				summary.Timestamp = parsed
			} else if parsed, err := time.Parse("2006-01-02 15:04:05", ts); err == nil {
				summary.Timestamp = parsed
			}
		}
		summaries = append(summaries, summary)
	}
	return summaries, rows.Err()
}

func (s *SQLiteStore) Read(streamID string, fromSeq int64) ([]Event, error) {
	if fromSeq <= 0 {
		fromSeq = 1
	}
	rows, err := s.db.Query(`
        SELECT id, stream_id, stream_seq, event_type, payload, parent_id, timestamp
        FROM event_log
        WHERE stream_id = ? AND stream_seq >= ?
        ORDER BY stream_seq ASC
    `, streamID, fromSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var evt Event
		var parentID sql.NullString
		var ts string
		if err := rows.Scan(&evt.ID, &evt.StreamID, &evt.StreamSeq, &evt.EventType, &evt.Payload, &parentID, &ts); err != nil {
			return nil, err
		}
		if parentID.Valid {
			evt.ParentID = parentID.String
		}
		if ts != "" {
			parsed, err := time.Parse(time.RFC3339, ts)
			if err != nil {
				// SQLite datetime('now') emits in "YYYY-MM-DD HH:MM:SS" so fallback.
				parsed, err = time.Parse("2006-01-02 15:04:05", ts)
				if err != nil {
					return nil, err
				}
			}
			evt.Timestamp = parsed
		}
		events = append(events, evt)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func (s *SQLiteStore) LatestSeq(streamID string) (int64, error) {
	var seq sql.NullInt64
	err := s.db.QueryRow(`
        SELECT MAX(stream_seq)
        FROM event_log
        WHERE stream_id = ?
    `, streamID).Scan(&seq)
	if err != nil {
		return 0, err
	}
	if !seq.Valid {
		return 0, nil
	}
	return seq.Int64, nil
}

func (s *SQLiteStore) Close() error {
	s.closeOnce.Do(func() {
		close(s.writeCh)
		s.wg.Wait()
	})
	return s.db.Close()
}

type appendJob struct {
	events []AppendEvent
	result chan appendResult
}

type appendResult struct {
	events []Event
	err    error
}

type forkJob struct {
	parentStreamID string
	forkFromSeq    int64
	newQuery       string
	result         chan forkResult
}

type forkResult struct {
	streamID string
	err      error
}

func (s *SQLiteStore) execAppend(job *appendJob) ([]Event, error) {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	nextSeqByStream := make(map[string]int64)
	appended := make([]Event, 0, len(job.events))
	for _, input := range job.events {
		if input.StreamID == "" {
			return nil, fmt.Errorf("stream_id is required")
		}
		if input.EventType == "" {
			return nil, fmt.Errorf("event_type is required")
		}
		payload, err := encodeEventPayload(input.Payload)
		if err != nil {
			return nil, fmt.Errorf("marshal payload: %w", err)
		}
		nextSeq, ok := nextSeqByStream[input.StreamID]
		if !ok {
			var latest sql.NullInt64
			if err := tx.QueryRow(
				"SELECT MAX(stream_seq) FROM event_log WHERE stream_id = ?",
				input.StreamID,
			).Scan(&latest); err != nil {
				return nil, err
			}
			if latest.Valid {
				nextSeq = latest.Int64 + 1
			} else {
				nextSeq = 1
			}
		}

		res, err := tx.Exec(
			"INSERT INTO event_log(stream_id, stream_seq, event_type, payload, parent_id) VALUES(?,?,?,?,?)",
			input.StreamID,
			nextSeq,
			input.EventType,
			string(payload),
			nullString(input.ParentID),
		)
		if err != nil {
			return nil, err
		}
		id, _ := res.LastInsertId()
		appended = append(appended, Event{
			ID:        id,
			StreamID:  input.StreamID,
			StreamSeq: nextSeq,
			EventType: input.EventType,
			Payload:   string(payload),
			ParentID:  input.ParentID,
			Timestamp: time.Now().UTC(),
		})
		nextSeqByStream[input.StreamID] = nextSeq + 1
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return appended, nil
}

func encodeEventPayload(payload any) ([]byte, error) {
	if payload == nil {
		return []byte(`{"schema_version":1}`), nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		return data, nil
	}
	if object == nil {
		return data, nil
	}
	if _, ok := object["schema_version"]; !ok {
		object["schema_version"] = 1
	}
	object = redactEventObject(object)
	return json.Marshal(object)
}

func redactEventObject(values map[string]any) map[string]any {
	out := make(map[string]any, len(values))
	for key, value := range values {
		if sensitiveKey(key) {
			out[key] = "[REDACTED]"
			continue
		}
		out[key] = redactEventValue(value)
	}
	return out
}

func redactEventValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return redactEventObject(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = redactEventValue(item)
		}
		return out
	default:
		return value
	}
}

func sensitiveKey(key string) bool {
	key = strings.ToLower(key)
	return strings.Contains(key, "api_key") ||
		strings.Contains(key, "apikey") ||
		strings.Contains(key, "authorization") ||
		strings.Contains(key, "password") ||
		strings.Contains(key, "secret") ||
		strings.Contains(key, "access_token") ||
		strings.Contains(key, "refresh_token")
}

func nullString(value string) sql.NullString {
	if value == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: value, Valid: true}
}

func parseSQLiteTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
		return parsed
	}
	return time.Time{}
}

func (s *SQLiteStore) execFork(job *forkJob) (string, error) {
	if job.parentStreamID == "" {
		return "", fmt.Errorf("parent_stream_id is required")
	}
	if job.forkFromSeq <= 0 {
		return "", fmt.Errorf("fork_from_seq must be positive")
	}
	newStreamID := fmt.Sprintf("%s/fork:%d", job.parentStreamID, job.forkFromSeq)
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return "", err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	res, err := tx.Exec(`
		INSERT INTO event_log(stream_id, stream_seq, event_type, payload, parent_id, timestamp)
		SELECT ?, stream_seq, event_type, payload, ?, timestamp
		FROM event_log
		WHERE stream_id = ? AND stream_seq <= ?
		ORDER BY stream_seq ASC
	`, newStreamID, job.parentStreamID, job.parentStreamID, job.forkFromSeq)
	if err != nil {
		return "", err
	}
	rows, _ := res.RowsAffected()
	if rows != job.forkFromSeq {
		return "", fmt.Errorf("fork source has %d events through seq %d", rows, job.forkFromSeq)
	}
	payload, err := json.Marshal(map[string]any{
		"forked_from": job.parentStreamID,
		"fork_at_seq": job.forkFromSeq,
		"new_query":   job.newQuery,
	})
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(
		"INSERT INTO event_log(stream_id, stream_seq, event_type, payload, parent_id) VALUES(?,?,?,?,?)",
		newStreamID,
		job.forkFromSeq+1,
		"ForkCreated",
		string(payload),
		job.parentStreamID,
	); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	committed = true
	return newStreamID, nil
}
