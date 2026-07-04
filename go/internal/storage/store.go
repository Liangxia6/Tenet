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
	SaveMemoryEntry(ctx context.Context, entry MemoryEntry) (MemoryEntry, error)
	SearchMemory(ctx context.Context, query string, limit int) ([]MemoryEntry, error)
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
	ID        int64
	StreamID  string
	TurnID    string
	RunID     string
	Kind      string
	Content   string
	CreatedAt time.Time
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
	result, err := s.Append(ctx, &WriteRequest{Statements: []WriteStatement{{
		SQL: `INSERT INTO memory_entries(stream_id, turn_id, run_id, kind, content)
			VALUES(?,?,?,?,?)`,
		Args: []any{entry.StreamID, entry.TurnID, entry.RunID, entry.Kind, entry.Content},
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
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("query is required")
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT m.id, m.stream_id, m.turn_id, m.run_id, m.kind, m.content, m.created_at
		FROM memory_entries_fts f
		JOIN memory_entries m ON m.id = f.rowid
		WHERE memory_entries_fts MATCH ?
		ORDER BY rank
		LIMIT ?
	`, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := []MemoryEntry{}
	for rows.Next() {
		var entry MemoryEntry
		var createdAt string
		if err := rows.Scan(&entry.ID, &entry.StreamID, &entry.TurnID, &entry.RunID, &entry.Kind, &entry.Content, &createdAt); err != nil {
			return nil, err
		}
		entry.CreatedAt = parseSQLiteTime(createdAt)
		entries = append(entries, entry)
	}
	return entries, rows.Err()
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
