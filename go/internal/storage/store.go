package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	Read(streamID string, fromSeq int64) ([]Event, error)
	LatestSeq(streamID string) (int64, error)
	Close() error
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
		payload, err := json.Marshal(input.Payload)
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

func nullString(value string) sql.NullString {
	if value == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: value, Valid: true}
}
