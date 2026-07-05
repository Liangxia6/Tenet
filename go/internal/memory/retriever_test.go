package memory

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/tenet/orchestrator/internal/storage"
	_ "modernc.org/sqlite"
)

func testStore(t *testing.T) storage.Store {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := storage.InitSchema(db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	return storage.NewSQLiteStore(db, storage.SQLiteOptions{})
}

func TestSQLiteRetrieverFiltersAndTrimsMemory(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	defer store.Close()
	if _, err := store.SaveMemoryEntry(ctx, storage.MemoryEntry{
		StreamID: "session:1",
		Kind:     "workspace_summary",
		Content:  strings.Repeat("parser bug fixed with context assembler memory ", 8),
	}); err != nil {
		t.Fatalf("save memory: %v", err)
	}
	if _, err := store.SaveMemoryEntry(ctx, storage.MemoryEntry{
		StreamID: "session:2",
		Kind:     "workspace_summary",
		Content:  "parser bug from another session",
	}); err != nil {
		t.Fatalf("save memory: %v", err)
	}
	retriever := NewSQLiteRetriever(store)
	results, err := retriever.Retrieve(ctx, RetrievalQuery{
		Query:     "parser bug",
		StreamID:  "session:1",
		Limit:     4,
		MaxTokens: 12,
	})
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %+v", results)
	}
	if results[0].Source != "sqlite_fts" || results[0].Kind != "workspace_summary" {
		t.Fatalf("result metadata = %+v", results[0])
	}
	if len(results[0].Content) > 12*4 {
		t.Fatalf("content was not trimmed: %d", len(results[0].Content))
	}
}

func TestBuildFTSQueryDropsPunctuation(t *testing.T) {
	query := buildFTSQuery(`Fix parser.go: "token" bug!`)
	if query != `"fix" OR "parser" OR "go" OR "token" OR "bug"` {
		t.Fatalf("query = %q", query)
	}
}
