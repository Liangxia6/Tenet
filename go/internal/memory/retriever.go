package memory

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/tenet/orchestrator/internal/storage"
)

type RetrievalQuery struct {
	Query          string
	StreamID       string
	Workspace      string
	Limit          int
	MaxTokens      int
	Kinds          []string
	CrossSession   bool
	CrossWorkspace bool
}

type RetrievedMemory struct {
	ID      string
	Kind    string
	Source  string
	Content string
	Score   float64
	Reason  string
}

type Retriever interface {
	Retrieve(ctx context.Context, query RetrievalQuery) ([]RetrievedMemory, error)
}

type SQLiteRetriever struct {
	store storage.Store
}

func NewSQLiteRetriever(store storage.Store) *SQLiteRetriever {
	return &SQLiteRetriever{store: store}
}

func (r *SQLiteRetriever) Retrieve(ctx context.Context, query RetrievalQuery) ([]RetrievedMemory, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("memory store is nil")
	}
	match := buildFTSQuery(query.Query)
	if match == "" {
		return nil, nil
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 8
	}
	candidateLimit := limit * 4
	if candidateLimit < 20 {
		candidateLimit = 20
	}
	searchQuery := storage.MemorySearchQuery{
		Query: match,
		Limit: candidateLimit,
		Kinds: query.Kinds,
	}
	if !query.CrossSession {
		searchQuery.StreamID = query.StreamID
	}
	if !query.CrossWorkspace {
		searchQuery.Workspace = query.Workspace
	}
	entries, err := r.store.SearchMemoryEntries(ctx, searchQuery)
	if err != nil {
		return nil, err
	}
	allowedKinds := map[string]bool{}
	for _, kind := range query.Kinds {
		kind = strings.TrimSpace(kind)
		if kind != "" {
			allowedKinds[kind] = true
		}
	}
	maxChars := query.MaxTokens * 4
	if maxChars <= 0 {
		maxChars = 32000
	}
	usedChars := 0
	results := make([]RetrievedMemory, 0, limit)
	seen := map[int64]bool{}
	for i, entry := range entries {
		if seen[entry.ID] {
			continue
		}
		seen[entry.ID] = true
		if len(allowedKinds) > 0 && !allowedKinds[entry.Kind] {
			continue
		}
		content := strings.TrimSpace(entry.Content)
		if content == "" {
			continue
		}
		if usedChars+len(content) > maxChars {
			remaining := maxChars - usedChars
			if remaining <= 0 {
				break
			}
			content = truncateBytes(content, remaining)
		}
		results = append(results, RetrievedMemory{
			ID:      fmt.Sprintf("%d", entry.ID),
			Kind:    entry.Kind,
			Source:  "sqlite_fts",
			Content: content,
			Score:   1.0 / float64(i+1),
			Reason:  "FTS match against current query",
		})
		usedChars += len(content)
		if len(results) >= limit {
			break
		}
	}
	return results, nil
}

func buildFTSQuery(query string) string {
	terms := tokenizeQuery(query)
	if len(terms) == 0 {
		return ""
	}
	if len(terms) > 8 {
		terms = terms[:8]
	}
	quoted := make([]string, 0, len(terms))
	for _, term := range terms {
		quoted = append(quoted, `"`+strings.ReplaceAll(term, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, " OR ")
}

func tokenizeQuery(query string) []string {
	terms := []string{}
	var b strings.Builder
	flush := func() {
		value := strings.TrimSpace(b.String())
		b.Reset()
		if len([]rune(value)) >= 2 {
			terms = append(terms, value)
		}
	}
	for _, r := range strings.ToLower(query) {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' {
			b.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return terms
}

func truncateBytes(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}
	end := 0
	for i := range value {
		if i > maxBytes {
			break
		}
		end = i
	}
	if end == 0 {
		return ""
	}
	return value[:end]
}
