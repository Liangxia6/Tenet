package assembler

import (
	"strings"
	"testing"

	"github.com/tenet/orchestrator/internal/worker"
)

func TestAssembleKeepsRecentMessagesWithinLimits(t *testing.T) {
	result := Assemble(Options{
		SystemPrompt: "system prompt",
		Messages: []worker.Message{
			{Role: "user", Content: "one"},
			{Role: "assistant", Content: "two"},
			{Role: "user", Content: "three"},
		},
		MaxMessages: 2,
		TokenBudget: 100,
	})
	if len(result.Messages) != 2 {
		t.Fatalf("messages = %+v", result.Messages)
	}
	if result.Messages[0].Content != "two" || result.Messages[1].Content != "three" {
		t.Fatalf("messages = %+v", result.Messages)
	}
	if !result.Compacted || len(result.OmittedRefs) == 0 {
		t.Fatalf("compaction refs = %+v", result)
	}
	if result.EstimatedTokens <= 0 || result.InputChars <= 0 {
		t.Fatalf("size estimate = %+v", result)
	}
}

func TestAssembleCompactsByTokenBudget(t *testing.T) {
	result := Assemble(Options{
		SystemPrompt: "system prompt",
		Messages: []worker.Message{
			{Role: "user", Content: "this is a long historical message that should be omitted first"},
			{Role: "assistant", Content: "this is another long historical message that should be omitted second"},
			{Role: "user", Content: "current request"},
		},
		MaxMessages: 10,
		TokenBudget: 10,
	})
	if len(result.Messages) >= 3 {
		t.Fatalf("expected compaction, got %+v", result.Messages)
	}
	if !result.Compacted || len(result.OmittedRefs) == 0 {
		t.Fatalf("compaction refs = %+v", result)
	}
	if result.Messages[len(result.Messages)-1].Content != "current request" {
		t.Fatalf("latest message should be retained: %+v", result.Messages)
	}
}

func TestAssembleUsesPrimerSummaryAndRecentWindow(t *testing.T) {
	longHistory := "long historical detail that should be represented by the summary rather than kept verbatim. "
	messages := []worker.Message{
		{Role: "user", Content: "initial goal"},
		{Role: "assistant", Content: longHistory + longHistory + longHistory},
		{Role: "user", Content: longHistory + longHistory},
		{Role: "assistant", Content: "middle two"},
		{Role: "user", Content: "latest request"},
	}
	result := Assemble(Options{
		SystemPrompt:            "system prompt",
		Messages:                messages,
		MaxMessages:             4,
		TokenBudget:             1000,
		Strategy:                "coding",
		PrimerCount:             1,
		RecentCount:             2,
		CompressionTriggerRatio: 0.75,
		MemoryRefs:              []MemoryRef{{ID: "mem:1", Kind: "workspace_summary", Source: "sqlite_fts", Score: 0.8}},
	})
	if result.Strategy != "coding" {
		t.Fatalf("strategy = %q", result.Strategy)
	}
	if len(result.Messages) != 4 {
		t.Fatalf("messages = %+v", result.Messages)
	}
	if result.Messages[0].Content != "initial goal" {
		t.Fatalf("primer not retained: %+v", result.Messages)
	}
	if result.Messages[1].Role != "system" || result.Messages[1].Content == "" {
		t.Fatalf("summary not injected: %+v", result.Messages)
	}
	if result.Messages[3].Content != "latest request" {
		t.Fatalf("recent message not retained: %+v", result.Messages)
	}
	if !result.Compacted || len(result.OmittedRefs) != 2 || result.TokensSaved <= 0 {
		t.Fatalf("compaction metadata = %+v", result)
	}
	if len(result.MemoryRefs) != 1 || result.MemoryRefs[0].ID != "mem:1" {
		t.Fatalf("memory refs = %+v", result.MemoryRefs)
	}
}

func TestAssembleInjectsRetrievedMemories(t *testing.T) {
	result := Assemble(Options{
		SystemPrompt: "system prompt",
		Messages: []worker.Message{
			{Role: "user", Content: "initial goal"},
			{Role: "user", Content: "current request"},
		},
		MaxMessages:     10,
		TokenBudget:     1000,
		PrimerCount:     1,
		MaxMemoryTokens: 20,
		MemoryBlocks: []MemoryBlock{{
			Ref:     MemoryRef{ID: "42", Kind: "workspace_summary", Source: "sqlite_fts", Score: 0.9, Reason: "query match"},
			Content: "Tenet keeps project context and previous code decisions for future turns. 记忆内容",
		}},
	})
	if len(result.MemoryRefs) != 1 || result.MemoryRefs[0].ID != "42" {
		t.Fatalf("memory refs = %+v", result.MemoryRefs)
	}
	if len(result.Messages) != 3 {
		t.Fatalf("messages = %+v", result.Messages)
	}
	if result.Messages[1].Role != "system" || !strings.Contains(result.Messages[1].Content, "Retrieved memories") {
		t.Fatalf("memory message = %+v", result.Messages)
	}
	if !strings.Contains(result.Messages[1].Content, "...truncated...") {
		t.Fatalf("expected memory truncation, got %q", result.Messages[1].Content)
	}
}
