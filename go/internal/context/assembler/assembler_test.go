package assembler

import (
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
