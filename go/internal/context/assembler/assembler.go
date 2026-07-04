package assembler

import (
	"strings"

	"github.com/tenet/orchestrator/internal/worker"
)

type EventRef struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
	Role  string `json:"role,omitempty"`
	Chars int    `json:"chars,omitempty"`
}

type Options struct {
	SystemPrompt string
	Messages     []worker.Message
	TokenBudget  int
	MaxMessages  int
}

type Result struct {
	Messages        []worker.Message `json:"messages"`
	IncludedRefs    []EventRef       `json:"included_refs"`
	OmittedRefs     []EventRef       `json:"omitted_refs"`
	EstimatedTokens int              `json:"estimated_tokens"`
	InputChars      int              `json:"input_chars"`
	Compacted       bool             `json:"compacted"`
}

func Assemble(opts Options) Result {
	maxMessages := opts.MaxMessages
	if maxMessages <= 0 {
		maxMessages = 24
	}
	budget := opts.TokenBudget
	if budget <= 0 {
		budget = 8000
	}
	kept := append([]worker.Message(nil), opts.Messages...)
	if len(kept) > maxMessages {
		kept = kept[len(kept)-maxMessages:]
	}
	result := Result{Messages: kept}
	result.recompute(opts.SystemPrompt, opts.Messages)
	for result.EstimatedTokens > budget && len(result.Messages) > 1 {
		result.OmittedRefs = append(result.OmittedRefs, refForMessage(opts.Messages, result.Messages[0]))
		result.Messages = result.Messages[1:]
		result.Compacted = true
		result.recompute(opts.SystemPrompt, opts.Messages)
	}
	if len(opts.Messages) > len(result.Messages) {
		result.Compacted = true
		omittedPrefix := len(opts.Messages) - len(result.Messages)
		for i := 0; i < omittedPrefix; i++ {
			result.OmittedRefs = appendUniqueRef(result.OmittedRefs, messageRef(opts.Messages[i], i))
		}
	}
	return result
}

func (r *Result) recompute(systemPrompt string, original []worker.Message) {
	r.InputChars = len(systemPrompt)
	r.IncludedRefs = r.IncludedRefs[:0]
	for _, message := range r.Messages {
		r.InputChars += messageChars(message)
		r.IncludedRefs = append(r.IncludedRefs, refForMessage(original, message))
	}
	r.EstimatedTokens = estimateTokens(r.InputChars)
}

func estimateTokens(chars int) int {
	if chars <= 0 {
		return 0
	}
	return (chars + 3) / 4
}

func refForMessage(messages []worker.Message, target worker.Message) EventRef {
	for i, message := range messages {
		if message.Role == target.Role && message.Content == target.Content && message.ToolCallID == target.ToolCallID {
			return messageRef(message, i)
		}
	}
	return messageRef(target, -1)
}

func messageRef(message worker.Message, index int) EventRef {
	return EventRef{Type: "message", Index: index, Role: message.Role, Chars: messageChars(message)}
}

func messageChars(message worker.Message) int {
	total := len(message.Role) + len(message.Content) + len(message.ToolCallID)
	for _, call := range message.ToolCalls {
		total += len(call.CallID) + len(call.ToolName) + len(call.Arguments)
	}
	return total
}

func appendUniqueRef(refs []EventRef, ref EventRef) []EventRef {
	for _, existing := range refs {
		if existing.Type == ref.Type && existing.Index == ref.Index && existing.Role == ref.Role {
			return refs
		}
	}
	return append(refs, ref)
}

func Summary(result Result) string {
	parts := []string{
		"messages=" + itoa(len(result.Messages)),
		"tokens~=" + itoa(result.EstimatedTokens),
	}
	if result.Compacted {
		parts = append(parts, "compacted")
	}
	return strings.Join(parts, " ")
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	negative := value < 0
	if negative {
		value = -value
	}
	for value > 0 {
		i--
		b[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
