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

type MemoryRef struct {
	ID     string  `json:"id,omitempty"`
	Kind   string  `json:"kind,omitempty"`
	Source string  `json:"source,omitempty"`
	Score  float64 `json:"score,omitempty"`
	Reason string  `json:"reason,omitempty"`
}

type MemoryBlock struct {
	Ref     MemoryRef
	Content string
}

type Options struct {
	SystemPrompt            string
	Messages                []worker.Message
	TokenBudget             int
	MaxMessages             int
	Strategy                string
	PrimerCount             int
	RecentCount             int
	CompressionTriggerRatio float64
	CompressionTargetRatio  float64
	MemoryRefs              []MemoryRef
	MemoryBlocks            []MemoryBlock
	MaxMemoryTokens         int
}

type Result struct {
	Messages         []worker.Message `json:"messages"`
	IncludedRefs     []EventRef       `json:"included_refs"`
	OmittedRefs      []EventRef       `json:"omitted_refs"`
	MemoryRefs       []MemoryRef      `json:"memory_refs,omitempty"`
	OriginalTokens   int              `json:"original_tokens,omitempty"`
	EstimatedTokens  int              `json:"estimated_tokens"`
	InputChars       int              `json:"input_chars"`
	Compacted        bool             `json:"compacted"`
	Strategy         string           `json:"strategy"`
	CompressionRatio float64          `json:"compression_ratio,omitempty"`
	TokensSaved      int              `json:"tokens_saved,omitempty"`
}

// Assemble 负责把 system prompt、历史消息、长期记忆和当前输入拼成模型上下文。
// 它同时输出 included/omitted refs 和 token 估算，方便 Trace 回答：
// “这次 LLM 到底看到了哪些上下文、丢弃了哪些上下文、用了多少预算”。
func Assemble(opts Options) Result {
	maxMessages := opts.MaxMessages
	if maxMessages <= 0 {
		maxMessages = 24
	}
	budget := opts.TokenBudget
	if budget <= 0 {
		budget = 8000
	}
	strategy := strings.TrimSpace(opts.Strategy)
	if strategy == "" {
		strategy = "default"
	}
	originalTokens := estimateTokens(len(opts.SystemPrompt) + messagesChars(opts.Messages))
	kept := shapeByWindow(opts.Messages, maxMessages, opts.PrimerCount, opts.RecentCount)
	omitted := omittedRefs(opts.Messages, kept)
	if len(omitted) > 0 {
		kept = injectCompressionSummary(kept, opts.PrimerCount, len(omitted))
	}
	kept, memoryRefs := injectMemoryBlocks(kept, opts.PrimerCount, opts.MemoryBlocks, opts.MemoryRefs, opts.MaxMemoryTokens)
	result := Result{
		Messages:       kept,
		OmittedRefs:    omitted,
		MemoryRefs:     memoryRefs,
		OriginalTokens: originalTokens,
		Strategy:       strategy,
		Compacted:      len(omitted) > 0,
	}
	result.recompute(opts.SystemPrompt, opts.Messages)
	triggerRatio := opts.CompressionTriggerRatio
	if triggerRatio <= 0 {
		triggerRatio = 1
	}
	triggerBudget := int(float64(budget) * triggerRatio)
	if triggerBudget <= 0 {
		triggerBudget = budget
	}
	for result.EstimatedTokens > triggerBudget && len(result.Messages) > 1 {
		result.OmittedRefs = appendUniqueRef(result.OmittedRefs, refForMessage(opts.Messages, result.Messages[0]))
		result.Messages = result.Messages[1:]
		result.Compacted = true
		result.recompute(opts.SystemPrompt, opts.Messages)
	}
	if originalTokens > 0 && result.Compacted {
		result.TokensSaved = max(0, originalTokens-result.EstimatedTokens)
		result.CompressionRatio = float64(result.TokensSaved) / float64(originalTokens)
	}
	return result
}

func shapeByWindow(messages []worker.Message, maxMessages, primerCount, recentCount int) []worker.Message {
	kept := append([]worker.Message(nil), messages...)
	if len(kept) <= maxMessages {
		return kept
	}
	if primerCount > 0 && recentCount > 0 && primerCount+recentCount < len(messages) {
		if primerCount >= maxMessages {
			primerCount = maxMessages / 2
		}
		if primerCount < 0 {
			primerCount = 0
		}
		maxRecent := maxMessages - primerCount - 1
		if maxRecent < 1 {
			maxRecent = 1
		}
		if recentCount > maxRecent {
			recentCount = maxRecent
		}
		kept = append([]worker.Message{}, messages[:primerCount]...)
		return append(kept, messages[len(messages)-recentCount:]...)
	}
	return kept[len(kept)-maxMessages:]
}

func omittedRefs(original, kept []worker.Message) []EventRef {
	refs := []EventRef{}
	used := map[int]bool{}
	for _, message := range kept {
		for i, candidate := range original {
			if used[i] {
				continue
			}
			if candidate.Role == message.Role && candidate.Content == message.Content && candidate.ToolCallID == message.ToolCallID {
				used[i] = true
				break
			}
		}
	}
	for i, message := range original {
		if !used[i] {
			refs = append(refs, messageRef(message, i))
		}
	}
	return refs
}

func injectCompressionSummary(messages []worker.Message, primerCount int, omittedCount int) []worker.Message {
	if omittedCount <= 0 || len(messages) == 0 || primerCount <= 0 || primerCount >= len(messages) {
		return messages
	}
	summary := worker.Message{Role: "system", Content: "Previous context summary: " + itoa(omittedCount) + " historical messages were omitted by the context window."}
	out := append([]worker.Message{}, messages[:primerCount]...)
	out = append(out, summary)
	out = append(out, messages[primerCount:]...)
	return out
}

func injectMemoryBlocks(messages []worker.Message, primerCount int, blocks []MemoryBlock, refs []MemoryRef, maxMemoryTokens int) ([]worker.Message, []MemoryRef) {
	memoryRefs := append([]MemoryRef(nil), refs...)
	if len(blocks) == 0 {
		return messages, memoryRefs
	}
	maxChars := maxMemoryTokens * 4
	if maxChars <= 0 {
		maxChars = 32000
	}
	var b strings.Builder
	b.WriteString("Retrieved memories:\n")
	usedChars := b.Len()
	for _, block := range blocks {
		content := strings.TrimSpace(block.Content)
		if content == "" {
			continue
		}
		prefix := "- "
		if block.Ref.Kind != "" || block.Ref.ID != "" {
			prefix += "[" + firstNonEmpty(block.Ref.Kind, "memory") + ":" + block.Ref.ID + "] "
		}
		line := prefix + content
		if usedChars+len(line)+1 > maxChars {
			remaining := maxChars - usedChars - len(prefix) - len("...truncated...")
			if remaining <= 0 {
				break
			}
			line = prefix + truncateBytes(content, remaining) + "...truncated..."
		}
		b.WriteString(line)
		b.WriteByte('\n')
		usedChars += len(line) + 1
		memoryRefs = append(memoryRefs, block.Ref)
		if usedChars >= maxChars {
			break
		}
	}
	if len(memoryRefs) == len(refs) {
		return messages, memoryRefs
	}
	memoryMessage := worker.Message{Role: "system", Content: strings.TrimSpace(b.String())}
	insertAt := primerCount
	if insertAt < 0 {
		insertAt = 0
	}
	if insertAt > len(messages) {
		insertAt = len(messages)
	}
	out := append([]worker.Message{}, messages[:insertAt]...)
	out = append(out, memoryMessage)
	out = append(out, messages[insertAt:]...)
	return out, memoryRefs
}

func truncateBytes(value string, maxBytes int) string {
	if maxBytes <= 0 || len(value) <= maxBytes {
		if maxBytes <= 0 {
			return ""
		}
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

func messagesChars(messages []worker.Message) int {
	total := 0
	for _, message := range messages {
		total += messageChars(message)
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
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
