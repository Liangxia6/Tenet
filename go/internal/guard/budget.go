package guard

import (
	"errors"
	"sync"

	"github.com/tenet/orchestrator/internal/worker"
)

type TokenBudget struct {
	limit int
	mu    sync.Mutex
	used  map[string]worker.TokenUsage
}

func NewTokenBudget(limit int) *TokenBudget {
	if limit <= 0 {
		limit = 100000
	}
	return &TokenBudget{
		limit: limit,
		used:  make(map[string]worker.TokenUsage),
	}
}

func (b *TokenBudget) Record(taskID string, usage worker.TokenUsage) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	current := b.used[taskID]
	current.PromptTokens += usage.PromptTokens
	current.CompletionTokens += usage.CompletionTokens
	current.TotalTokens += usage.TotalTokens
	current.CostUSD += usage.CostUSD
	if current.TotalTokens > b.limit {
		return errors.New("token budget exceeded")
	}
	b.used[taskID] = current
	return nil
}

func (b *TokenBudget) Usage(taskID string) worker.TokenUsage {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used[taskID]
}
