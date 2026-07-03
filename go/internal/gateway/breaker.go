package gateway

import (
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("worker circuit breaker is open")

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

type CircuitBreaker struct {
	mu          sync.Mutex
	failMax     int
	cooldown    time.Duration
	state       breakerState
	failures    int
	openedAt    time.Time
	halfOpenUse bool
}

func NewCircuitBreaker(failMax int, cooldown time.Duration) *CircuitBreaker {
	if failMax <= 0 {
		failMax = 5
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	return &CircuitBreaker{failMax: failMax, cooldown: cooldown}
}

func (b *CircuitBreaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != breakerOpen {
		if b.state == breakerHalfOpen {
			if b.halfOpenUse {
				return ErrCircuitOpen
			}
			b.halfOpenUse = true
		}
		return nil
	}
	if time.Since(b.openedAt) < b.cooldown {
		return ErrCircuitOpen
	}
	b.state = breakerHalfOpen
	b.halfOpenUse = true
	return nil
}

func (b *CircuitBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = breakerClosed
	b.failures = 0
	b.halfOpenUse = false
}

func (b *CircuitBreaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.halfOpenUse = false
	b.failures++
	if b.failures >= b.failMax || b.state == breakerHalfOpen {
		b.state = breakerOpen
		b.openedAt = time.Now()
	}
}
