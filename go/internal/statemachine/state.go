package statemachine

import "fmt"

const (
	StatusRunning   = "RUNNING"
	StatusPaused    = "PAUSED"
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
)

func CanTransition(from, to string) bool {
	if to == "" || from == to {
		return true
	}
	switch from {
	case "":
		return to == StatusRunning || to == StatusPaused || to == StatusCompleted || to == StatusFailed
	case StatusRunning:
		return to == StatusPaused || to == StatusCompleted || to == StatusFailed
	case StatusPaused:
		return to == StatusRunning || to == StatusCompleted || to == StatusFailed
	case StatusCompleted, StatusFailed:
		return false
	default:
		return false
	}
}

func ValidateTransition(entity, id, from, to string) error {
	if CanTransition(from, to) {
		return nil
	}
	if id == "" {
		return fmt.Errorf("invalid %s state transition: %s -> %s", entity, from, to)
	}
	return fmt.Errorf("invalid %s state transition for %s: %s -> %s", entity, id, from, to)
}
