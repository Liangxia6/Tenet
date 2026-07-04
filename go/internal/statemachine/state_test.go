package statemachine

import "testing"

func TestCanTransition(t *testing.T) {
	valid := [][2]string{
		{"", StatusRunning},
		{StatusRunning, StatusCompleted},
		{StatusRunning, StatusFailed},
		{StatusRunning, StatusPaused},
		{StatusPaused, StatusRunning},
		{StatusCompleted, StatusCompleted},
	}
	for _, item := range valid {
		if !CanTransition(item[0], item[1]) {
			t.Fatalf("transition %s -> %s should be valid", item[0], item[1])
		}
	}
	invalid := [][2]string{
		{StatusCompleted, StatusRunning},
		{StatusFailed, StatusRunning},
		{StatusCompleted, StatusFailed},
	}
	for _, item := range invalid {
		if CanTransition(item[0], item[1]) {
			t.Fatalf("transition %s -> %s should be invalid", item[0], item[1])
		}
	}
}
