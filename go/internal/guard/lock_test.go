package guard

import (
	"strings"
	"testing"
	"time"
)

func TestLocalLockManagerExcludesOtherAgents(t *testing.T) {
	manager := NewLocalLockManager(time.Minute)
	lease, err := manager.Acquire("session:1", "agent-a")
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	if lease.Token != 1 {
		t.Fatalf("token = %d, want 1", lease.Token)
	}

	_, err = manager.Acquire("session:1", "agent-b")
	if err == nil || !strings.Contains(err.Error(), "locked by another agent") {
		t.Fatalf("expected locked error, got %v", err)
	}
}

func TestLocalLockManagerValidateRejectsOldToken(t *testing.T) {
	manager := NewLocalLockManager(time.Minute)
	oldLease, err := manager.Acquire("session:1", "agent-a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	newLease, err := manager.Renew(oldLease)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if manager.Validate(oldLease) {
		t.Fatalf("old lease should not validate")
	}
	if !manager.Validate(newLease) {
		t.Fatalf("new lease should validate")
	}
}
