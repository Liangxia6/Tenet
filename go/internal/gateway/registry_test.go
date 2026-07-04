package gateway

import "testing"

func TestWorkerRegistryHeartbeatAndLifecycle(t *testing.T) {
	registry := NewWorkerRegistry()
	worker, err := registry.Register("worker-a", 50051, 2, "127.0.0.1")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if worker.Status != "healthy" || worker.LastHeartbeat == 0 {
		t.Fatalf("worker = %+v", worker)
	}
	if err := registry.Heartbeat("worker-a", []string{"tools", "llm"}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	snapshot := registry.Snapshot()
	if len(snapshot) != 1 || len(snapshot[0].Capabilities) != 2 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if err := registry.MarkUnhealthy("worker-a", "unhealthy"); err != nil {
		t.Fatalf("mark unhealthy: %v", err)
	}
	if _, err := registry.Lease(); err != ErrNoAvailableWorker {
		t.Fatalf("Lease err = %v, want ErrNoAvailableWorker", err)
	}
	if err := registry.Heartbeat("worker-a", []string{"tools"}); err != nil {
		t.Fatalf("heartbeat healthy: %v", err)
	}
	lease, err := registry.Lease()
	if err != nil {
		t.Fatalf("lease healthy: %v", err)
	}
	lease.Release()
	registry.Unregister("worker-a")
	if registry.Count() != 0 {
		t.Fatalf("count = %d, want 0", registry.Count())
	}
}
