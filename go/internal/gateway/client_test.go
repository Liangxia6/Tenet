package gateway

import (
	"context"
	"net"
	"testing"
	"time"

	tenetv1 "github.com/tenet/orchestrator/internal/gateway/gen/tenet/v1"
	"github.com/tenet/orchestrator/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestOrchestratorRegisterAgent(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	registry := NewWorkerRegistry()
	orchestrator := NewOrchestratorServer("orchestrator-test", registry)
	go func() {
		_ = orchestrator.Serve(listener)
	}()
	t.Cleanup(func() {
		orchestrator.Stop()
		_ = listener.Close()
	})

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	response, err := tenetv1.NewTenetOrchestratorClient(conn).RegisterAgent(context.Background(), &tenetv1.RegisterAgentRequest{
		AgentId:        "worker-test",
		ListenPort:     50052,
		MaxConcurrency: 3,
	})
	if err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	if !response.Accepted || response.OrchestratorId != "orchestrator-test" {
		t.Fatalf("response = %+v", response)
	}
	if registry.Count() != 1 {
		t.Fatalf("registry count = %d, want 1", registry.Count())
	}
	lease, err := registry.Lease()
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	defer lease.Release()
	if lease.Worker().AgentID != "worker-test" || lease.Worker().MaxConcurrency != 3 {
		t.Fatalf("leased worker = %+v", lease.Worker())
	}
}

func TestWorkerClientRoundTrip(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	tenetv1.RegisterTenetWorkerServer(server, testWorkerService{})
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client, err := NewWorkerClient(ClientOptions{
		Address:          listener.Addr().String(),
		ControlTimeout:   time.Second,
		ExecuteTimeout:   time.Second,
		RetryMaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("NewWorkerClient: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})

	thought, err := client.GenerateThought(context.Background(), worker.GenerateThoughtRequest{
		SessionID:    "session-1",
		TaskID:       "task-1",
		SystemPrompt: "system",
		Messages: []worker.Message{{
			Role:    "user",
			Content: "hello",
		}},
		Tools: []worker.ToolDefinition{{
			Name:             "read_file",
			Description:      "read",
			ParametersSchema: "{}",
		}},
	})
	if err != nil {
		t.Fatalf("GenerateThought: %v", err)
	}
	if thought.Thought != "received task-1: hello" {
		t.Fatalf("thought = %q", thought.Thought)
	}
	if !thought.IsFinal || thought.Usage.TotalTokens != 7 {
		t.Fatalf("unexpected thought response: %+v", thought)
	}

	tool, err := client.ExecuteTool(context.Background(), worker.ExecuteToolRequest{
		SessionID: "session-1",
		Workspace: t.TempDir(),
		ToolName:  "shell",
		Arguments: `{"command":"pwd"}`,
	})
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	if tool.Stdout != "tool shell args {\"command\":\"pwd\"}" {
		t.Fatalf("stdout = %q", tool.Stdout)
	}

	health, err := client.HealthCheck(context.Background())
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if health.Status != "SERVING" || health.WorkerCount != 1 {
		t.Fatalf("health = %+v", health)
	}
}

type testWorkerService struct {
	tenetv1.UnimplementedTenetWorkerServer
}

func (testWorkerService) GenerateThought(_ context.Context, req *tenetv1.GenerateThoughtRequest) (*tenetv1.GenerateThoughtResponse, error) {
	content := ""
	if len(req.Messages) > 0 {
		content = req.Messages[len(req.Messages)-1].Content
	}
	return &tenetv1.GenerateThoughtResponse{
		Thought:      "received " + req.TaskId + ": " + content,
		IsFinal:      true,
		FinishReason: "stop",
		Usage:        &tenetv1.TokenUsage{TotalTokens: 7},
	}, nil
}

func (testWorkerService) ExecuteTool(_ context.Context, req *tenetv1.ExecuteToolRequest) (*tenetv1.ExecuteToolResponse, error) {
	return &tenetv1.ExecuteToolResponse{
		Stdout:     "tool " + req.ToolName + " args " + req.Arguments,
		ExitCode:   0,
		DurationMs: 1,
	}, nil
}

func (testWorkerService) HealthCheck(context.Context, *tenetv1.HealthCheckRequest) (*tenetv1.HealthCheckResponse, error) {
	return &tenetv1.HealthCheckResponse{Status: "SERVING", WorkerCount: 1}, nil
}
