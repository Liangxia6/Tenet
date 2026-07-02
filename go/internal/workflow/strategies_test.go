package workflow

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/worker"
)

type scriptedGenerator struct {
	responses []worker.GenerateThoughtResponse
	requests  []worker.GenerateThoughtRequest
}

func (c *scriptedGenerator) GenerateThought(_ context.Context, req worker.GenerateThoughtRequest) (worker.GenerateThoughtResponse, error) {
	c.requests = append(c.requests, req)
	if len(c.responses) == 0 {
		return worker.GenerateThoughtResponse{Thought: "done", IsFinal: true, FinishReason: "stop"}, nil
	}
	resp := c.responses[0]
	c.responses = c.responses[1:]
	return resp, nil
}

func (c *scriptedGenerator) ExecuteTool(context.Context, worker.ExecuteToolRequest) (worker.ExecuteToolResponse, error) {
	return worker.ExecuteToolResponse{}, nil
}

func (c *scriptedGenerator) HealthCheck(context.Context) (worker.HealthCheckResponse, error) {
	return worker.HealthCheckResponse{Status: "SERVING", WorkerCount: 1}, nil
}

func TestReactWorkflowExecutesLocalToolAndRecordsEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()

	workspace := t.TempDir()
	if err := os.WriteFile(workspace+"/README.md", []byte("Tenet reads this file."), 0644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	generator := &scriptedGenerator{
		responses: []worker.GenerateThoughtResponse{
			{
				Thought:      "I should inspect the README.",
				FinishReason: "tool_calls",
				ToolCalls: []worker.ToolCall{{
					CallID:    "call_readme",
					ToolName:  "read_file",
					Arguments: `{"path":"README.md"}`,
				}},
			},
			{
				Thought:      "The README says: Tenet reads this file.",
				IsFinal:      true,
				FinishReason: "stop",
			},
		},
	}
	cfg := config.Default()
	cfg.Agent.DefaultMaxSteps = 4
	cfg.Agent.ConvergenceNoToolCalls = 1
	cfg.Workflow.RecordBatchSize = 20

	result, err := Execute(ctx, store, NewRegistry(), &TaskHandle{
		StreamID:     "task:react-tool",
		WorkflowType: "react",
		SessionID:    "task:react-tool",
		Query:        "Summarize README.md",
		Workspace:    workspace,
		SystemPrompt: "Use tools.",
		Tools:        worker.BuiltinToolDefinitions(),
		Config:       cfg,
		Client:       worker.NewLocalAgentClient(generator, workspace, nil),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Result != "The README says: Tenet reads this file." {
		t.Fatalf("result = %v", result.Result)
	}
	if len(generator.requests) != 2 {
		t.Fatalf("generate calls = %d, want 2", len(generator.requests))
	}
	lastReq := generator.requests[1]
	if len(lastReq.Messages) == 0 || lastReq.Messages[len(lastReq.Messages)-1].Role != "tool" {
		t.Fatalf("last request messages = %+v, want trailing tool message", lastReq.Messages)
	}
	if !strings.Contains(lastReq.Messages[len(lastReq.Messages)-1].Content, "Tenet reads this file.") {
		t.Fatalf("tool message content = %q", lastReq.Messages[len(lastReq.Messages)-1].Content)
	}

	events, err := store.Read("task:react-tool", 1)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	seen := map[string]bool{}
	for _, evt := range events {
		seen[evt.EventType] = true
	}
	for _, want := range []string{"TaskStarted", "GenerateThought", "ToolExecuted", "TaskCompleted"} {
		if !seen[want] {
			t.Fatalf("missing event %s in %+v", want, events)
		}
	}
}
