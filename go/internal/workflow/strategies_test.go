package workflow

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/guard"
	"github.com/tenet/orchestrator/internal/storage"
	"github.com/tenet/orchestrator/internal/worker"
)

type scriptedGenerator struct {
	responses    []worker.GenerateThoughtResponse
	requests     []worker.GenerateThoughtRequest
	toolRequests []worker.ExecuteToolRequest
	toolResponse worker.ExecuteToolResponse
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

func (c *scriptedGenerator) ExecuteTool(_ context.Context, req worker.ExecuteToolRequest) (worker.ExecuteToolResponse, error) {
	c.toolRequests = append(c.toolRequests, req)
	if c.toolResponse.Stdout != "" || c.toolResponse.Stderr != "" || c.toolResponse.ExitCode != 0 || c.toolResponse.IsError {
		return c.toolResponse, nil
	}
	return worker.ExecuteToolResponse{Stdout: "ok", ExitCode: 0}, nil
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

func TestReactWorkflowValidatesAndPassesFencingTokenToTool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()

	client := &scriptedGenerator{
		responses: []worker.GenerateThoughtResponse{
			{
				Thought:      "Need a tool.",
				FinishReason: "tool_calls",
				ToolCalls: []worker.ToolCall{{
					CallID:    "call_shell",
					ToolName:  "shell",
					Arguments: `{"command":"pwd"}`,
				}},
			},
			{
				Thought:      "done",
				IsFinal:      true,
				FinishReason: "stop",
			},
		},
		toolResponse: worker.ExecuteToolResponse{Stdout: "workspace\n", ExitCode: 0},
	}
	lockManager := &recordingLockManager{lease: guard.FencingLease{
		SessionID:       "task:fencing",
		AgentID:         "default",
		Token:           42,
		Backend:         "redis",
		FencingRequired: true,
	}}
	cfg := config.Default()
	cfg.Agent.DefaultMaxSteps = 4
	cfg.Agent.ConvergenceNoToolCalls = 1

	_, err := Execute(ctx, store, NewRegistry(), &TaskHandle{
		StreamID:     "task:fencing",
		WorkflowType: "react",
		SessionID:    "task:fencing",
		Query:        "Use a tool",
		Workspace:    t.TempDir(),
		SystemPrompt: "Use tools.",
		Tools:        worker.BuiltinToolDefinitions(),
		Config:       cfg,
		Client:       client,
		LockManager:  lockManager,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if lockManager.validateCount != 1 {
		t.Fatalf("validate count = %d, want 1", lockManager.validateCount)
	}
	if len(client.toolRequests) != 1 {
		t.Fatalf("tool requests = %d, want 1", len(client.toolRequests))
	}
	if client.toolRequests[0].FencingToken != 42 {
		t.Fatalf("fencing token = %d, want 42", client.toolRequests[0].FencingToken)
	}
}

func TestDAGWorkflowDecomposesRunsSubtasksAndSummarizes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()
	client := &scriptedGenerator{responses: []worker.GenerateThoughtResponse{
		{Thought: `[{"id":"a","agent":"researcher","task":"find facts","depends_on":[]},{"id":"b","agent":"analyst","task":"summarize facts","depends_on":["a"]}]`, IsFinal: true},
		{Thought: "facts", IsFinal: true},
		{Thought: "summary", IsFinal: true},
		{Thought: "final dag answer", IsFinal: true},
	}}
	result, err := Execute(ctx, store, NewRegistry(), &TaskHandle{
		StreamID:     "task:dag",
		WorkflowType: "dag",
		SessionID:    "task:dag",
		Query:        "analyze",
		Workspace:    t.TempDir(),
		Config:       config.Default(),
		Client:       client,
	})
	if err != nil {
		t.Fatalf("execute dag: %v", err)
	}
	if result.Result != "final dag answer" {
		t.Fatalf("result = %v", result.Result)
	}
	assertEvents(t, store, "task:dag", "TaskDecomposed", "SubTaskDispatched", "SubTaskCompleted", "TaskCompleted")
}

func TestScientificWorkflowRunsReasoningPatterns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()
	client := &scriptedGenerator{responses: []worker.GenerateThoughtResponse{
		{Thought: "FINAL: hypothesis", IsFinal: true},
		{Thought: "pro", IsFinal: true},
		{Thought: "con", IsFinal: true},
		{Thought: "judge", IsFinal: true},
		{Thought: "candidates", IsFinal: true},
		{Thought: "evaluation", IsFinal: true},
		{Thought: "proposal", IsFinal: true},
		{Thought: "PASS", IsFinal: true},
	}}
	result, err := Execute(ctx, store, NewRegistry(), &TaskHandle{
		StreamID:     "task:scientific",
		WorkflowType: "scientific",
		SessionID:    "task:scientific",
		Query:        "test hypothesis",
		Workspace:    t.TempDir(),
		Config:       config.Default(),
		Client:       client,
	})
	if err != nil {
		t.Fatalf("execute scientific: %v", err)
	}
	if result.Result != "proposal" {
		t.Fatalf("result = %v", result.Result)
	}
	assertEvents(t, store, "task:scientific", "ReasoningPatternStarted", "ReasoningPatternCompleted", "TaskCompleted")
	if len(client.requests) < 8 {
		t.Fatalf("generate calls = %d, want at least 8", len(client.requests))
	}
}

func TestCodingWorkflowRunsPhasesAndChecks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()
	client := &scriptedGenerator{responses: []worker.GenerateThoughtResponse{
		{Thought: "design", IsFinal: true},
		{Thought: "coding", IsFinal: true},
		{Thought: "review", IsFinal: true},
	}}
	cfg := config.Default()
	cfg.Coding.StaticCheckCmd = "printf static-ok"
	cfg.Coding.TestCmd = "printf test-ok"
	result, err := Execute(ctx, store, NewRegistry(), &TaskHandle{
		StreamID:     "task:coding",
		WorkflowType: "coding",
		SessionID:    "task:coding",
		Query:        "implement",
		Workspace:    t.TempDir(),
		Config:       cfg,
		Client:       client,
	})
	if err != nil {
		t.Fatalf("execute coding: %v", err)
	}
	if !strings.Contains(result.Result.(string), "Design:") {
		t.Fatalf("result = %v", result.Result)
	}
	assertEvents(t, store, "task:coding", "CodingPhaseStarted", "CodingPhaseCompleted", "CodingSnapshotCreated", "TaskCompleted")
}

func TestInteractiveWorkflowRecordsHumanWaitWhenNeeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := testStore(t)
	defer store.Close()
	client := &scriptedGenerator{responses: []worker.GenerateThoughtResponse{
		{Thought: "draft NEEDS_HUMAN_REVIEW", IsFinal: false},
		{Thought: "final interactive", IsFinal: true},
	}}
	cfg := config.Default()
	cfg.Interactive.HumanTimeoutSeconds = 1
	result, err := Execute(ctx, store, NewRegistry(), &TaskHandle{
		StreamID:     "task:interactive",
		WorkflowType: "interactive",
		SessionID:    "task:interactive",
		Query:        "draft",
		Workspace:    t.TempDir(),
		Config:       cfg,
		Client:       client,
	})
	if err != nil {
		t.Fatalf("execute interactive: %v", err)
	}
	if result.Result != "final interactive" {
		t.Fatalf("result = %v", result.Result)
	}
	assertEvents(t, store, "task:interactive", "InteractiveRoundStarted", "WaitingForHumanInput", "TaskCompleted")
}

func assertEvents(t *testing.T, store interface {
	Read(string, int64) ([]storage.Event, error)
}, streamID string, wants ...string) {
	t.Helper()
	events, err := store.Read(streamID, 1)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	seen := map[string]bool{}
	for _, evt := range events {
		seen[evt.EventType] = true
	}
	for _, want := range wants {
		if !seen[want] {
			t.Fatalf("missing event %s in %+v", want, events)
		}
	}
}

type recordingLockManager struct {
	lease         guard.FencingLease
	validateCount int
	releaseCount  int
}

func (m *recordingLockManager) Acquire(context.Context, string, string) (guard.FencingLease, error) {
	return m.lease, nil
}

func (m *recordingLockManager) Renew(context.Context, guard.FencingLease) (guard.FencingLease, error) {
	return m.lease, nil
}

func (m *recordingLockManager) Validate(context.Context, guard.FencingLease) error {
	m.validateCount++
	return nil
}

func (m *recordingLockManager) Release(context.Context, guard.FencingLease) error {
	m.releaseCount++
	return nil
}

func (m *recordingLockManager) Backend() string {
	return "redis"
}
