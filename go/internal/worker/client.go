package worker

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type GenerateThoughtRequest struct {
	SessionID    string
	TaskID       string
	Model        string
	Temperature  float64
	SystemPrompt string
	Messages     []Message
	Tools        []ToolDefinition
}

type GenerateThoughtResponse struct {
	Thought         string
	ToolCalls       []ToolCall
	IsFinal         bool
	FinishReason    string
	Usage           TokenUsage
	DiscoveredTools []ToolDefinition
}

type Message struct {
	Role       string
	Content    string
	ToolCallID string
	ToolCalls  []ToolCall
}

type ToolCall struct {
	CallID    string
	ToolName  string
	Arguments string
}

type ToolDefinition struct {
	Name             string
	Description      string
	ParametersSchema string
}

type TokenUsage struct {
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
	CostUSD          float64
}

type ExecuteToolRequest struct {
	SessionID    string
	FencingToken int64
	Workspace    string
	ToolName     string
	Arguments    string
}

type ExecuteToolResponse struct {
	Stdout     string
	Stderr     string
	ExitCode   int
	IsError    bool
	DurationMS int64
}

type HealthCheckResponse struct {
	Status        string
	WorkerCount   int
	UptimeSeconds int64
}

type Client interface {
	GenerateThought(context.Context, GenerateThoughtRequest) (GenerateThoughtResponse, error)
	ExecuteTool(context.Context, ExecuteToolRequest) (ExecuteToolResponse, error)
	HealthCheck(context.Context) (HealthCheckResponse, error)
}

type StaticClient struct {
	Response     GenerateThoughtResponse
	ToolResponse ExecuteToolResponse
	Err          error
}

func (c StaticClient) GenerateThought(context.Context, GenerateThoughtRequest) (GenerateThoughtResponse, error) {
	if c.Err != nil {
		return GenerateThoughtResponse{}, c.Err
	}
	return c.Response, nil
}

func (c StaticClient) ExecuteTool(context.Context, ExecuteToolRequest) (ExecuteToolResponse, error) {
	if c.Err != nil {
		return ExecuteToolResponse{}, c.Err
	}
	return c.ToolResponse, nil
}

func (c StaticClient) HealthCheck(context.Context) (HealthCheckResponse, error) {
	if c.Err != nil {
		return HealthCheckResponse{}, c.Err
	}
	return HealthCheckResponse{Status: "SERVING", WorkerCount: 1}, nil
}

type EchoClient struct {
	StartedAt time.Time
}

func NewEchoClient() EchoClient {
	return EchoClient{StartedAt: time.Now()}
}

func (c EchoClient) GenerateThought(_ context.Context, req GenerateThoughtRequest) (GenerateThoughtResponse, error) {
	query := ""
	if len(req.Messages) > 0 {
		query = req.Messages[len(req.Messages)-1].Content
	}
	if query == "" {
		query = req.SystemPrompt
	}
	thought := fmt.Sprintf("Echo response for task %s: %s", req.TaskID, strings.TrimSpace(query))
	return GenerateThoughtResponse{
		Thought:      thought,
		IsFinal:      true,
		FinishReason: "stop",
		Usage: TokenUsage{
			PromptTokens:     len(req.SystemPrompt) / 4,
			CompletionTokens: len(thought) / 4,
			TotalTokens:      (len(req.SystemPrompt) + len(thought)) / 4,
		},
	}, nil
}

func (c EchoClient) ExecuteTool(_ context.Context, req ExecuteToolRequest) (ExecuteToolResponse, error) {
	return ExecuteToolResponse{
		Stdout:     fmt.Sprintf("tool %s executed with %s", req.ToolName, req.Arguments),
		ExitCode:   0,
		DurationMS: 0,
	}, nil
}

func (c EchoClient) HealthCheck(context.Context) (HealthCheckResponse, error) {
	return HealthCheckResponse{
		Status:        "SERVING",
		WorkerCount:   1,
		UptimeSeconds: int64(time.Since(c.StartedAt).Seconds()),
	}, nil
}
