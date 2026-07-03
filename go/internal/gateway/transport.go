package gateway

import "github.com/tenet/orchestrator/internal/worker"

type generateThoughtRequest struct {
	SessionID    string           `json:"session_id"`
	TaskID       string           `json:"task_id"`
	Model        string           `json:"model"`
	Temperature  float64          `json:"temperature"`
	SystemPrompt string           `json:"system_prompt"`
	Messages     []message        `json:"messages"`
	Tools        []toolDefinition `json:"tools"`
}

type generateThoughtResponse struct {
	Thought         string           `json:"thought"`
	ToolCalls       []toolCall       `json:"tool_calls"`
	IsFinal         bool             `json:"is_final"`
	FinishReason    string           `json:"finish_reason"`
	Usage           tokenUsage       `json:"usage"`
	DiscoveredTools []toolDefinition `json:"discovered_tools"`
}

type executeToolRequest struct {
	SessionID    string `json:"session_id"`
	FencingToken int64  `json:"fencing_token"`
	Workspace    string `json:"workspace"`
	ToolName     string `json:"tool_name"`
	Arguments    string `json:"arguments"`
}

type executeToolResponse struct {
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	ExitCode   int    `json:"exit_code"`
	IsError    bool   `json:"is_error"`
	DurationMS int64  `json:"duration_ms"`
}

type healthCheckResponse struct {
	Status        string `json:"status"`
	WorkerCount   int    `json:"worker_count"`
	UptimeSeconds int64  `json:"uptime_seconds"`
}

type message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	ToolCallID string     `json:"tool_call_id"`
	ToolCalls  []toolCall `json:"tool_calls"`
}

type toolCall struct {
	CallID    string `json:"call_id"`
	ToolName  string `json:"tool_name"`
	Arguments string `json:"arguments"`
}

type toolDefinition struct {
	Name             string `json:"name"`
	Description      string `json:"description"`
	ParametersSchema string `json:"parameters_schema"`
}

type tokenUsage struct {
	PromptTokens     int     `json:"prompt_tokens"`
	CompletionTokens int     `json:"completion_tokens"`
	TotalTokens      int     `json:"total_tokens"`
	CostUSD          float64 `json:"cost_usd"`
}

func toGenerateThoughtRequest(req worker.GenerateThoughtRequest) generateThoughtRequest {
	return generateThoughtRequest{
		SessionID:    req.SessionID,
		TaskID:       req.TaskID,
		Model:        req.Model,
		Temperature:  req.Temperature,
		SystemPrompt: req.SystemPrompt,
		Messages:     toMessages(req.Messages),
		Tools:        toToolDefinitions(req.Tools),
	}
}

func fromGenerateThoughtResponse(resp generateThoughtResponse) worker.GenerateThoughtResponse {
	return worker.GenerateThoughtResponse{
		Thought:         resp.Thought,
		ToolCalls:       fromToolCalls(resp.ToolCalls),
		IsFinal:         resp.IsFinal,
		FinishReason:    resp.FinishReason,
		Usage:           fromTokenUsage(resp.Usage),
		DiscoveredTools: fromToolDefinitions(resp.DiscoveredTools),
	}
}

func toExecuteToolRequest(req worker.ExecuteToolRequest) executeToolRequest {
	return executeToolRequest{
		SessionID:    req.SessionID,
		FencingToken: req.FencingToken,
		Workspace:    req.Workspace,
		ToolName:     req.ToolName,
		Arguments:    req.Arguments,
	}
}

func fromExecuteToolResponse(resp executeToolResponse) worker.ExecuteToolResponse {
	return worker.ExecuteToolResponse{
		Stdout:     resp.Stdout,
		Stderr:     resp.Stderr,
		ExitCode:   resp.ExitCode,
		IsError:    resp.IsError,
		DurationMS: resp.DurationMS,
	}
}

func fromHealthCheckResponse(resp healthCheckResponse) worker.HealthCheckResponse {
	return worker.HealthCheckResponse{
		Status:        resp.Status,
		WorkerCount:   resp.WorkerCount,
		UptimeSeconds: resp.UptimeSeconds,
	}
}

func toMessages(in []worker.Message) []message {
	out := make([]message, 0, len(in))
	for _, item := range in {
		out = append(out, message{
			Role:       item.Role,
			Content:    item.Content,
			ToolCallID: item.ToolCallID,
			ToolCalls:  toToolCalls(item.ToolCalls),
		})
	}
	return out
}

func toToolCalls(in []worker.ToolCall) []toolCall {
	out := make([]toolCall, 0, len(in))
	for _, item := range in {
		out = append(out, toolCall{
			CallID:    item.CallID,
			ToolName:  item.ToolName,
			Arguments: item.Arguments,
		})
	}
	return out
}

func fromToolCalls(in []toolCall) []worker.ToolCall {
	out := make([]worker.ToolCall, 0, len(in))
	for _, item := range in {
		out = append(out, worker.ToolCall{
			CallID:    item.CallID,
			ToolName:  item.ToolName,
			Arguments: item.Arguments,
		})
	}
	return out
}

func toToolDefinitions(in []worker.ToolDefinition) []toolDefinition {
	out := make([]toolDefinition, 0, len(in))
	for _, item := range in {
		out = append(out, toolDefinition{
			Name:             item.Name,
			Description:      item.Description,
			ParametersSchema: item.ParametersSchema,
		})
	}
	return out
}

func fromToolDefinitions(in []toolDefinition) []worker.ToolDefinition {
	out := make([]worker.ToolDefinition, 0, len(in))
	for _, item := range in {
		out = append(out, worker.ToolDefinition{
			Name:             item.Name,
			Description:      item.Description,
			ParametersSchema: item.ParametersSchema,
		})
	}
	return out
}

func fromTokenUsage(in tokenUsage) worker.TokenUsage {
	return worker.TokenUsage{
		PromptTokens:     in.PromptTokens,
		CompletionTokens: in.CompletionTokens,
		TotalTokens:      in.TotalTokens,
		CostUSD:          in.CostUSD,
	}
}
