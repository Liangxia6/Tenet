package gateway

import (
	tenetv1 "github.com/tenet/orchestrator/internal/gateway/gen/tenet/v1"
	"github.com/tenet/orchestrator/internal/worker"
)

func toProtoGenerateThoughtRequest(req worker.GenerateThoughtRequest) *tenetv1.GenerateThoughtRequest {
	return &tenetv1.GenerateThoughtRequest{
		SessionId:    req.SessionID,
		TaskId:       req.TaskID,
		Model:        req.Model,
		Temperature:  req.Temperature,
		SystemPrompt: req.SystemPrompt,
		Messages:     toProtoMessages(req.Messages),
		Tools:        toProtoToolDefinitions(req.Tools),
	}
}

func fromProtoGenerateThoughtResponse(resp *tenetv1.GenerateThoughtResponse) worker.GenerateThoughtResponse {
	return worker.GenerateThoughtResponse{
		Thought:         resp.Thought,
		ToolCalls:       fromProtoToolCalls(resp.ToolCalls),
		IsFinal:         resp.IsFinal,
		FinishReason:    resp.FinishReason,
		Usage:           fromProtoTokenUsage(resp.Usage),
		DiscoveredTools: fromProtoToolDefinitions(resp.DiscoveredTools),
	}
}

func toProtoExecuteToolRequest(req worker.ExecuteToolRequest) *tenetv1.ExecuteToolRequest {
	return &tenetv1.ExecuteToolRequest{
		SessionId:    req.SessionID,
		FencingToken: req.FencingToken,
		ToolName:     req.ToolName,
		Arguments:    req.Arguments,
		Workspace:    req.Workspace,
	}
}

func fromProtoExecuteToolResponse(resp *tenetv1.ExecuteToolResponse) worker.ExecuteToolResponse {
	return worker.ExecuteToolResponse{
		Stdout:     resp.Stdout,
		Stderr:     resp.Stderr,
		ExitCode:   int(resp.ExitCode),
		IsError:    resp.IsError,
		DurationMS: resp.DurationMs,
	}
}

func toProtoMessages(in []worker.Message) []*tenetv1.Message {
	out := make([]*tenetv1.Message, 0, len(in))
	for _, item := range in {
		out = append(out, &tenetv1.Message{
			Role:       item.Role,
			Content:    item.Content,
			ToolCallId: item.ToolCallID,
			ToolCalls:  toProtoToolCalls(item.ToolCalls),
		})
	}
	return out
}

func toProtoToolCalls(in []worker.ToolCall) []*tenetv1.ToolCall {
	out := make([]*tenetv1.ToolCall, 0, len(in))
	for _, item := range in {
		out = append(out, &tenetv1.ToolCall{
			CallId:    item.CallID,
			ToolName:  item.ToolName,
			Arguments: item.Arguments,
		})
	}
	return out
}

func fromProtoToolCalls(in []*tenetv1.ToolCall) []worker.ToolCall {
	out := make([]worker.ToolCall, 0, len(in))
	for _, item := range in {
		out = append(out, worker.ToolCall{
			CallID:    item.CallId,
			ToolName:  item.ToolName,
			Arguments: item.Arguments,
		})
	}
	return out
}

func toProtoToolDefinitions(in []worker.ToolDefinition) []*tenetv1.ToolDefinition {
	out := make([]*tenetv1.ToolDefinition, 0, len(in))
	for _, item := range in {
		out = append(out, &tenetv1.ToolDefinition{
			Name:             item.Name,
			Description:      item.Description,
			ParametersSchema: item.ParametersSchema,
		})
	}
	return out
}

func fromProtoToolDefinitions(in []*tenetv1.ToolDefinition) []worker.ToolDefinition {
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

func fromProtoTokenUsage(in *tenetv1.TokenUsage) worker.TokenUsage {
	if in == nil {
		return worker.TokenUsage{}
	}
	return worker.TokenUsage{
		PromptTokens:     int(in.PromptTokens),
		CompletionTokens: int(in.CompletionTokens),
		TotalTokens:      int(in.TotalTokens),
		CostUSD:          in.CostUsd,
	}
}
