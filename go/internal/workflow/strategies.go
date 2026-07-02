package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/tenet/orchestrator/internal/worker"
)

func SimpleWorkflow(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
	if err := wfctx.Record(ctx, "TaskStarted", map[string]any{
		"session_id":    task.SessionID,
		"workspace":     task.Workspace,
		"workflow_type": task.WorkflowType,
		"query":         task.Query,
	}); err != nil {
		return nil, err
	}

	messages := task.Messages
	if len(messages) == 0 {
		messages = []worker.Message{{Role: "user", Content: task.Query}}
	}
	decision, err := wfctx.Decide(ctx, "GenerateThought", func(ctx context.Context) (any, error) {
		return task.Client.GenerateThought(ctx, worker.GenerateThoughtRequest{
			SessionID:    task.SessionID,
			TaskID:       task.StreamID,
			Model:        task.Model,
			Temperature:  task.Config.Agent.DefaultTemperature,
			SystemPrompt: task.SystemPrompt,
			Messages:     messages,
			Tools:        task.Tools,
		})
	})
	if err != nil {
		return nil, err
	}
	response, err := coerce[worker.GenerateThoughtResponse](decision)
	if err != nil {
		return nil, err
	}
	recordUsage(ctx, wfctx, task, response.Usage)
	if len(response.DiscoveredTools) > 0 {
		_ = wfctx.Record(ctx, "ToolsDiscovered", map[string]any{"tools": response.DiscoveredTools})
	}
	if err := wfctx.Record(ctx, "TaskCompleted", map[string]any{
		"final_answer": response.Thought,
		"total_steps":  1,
	}); err != nil {
		return nil, err
	}
	return response.Thought, nil
}

func ReactWorkflow(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
	if err := wfctx.Record(ctx, "TaskStarted", map[string]any{
		"session_id":    task.SessionID,
		"workspace":     task.Workspace,
		"workflow_type": task.WorkflowType,
		"query":         task.Query,
	}); err != nil {
		return nil, err
	}

	messages := task.Messages
	if len(messages) == 0 {
		messages = []worker.Message{{Role: "user", Content: task.Query}}
	}
	maxSteps := task.Config.Agent.DefaultMaxSteps
	if maxSteps <= 0 {
		maxSteps = 50
	}
	convergedWithoutTools := 0
	for step := 1; step <= maxSteps; step++ {
		decision, err := wfctx.Decide(ctx, "GenerateThought", func(ctx context.Context) (any, error) {
			return task.Client.GenerateThought(ctx, worker.GenerateThoughtRequest{
				SessionID:    task.SessionID,
				TaskID:       task.StreamID,
				Model:        task.Model,
				Temperature:  task.Config.Agent.DefaultTemperature,
				SystemPrompt: task.SystemPrompt,
				Messages:     messages,
				Tools:        task.Tools,
			})
		})
		if err != nil {
			return nil, err
		}
		response, err := coerce[worker.GenerateThoughtResponse](decision)
		if err != nil {
			return nil, err
		}
		recordUsage(ctx, wfctx, task, response.Usage)
		if len(response.DiscoveredTools) > 0 {
			_ = wfctx.Record(ctx, "ToolsDiscovered", map[string]any{"tools": response.DiscoveredTools})
			task.Tools = mergeTools(task.Tools, response.DiscoveredTools)
		}
		messages = append(messages, worker.Message{
			Role:      "assistant",
			Content:   response.Thought,
			ToolCalls: response.ToolCalls,
		})
		if response.IsFinal || len(response.ToolCalls) == 0 {
			convergedWithoutTools++
			if convergedWithoutTools >= task.Config.Agent.ConvergenceNoToolCalls {
				if err := wfctx.Record(ctx, "TaskCompleted", map[string]any{
					"final_answer": response.Thought,
					"total_steps":  step,
				}); err != nil {
					return nil, err
				}
				return response.Thought, nil
			}
			continue
		}

		convergedWithoutTools = 0
		for _, call := range response.ToolCalls {
			decision, err := wfctx.Decide(ctx, "ToolExecuted", func(ctx context.Context) (any, error) {
				return task.Client.ExecuteTool(ctx, worker.ExecuteToolRequest{
					SessionID:    task.SessionID,
					FencingToken: 0,
					Workspace:    task.Workspace,
					ToolName:     call.ToolName,
					Arguments:    call.Arguments,
				})
			})
			if err != nil {
				return nil, err
			}
			toolResponse, err := coerce[worker.ExecuteToolResponse](decision)
			if err != nil {
				return nil, err
			}
			content := toolResponse.Stdout
			if toolResponse.IsError || toolResponse.ExitCode != 0 {
				content = fmt.Sprintf("error: %s\n%s", toolResponse.Stderr, toolResponse.Stdout)
			}
			messages = append(messages, worker.Message{
				Role:       "tool",
				Content:    content,
				ToolCallID: call.CallID,
			})
		}
	}
	return nil, errors.New("exceeded max steps")
}

func DAGWorkflow(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
	_ = wfctx.Record(ctx, "TaskDecomposed", map[string]any{"subtasks": []any{}, "fallback": "simple"})
	task.WorkflowType = "simple"
	return SimpleWorkflow(ctx, wfctx, task)
}

func InteractiveWorkflow(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
	task.WorkflowType = "react"
	return ReactWorkflow(ctx, wfctx, task)
}

func ScientificWorkflow(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
	_ = wfctx.Record(ctx, "ReasoningPatternStarted", map[string]any{"pattern": "scientific-fallback"})
	task.WorkflowType = "react"
	return ReactWorkflow(ctx, wfctx, task)
}

func CodingWorkflow(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
	_ = wfctx.Record(ctx, "CodingPhaseStarted", map[string]any{"phase": "design"})
	task.WorkflowType = "react"
	return ReactWorkflow(ctx, wfctx, task)
}

func recordUsage(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, usage worker.TokenUsage) {
	if usage.TotalTokens == 0 && usage.PromptTokens == 0 && usage.CompletionTokens == 0 {
		return
	}
	_ = wfctx.Record(ctx, "TokenUsed", map[string]any{
		"task_id":           task.StreamID,
		"agent":             task.AgentRole,
		"model":             task.Model,
		"prompt_tokens":     usage.PromptTokens,
		"completion_tokens": usage.CompletionTokens,
		"total_tokens":      usage.TotalTokens,
		"cost_usd":          usage.CostUSD,
	})
}

func mergeTools(existing, discovered []worker.ToolDefinition) []worker.ToolDefinition {
	seen := make(map[string]bool, len(existing)+len(discovered))
	out := make([]worker.ToolDefinition, 0, len(existing)+len(discovered))
	for _, tool := range existing {
		seen[tool.Name] = true
		out = append(out, tool)
	}
	for _, tool := range discovered {
		if !seen[tool.Name] {
			out = append(out, tool)
			seen[tool.Name] = true
		}
	}
	return out
}
