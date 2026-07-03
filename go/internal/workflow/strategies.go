package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

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
			if err := task.CheckToolRateLimit(ctx, call.ToolName); err != nil {
				return nil, err
			}
			if err := task.ValidateFencingLease(ctx); err != nil {
				return nil, err
			}
			decision, err := wfctx.Decide(ctx, "ToolExecuted", func(ctx context.Context) (any, error) {
				return task.Client.ExecuteTool(ctx, worker.ExecuteToolRequest{
					SessionID:    task.SessionID,
					FencingToken: task.CurrentFencingToken(),
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
	if err := recordTaskStarted(ctx, wfctx, task); err != nil {
		return nil, err
	}
	decomposePrompt := "Decompose this task into a small DAG. Return JSON array items with id, agent, task, depends_on."
	response, err := generateThought(ctx, wfctx, task, decomposePrompt, []worker.Message{{Role: "user", Content: task.Query}}, nil)
	if err != nil {
		return nil, err
	}
	subtasks := parseDAGSubtasks(response.Thought, task.Query)
	if len(subtasks) > 50 {
		subtasks = subtasks[:50]
	}
	if err := wfctx.Record(ctx, "TaskDecomposed", map[string]any{"subtasks": subtasks}); err != nil {
		return nil, err
	}
	results := make(map[string]string, len(subtasks))
	for _, subtask := range topoSortSubtasks(subtasks) {
		dependencyContext := collectDependencyContext(subtask, results)
		if err := wfctx.Record(ctx, "SubTaskDispatched", map[string]any{
			"subtask_id": subtask.ID,
			"agent_role": subtask.Agent,
			"depends_on": subtask.DependsOn,
		}); err != nil {
			return nil, err
		}
		subtaskPrompt := strings.TrimSpace(dependencyContext + "\n\n" + subtask.Task)
		subtaskResp, err := generateThought(ctx, wfctx, task, "You are executing one DAG subtask. Return concise findings.", []worker.Message{{Role: "user", Content: subtaskPrompt}}, nil)
		if err != nil {
			return nil, err
		}
		results[subtask.ID] = subtaskResp.Thought
		if err := wfctx.Record(ctx, "SubTaskCompleted", map[string]any{
			"subtask_id":   subtask.ID,
			"result":       subtaskResp.Thought,
			"key_findings": []string{subtaskResp.Thought},
		}); err != nil {
			return nil, err
		}
	}
	summaryMessages := []worker.Message{{Role: "user", Content: formatSubtaskResults(task.Query, results)}}
	summary, err := generateThought(ctx, wfctx, task, "Synthesize DAG subtask findings into a final answer.", summaryMessages, nil)
	if err != nil {
		return nil, err
	}
	if err := wfctx.Record(ctx, "TaskCompleted", map[string]any{
		"final_answer": summary.Thought,
		"total_steps":  len(subtasks) + 2,
	}); err != nil {
		return nil, err
	}
	return summary.Thought, nil
}

func InteractiveWorkflow(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
	if err := recordTaskStarted(ctx, wfctx, task); err != nil {
		return nil, err
	}
	maxRounds := 1
	if task.Config.Interactive.HumanTimeoutSeconds > 0 {
		maxRounds = 2
	}
	messages := []worker.Message{{Role: "user", Content: task.Query}}
	var last worker.GenerateThoughtResponse
	for round := 1; round <= maxRounds; round++ {
		if err := wfctx.Record(ctx, "InteractiveRoundStarted", map[string]any{"round": round, "workspace": task.Workspace}); err != nil {
			return nil, err
		}
		lastResp, err := generateThought(ctx, wfctx, task, "Produce an interactive draft. Mention NEEDS_HUMAN_REVIEW if human input is required.", messages, nil)
		if err != nil {
			return nil, err
		}
		last = lastResp
		if lastResp.IsFinal && !strings.Contains(strings.ToUpper(lastResp.Thought), "NEEDS_HUMAN_REVIEW") {
			break
		}
		if err := wfctx.Record(ctx, "WaitingForHumanInput", map[string]any{
			"round":           round,
			"timeout_seconds": task.Config.Interactive.HumanTimeoutSeconds,
		}); err != nil {
			return nil, err
		}
		if task.Config.Interactive.HumanTimeoutSeconds > 0 {
			if err := wfctx.Sleep(ctx, fmt.Sprintf("interactive:%d", round), time.Duration(task.Config.Interactive.HumanTimeoutSeconds)*time.Second); err != nil {
				return nil, err
			}
		}
		messages = append(messages,
			worker.Message{Role: "assistant", Content: lastResp.Thought},
			worker.Message{Role: "user", Content: "No external human input was provided before timeout. Continue with the best available revision."},
		)
	}
	if err := wfctx.Record(ctx, "TaskCompleted", map[string]any{"final_answer": last.Thought, "total_steps": maxRounds}); err != nil {
		return nil, err
	}
	return last.Thought, nil
}

func ScientificWorkflow(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
	if err := recordTaskStarted(ctx, wfctx, task); err != nil {
		return nil, err
	}
	hypothesis, err := chainOfThought(ctx, wfctx, task, task.Query, 3)
	if err != nil {
		return nil, err
	}
	consensus, err := debate(ctx, wfctx, task, hypothesis)
	if err != nil {
		return nil, err
	}
	riskAnalysis, err := treeOfThoughts(ctx, wfctx, task, consensus)
	if err != nil {
		return nil, err
	}
	final, err := reflection(ctx, wfctx, task, riskAnalysis)
	if err != nil {
		return nil, err
	}
	if err := wfctx.Record(ctx, "TaskCompleted", map[string]any{"final_answer": final, "total_steps": 4}); err != nil {
		return nil, err
	}
	return final, nil
}

func CodingWorkflow(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
	if err := recordTaskStarted(ctx, wfctx, task); err != nil {
		return nil, err
	}
	design, err := codingGeneratePhase(ctx, wfctx, task, "design", "Analyze the codebase request and produce an implementation plan.")
	if err != nil {
		return nil, err
	}
	if err := wfctx.Record(ctx, "CodingPhaseStarted", map[string]any{"phase": "snapshot"}); err != nil {
		return nil, err
	}
	if err := wfctx.Record(ctx, "CodingSnapshotCreated", map[string]any{"workspace": task.Workspace, "strategy": "logical"}); err != nil {
		return nil, err
	}
	if err := wfctx.Record(ctx, "CodingPhaseCompleted", map[string]any{"phase": "snapshot"}); err != nil {
		return nil, err
	}
	coding, err := codingGeneratePhase(ctx, wfctx, task, "coding", "Implement the requested change using available tools if needed.")
	if err != nil {
		return nil, err
	}
	if err := runCodingCheck(ctx, wfctx, task, "static_check", task.Config.Coding.StaticCheckCmd); err != nil {
		return nil, err
	}
	if err := runCodingCheck(ctx, wfctx, task, "unit_test", task.Config.Coding.TestCmd); err != nil {
		return nil, err
	}
	review, err := codingGeneratePhase(ctx, wfctx, task, "review", "Review the implementation plan and coding result. Return PASS or concrete risks.")
	if err != nil {
		return nil, err
	}
	final := strings.TrimSpace(fmt.Sprintf("Design:\n%s\n\nCoding:\n%s\n\nReview:\n%s", design, coding, review))
	if err := wfctx.Record(ctx, "CodingPhaseStarted", map[string]any{"phase": "finalize"}); err != nil {
		return nil, err
	}
	if err := wfctx.Record(ctx, "CodingPhaseCompleted", map[string]any{"phase": "finalize"}); err != nil {
		return nil, err
	}
	if err := wfctx.Record(ctx, "TaskCompleted", map[string]any{"final_answer": final, "total_steps": 7}); err != nil {
		return nil, err
	}
	return final, nil
}

func recordTaskStarted(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) error {
	return wfctx.Record(ctx, "TaskStarted", map[string]any{
		"session_id":    task.SessionID,
		"workspace":     task.Workspace,
		"workflow_type": task.WorkflowType,
		"query":         task.Query,
	})
}

func generateThought(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, systemPrompt string, messages []worker.Message, tools []worker.ToolDefinition) (worker.GenerateThoughtResponse, error) {
	if systemPrompt == "" {
		systemPrompt = task.SystemPrompt
	}
	if len(tools) == 0 {
		tools = task.Tools
	}
	decision, err := wfctx.Decide(ctx, "GenerateThought", func(ctx context.Context) (any, error) {
		return task.Client.GenerateThought(ctx, worker.GenerateThoughtRequest{
			SessionID:    task.SessionID,
			TaskID:       task.StreamID,
			Model:        task.Model,
			Temperature:  task.Config.Agent.DefaultTemperature,
			SystemPrompt: systemPrompt,
			Messages:     messages,
			Tools:        tools,
		})
	})
	if err != nil {
		return worker.GenerateThoughtResponse{}, err
	}
	response, err := coerce[worker.GenerateThoughtResponse](decision)
	if err != nil {
		return worker.GenerateThoughtResponse{}, err
	}
	recordUsage(ctx, wfctx, task, response.Usage)
	if len(response.DiscoveredTools) > 0 {
		_ = wfctx.Record(ctx, "ToolsDiscovered", map[string]any{"tools": response.DiscoveredTools})
		task.Tools = mergeTools(task.Tools, response.DiscoveredTools)
	}
	return response, nil
}

type dagSubtask struct {
	ID        string   `json:"id"`
	Agent     string   `json:"agent"`
	Task      string   `json:"task"`
	DependsOn []string `json:"depends_on"`
}

func parseDAGSubtasks(raw, fallbackQuery string) []dagSubtask {
	var subtasks []dagSubtask
	text := strings.TrimSpace(raw)
	if start := strings.Index(text, "["); start >= 0 {
		if end := strings.LastIndex(text, "]"); end >= start {
			text = text[start : end+1]
		}
	}
	if err := json.Unmarshal([]byte(text), &subtasks); err == nil {
		out := make([]dagSubtask, 0, len(subtasks))
		for i, subtask := range subtasks {
			if subtask.ID == "" {
				subtask.ID = fmt.Sprintf("subtask-%d", i+1)
			}
			if subtask.Agent == "" {
				subtask.Agent = "default"
			}
			if subtask.Task != "" {
				out = append(out, subtask)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return []dagSubtask{
		{ID: "research", Agent: "researcher", Task: "Gather key facts for: " + fallbackQuery},
		{ID: "synthesize", Agent: "analyst", Task: "Synthesize a final answer for: " + fallbackQuery, DependsOn: []string{"research"}},
	}
}

func topoSortSubtasks(subtasks []dagSubtask) []dagSubtask {
	remaining := append([]dagSubtask(nil), subtasks...)
	done := map[string]bool{}
	ordered := make([]dagSubtask, 0, len(subtasks))
	for len(remaining) > 0 {
		progress := false
		next := remaining[:0]
		for _, subtask := range remaining {
			ready := true
			for _, dep := range subtask.DependsOn {
				if !done[dep] {
					ready = false
					break
				}
			}
			if ready {
				ordered = append(ordered, subtask)
				done[subtask.ID] = true
				progress = true
			} else {
				next = append(next, subtask)
			}
		}
		if !progress {
			ordered = append(ordered, next...)
			break
		}
		remaining = next
	}
	return ordered
}

func collectDependencyContext(subtask dagSubtask, results map[string]string) string {
	if len(subtask.DependsOn) == 0 {
		return ""
	}
	var parts []string
	for _, dep := range subtask.DependsOn {
		if result := results[dep]; result != "" {
			parts = append(parts, fmt.Sprintf("[%s]\n%s", dep, result))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "Prior findings:\n" + strings.Join(parts, "\n\n")
}

func formatSubtaskResults(query string, results map[string]string) string {
	var parts []string
	for id, result := range results {
		parts = append(parts, fmt.Sprintf("%s: %s", id, result))
	}
	return fmt.Sprintf("Original task: %s\nSubtask results:\n%s", query, strings.Join(parts, "\n"))
}

func chainOfThought(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, prompt string, maxSteps int) (string, error) {
	if err := wfctx.Record(ctx, "ReasoningPatternStarted", map[string]any{"pattern": "chain_of_thought"}); err != nil {
		return "", err
	}
	messages := []worker.Message{{Role: "user", Content: prompt}}
	var steps []string
	for step := 1; step <= maxSteps; step++ {
		resp, err := generateThought(ctx, wfctx, task, "Think step by step. If finished, include FINAL: <answer>.", append(messages, worker.Message{Role: "user", Content: fmt.Sprintf("Step %d:", step)}), nil)
		if err != nil {
			return "", err
		}
		steps = append(steps, resp.Thought)
		messages = append(messages, worker.Message{Role: "assistant", Content: resp.Thought})
		if resp.IsFinal || strings.Contains(strings.ToUpper(resp.Thought), "FINAL:") {
			break
		}
	}
	out := strings.Join(steps, "\n")
	_ = wfctx.Record(ctx, "ReasoningPatternCompleted", map[string]any{"pattern": "chain_of_thought", "result": out})
	return out, nil
}

func debate(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, topic string) (string, error) {
	if err := wfctx.Record(ctx, "ReasoningPatternStarted", map[string]any{"pattern": "debate"}); err != nil {
		return "", err
	}
	shared := "Topic:\n" + topic
	for _, role := range []string{"pro", "con", "judge"} {
		resp, err := generateThought(ctx, wfctx, task, "You are the "+role+" role in a debate. Contribute to a consensus.", []worker.Message{{Role: "user", Content: shared}}, nil)
		if err != nil {
			return "", err
		}
		shared += "\n\n" + role + ":\n" + resp.Thought
	}
	_ = wfctx.Record(ctx, "ReasoningPatternCompleted", map[string]any{"pattern": "debate", "result": shared})
	return shared, nil
}

func treeOfThoughts(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, root string) (string, error) {
	if err := wfctx.Record(ctx, "ReasoningPatternStarted", map[string]any{"pattern": "tree_of_thoughts"}); err != nil {
		return "", err
	}
	candidates, err := generateThought(ctx, wfctx, task, "Generate three candidate implications for this reasoning path.", []worker.Message{{Role: "user", Content: root}}, nil)
	if err != nil {
		return "", err
	}
	evaluation, err := generateThought(ctx, wfctx, task, "Evaluate the candidate implications and keep the strongest path.", []worker.Message{{Role: "user", Content: candidates.Thought}}, nil)
	if err != nil {
		return "", err
	}
	result := "Candidates:\n" + candidates.Thought + "\n\nEvaluation:\n" + evaluation.Thought
	_ = wfctx.Record(ctx, "ReasoningPatternCompleted", map[string]any{"pattern": "tree_of_thoughts", "result": result})
	return result, nil
}

func reflection(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, proposal string) (string, error) {
	if err := wfctx.Record(ctx, "ReasoningPatternStarted", map[string]any{"pattern": "reflection"}); err != nil {
		return "", err
	}
	output, err := generateThought(ctx, wfctx, task, "Improve and finalize this proposal.", []worker.Message{{Role: "user", Content: proposal}}, nil)
	if err != nil {
		return "", err
	}
	critique, err := generateThought(ctx, wfctx, task, "Strictly review the output. Reply PASS or list issues.", []worker.Message{{Role: "user", Content: output.Thought}}, nil)
	if err != nil {
		return "", err
	}
	result := output.Thought
	if !strings.Contains(strings.ToUpper(critique.Thought), "PASS") && critique.Thought != "" {
		result += "\n\nReflection:\n" + critique.Thought
	}
	_ = wfctx.Record(ctx, "ReasoningPatternCompleted", map[string]any{"pattern": "reflection", "result": result})
	return result, nil
}

func codingGeneratePhase(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, phase, prompt string) (string, error) {
	if err := wfctx.Record(ctx, "CodingPhaseStarted", map[string]any{"phase": phase}); err != nil {
		return "", err
	}
	resp, err := generateThought(ctx, wfctx, task, prompt, []worker.Message{{Role: "user", Content: task.Query}}, task.Tools)
	if err != nil {
		return "", err
	}
	if err := wfctx.Record(ctx, "CodingPhaseCompleted", map[string]any{"phase": phase, "result": resp.Thought}); err != nil {
		return "", err
	}
	return resp.Thought, nil
}

func runCodingCheck(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, phase, command string) error {
	if strings.TrimSpace(command) == "" {
		return wfctx.Record(ctx, "CodingPhaseSkipped", map[string]any{"phase": phase, "reason": "command not configured"})
	}
	if err := wfctx.Record(ctx, "CodingPhaseStarted", map[string]any{"phase": phase, "command": command}); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.Dir = task.Workspace
	out, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		exitCode = 1
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
	}
	payload := map[string]any{"phase": phase, "command": command, "output": string(out), "exit_code": exitCode}
	if exitCode != 0 {
		_ = wfctx.Record(ctx, "CodingCheckFailed", payload)
		return fmt.Errorf("%s failed: %s", phase, strings.TrimSpace(string(out)))
	}
	return wfctx.Record(ctx, "CodingPhaseCompleted", payload)
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
