package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"

	contextassembler "github.com/tenet/orchestrator/internal/context/assembler"
	"github.com/tenet/orchestrator/internal/worker"
)

func SimpleWorkflow(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
	if err := wfctx.Record(ctx, "TaskStarted", map[string]any{
		"session_id":    task.SessionID,
		"turn_id":       task.TurnID,
		"run_id":        task.RunID,
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
	response, err := generateThought(ctx, wfctx, task, task.SystemPrompt, messages, task.Tools)
	if err != nil {
		return nil, err
	}
	if err := wfctx.Record(ctx, "TaskCompleted", map[string]any{
		"session_id":   task.SessionID,
		"turn_id":      task.TurnID,
		"run_id":       task.RunID,
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
		"turn_id":       task.TurnID,
		"run_id":        task.RunID,
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
		response, err := generateThought(ctx, wfctx, task, task.SystemPrompt, messages, task.Tools)
		if err != nil {
			return nil, err
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
					"session_id":   task.SessionID,
					"turn_id":      task.TurnID,
					"run_id":       task.RunID,
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
			toolResponse, err := executeTool(ctx, wfctx, task, call)
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
	results, err := executeDAGSubtasks(ctx, wfctx, task, subtasks)
	if err != nil {
		return nil, err
	}
	summaryMessages := []worker.Message{{Role: "user", Content: formatSubtaskResults(task.Query, results)}}
	summary, err := generateThought(ctx, wfctx, task, "Synthesize DAG subtask findings into a final answer.", summaryMessages, nil)
	if err != nil {
		return nil, err
	}
	if err := wfctx.Record(ctx, "TaskCompleted", map[string]any{
		"session_id":   task.SessionID,
		"turn_id":      task.TurnID,
		"run_id":       task.RunID,
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
	if err := wfctx.Record(ctx, "TaskCompleted", map[string]any{"session_id": task.SessionID, "turn_id": task.TurnID, "run_id": task.RunID, "final_answer": last.Thought, "total_steps": maxRounds}); err != nil {
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
	if err := wfctx.Record(ctx, "TaskCompleted", map[string]any{"session_id": task.SessionID, "turn_id": task.TurnID, "run_id": task.RunID, "final_answer": final, "total_steps": 4}); err != nil {
		return nil, err
	}
	return final, nil
}

func CodingWorkflow(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) (any, error) {
	if err := recordTaskStarted(ctx, wfctx, task); err != nil {
		return nil, err
	}
	inspect, err := codingGeneratePhase(ctx, wfctx, task, "inspect", "Inspect the codebase request and identify the files, tests, and risks likely involved.")
	if err != nil {
		return nil, err
	}
	plan, err := codingGeneratePhase(ctx, wfctx, task, "plan", "Produce a focused implementation plan with validation steps.")
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
	edit, err := codingGeneratePhase(ctx, wfctx, task, "edit", "Implement the requested change using available tools if needed.")
	if err != nil {
		return nil, err
	}
	if err := runCodingCheck(ctx, wfctx, task, "static_check", task.Config.Coding.StaticCheckCmd); err != nil {
		return nil, err
	}
	testErr := runCodingCheck(ctx, wfctx, task, "test", task.Config.Coding.TestCmd)
	fixes := []string{}
	maxFixes := task.Config.Coding.AutoFixMaxRetries
	if maxFixes <= 0 {
		maxFixes = 1
	}
	for attempt := 1; testErr != nil && attempt <= maxFixes; attempt++ {
		if err := wfctx.Record(ctx, "CodingAutoFixStarted", map[string]any{"attempt": attempt, "error": testErr.Error()}); err != nil {
			return nil, err
		}
		fix, err := codingGeneratePhase(ctx, wfctx, task, "fix", fmt.Sprintf("Tests failed with this error:\n%s\nProduce a targeted fix plan or patch using available tools.", testErr.Error()))
		if err != nil {
			return nil, err
		}
		fixes = append(fixes, fix)
		if err := wfctx.Record(ctx, "CodingAutoFixCompleted", map[string]any{"attempt": attempt, "result": fix}); err != nil {
			return nil, err
		}
		testErr = runCodingCheck(ctx, wfctx, task, fmt.Sprintf("test_retry_%d", attempt), task.Config.Coding.TestCmd)
	}
	if testErr != nil {
		return nil, testErr
	}
	review, err := codingGeneratePhase(ctx, wfctx, task, "review", "Review the implementation plan and coding result. Return PASS or concrete risks.")
	if err != nil {
		return nil, err
	}
	summary, err := codingGeneratePhase(ctx, wfctx, task, "summarize", "Summarize the completed code change, tests run, and residual risks.")
	if err != nil {
		return nil, err
	}
	final := strings.TrimSpace(fmt.Sprintf("Inspect:\n%s\n\nPlan:\n%s\n\nEdit:\n%s\n\nFixes:\n%s\n\nReview:\n%s\n\nSummary:\n%s", inspect, plan, edit, strings.Join(fixes, "\n\n"), review, summary))
	if err := wfctx.Record(ctx, "CodingPhaseStarted", map[string]any{"phase": "finalize"}); err != nil {
		return nil, err
	}
	if err := wfctx.Record(ctx, "CodingPhaseCompleted", map[string]any{"phase": "finalize"}); err != nil {
		return nil, err
	}
	if err := wfctx.Record(ctx, "TaskCompleted", map[string]any{"session_id": task.SessionID, "turn_id": task.TurnID, "run_id": task.RunID, "final_answer": final, "total_steps": 7 + len(fixes)*2}); err != nil {
		return nil, err
	}
	return final, nil
}

func recordTaskStarted(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle) error {
	return wfctx.Record(ctx, "TaskStarted", map[string]any{
		"session_id":    task.SessionID,
		"turn_id":       task.TurnID,
		"run_id":        task.RunID,
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
	traceContext := wfctx.GetVersion("context-assembler", 2) >= 2
	if traceContext {
		assembly := contextassembler.Assemble(contextassembler.Options{
			SystemPrompt: systemPrompt,
			Messages:     messages,
			TokenBudget:  task.Config.Agent.DefaultTokenBudget,
			MaxMessages:  24,
		})
		messages = assembly.Messages
		if err := wfctx.Record(ctx, "ContextAssembled", map[string]any{
			"session_id":       task.SessionID,
			"turn_id":          task.TurnID,
			"run_id":           task.RunID,
			"message_count":    len(assembly.Messages),
			"included_refs":    assembly.IncludedRefs,
			"omitted_refs":     assembly.OmittedRefs,
			"omitted_count":    len(assembly.OmittedRefs),
			"estimated_tokens": assembly.EstimatedTokens,
			"input_chars":      assembly.InputChars,
			"token_budget":     task.Config.Agent.DefaultTokenBudget,
			"compacted":        assembly.Compacted,
		}); err != nil {
			return worker.GenerateThoughtResponse{}, err
		}
		if assembly.Compacted {
			if err := wfctx.Record(ctx, "ContextCompacted", map[string]any{
				"session_id":       task.SessionID,
				"turn_id":          task.TurnID,
				"run_id":           task.RunID,
				"omitted_refs":     assembly.OmittedRefs,
				"omitted_count":    len(assembly.OmittedRefs),
				"estimated_tokens": assembly.EstimatedTokens,
				"token_budget":     task.Config.Agent.DefaultTokenBudget,
			}); err != nil {
				return worker.GenerateThoughtResponse{}, err
			}
		}
		if task.Config.Agent.DefaultTokenBudget > 0 && assembly.EstimatedTokens > task.Config.Agent.DefaultTokenBudget {
			return worker.GenerateThoughtResponse{}, fmt.Errorf("context token budget exceeded: estimated=%d budget=%d", assembly.EstimatedTokens, task.Config.Agent.DefaultTokenBudget)
		}
	}
	traceLLM := wfctx.GetVersion("llm-call-trace", 2) >= 2
	callID := ""
	startedAt := time.Now()
	if traceLLM {
		callID = nextLLMCallID(task, wfctx)
		if err := wfctx.Record(ctx, "LLMCallStarted", map[string]any{
			"session_id":         task.SessionID,
			"turn_id":            task.TurnID,
			"run_id":             task.RunID,
			"call_id":            callID,
			"provider":           llmProvider(task),
			"model":              task.Model,
			"system_prompt_hash": hashText(systemPrompt),
			"message_count":      len(messages),
			"tools_count":        len(tools),
			"input_chars":        llmInputChars(systemPrompt, messages),
		}); err != nil {
			return worker.GenerateThoughtResponse{}, err
		}
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
		if traceLLM {
			_ = wfctx.Record(ctx, "LLMCallFailed", map[string]any{
				"session_id":  task.SessionID,
				"turn_id":     task.TurnID,
				"run_id":      task.RunID,
				"call_id":     callID,
				"error":       err.Error(),
				"latency_ms":  time.Since(startedAt).Milliseconds(),
				"retry_count": 0,
			})
		}
		return worker.GenerateThoughtResponse{}, err
	}
	response, err := coerce[worker.GenerateThoughtResponse](decision)
	if err != nil {
		if traceLLM {
			_ = wfctx.Record(ctx, "LLMCallFailed", map[string]any{
				"session_id":  task.SessionID,
				"turn_id":     task.TurnID,
				"run_id":      task.RunID,
				"call_id":     callID,
				"error":       err.Error(),
				"latency_ms":  time.Since(startedAt).Milliseconds(),
				"retry_count": 0,
			})
		}
		return worker.GenerateThoughtResponse{}, err
	}
	if traceLLM {
		if err := wfctx.Record(ctx, "LLMCallCompleted", map[string]any{
			"session_id":        task.SessionID,
			"turn_id":           task.TurnID,
			"run_id":            task.RunID,
			"call_id":           callID,
			"finish_reason":     response.FinishReason,
			"latency_ms":        time.Since(startedAt).Milliseconds(),
			"retry_count":       0,
			"prompt_tokens":     response.Usage.PromptTokens,
			"completion_tokens": response.Usage.CompletionTokens,
			"total_tokens":      response.Usage.TotalTokens,
			"cost_usd":          response.Usage.CostUSD,
		}); err != nil {
			return worker.GenerateThoughtResponse{}, err
		}
	}
	if err := recordUsage(ctx, wfctx, task, callID, response.Usage); err != nil {
		return worker.GenerateThoughtResponse{}, err
	}
	if len(response.DiscoveredTools) > 0 {
		_ = wfctx.Record(ctx, "ToolsDiscovered", map[string]any{"tools": response.DiscoveredTools})
		task.Tools = mergeTools(task.Tools, response.DiscoveredTools)
	}
	return response, nil
}

func executeTool(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, call worker.ToolCall) (worker.ExecuteToolResponse, error) {
	traceTool := wfctx.GetVersion("tool-runtime-trace", 2) >= 2
	callID := call.CallID
	if callID == "" {
		callID = fmt.Sprintf("tool:%s:%d", task.RunID, wfctx.HistoryPosition()+1)
	}
	startedAt := time.Now()
	if traceTool {
		if err := wfctx.Record(ctx, "ToolCallStarted", map[string]any{
			"session_id":    task.SessionID,
			"turn_id":       task.TurnID,
			"run_id":        task.RunID,
			"tool_call_id":  callID,
			"tool_name":     call.ToolName,
			"arguments":     call.Arguments,
			"workspace":     task.Workspace,
			"fencing_token": task.CurrentFencingToken(),
		}); err != nil {
			return worker.ExecuteToolResponse{}, err
		}
	}
	if task.Config != nil && !worker.ToolAllowed(task.Config.Safety.ToolAllowlist, call.ToolName) {
		err := fmt.Errorf("tool not allowed by safety.tool_allowlist: %s", call.ToolName)
		if traceTool {
			_ = wfctx.Record(ctx, "ToolCallFailed", toolFailurePayload(task, callID, call, "PERMISSION_DENIED", err.Error(), startedAt, worker.ExecuteToolResponse{}))
		}
		return worker.ExecuteToolResponse{}, err
	}
	if err := task.CheckToolRateLimit(ctx, call.ToolName); err != nil {
		if traceTool {
			_ = wfctx.Record(ctx, "ToolCallFailed", toolFailurePayload(task, callID, call, "RATE_LIMITED", err.Error(), startedAt, worker.ExecuteToolResponse{}))
		}
		return worker.ExecuteToolResponse{}, err
	}
	if err := task.ValidateFencingLease(ctx); err != nil {
		if traceTool {
			_ = wfctx.Record(ctx, "ToolCallFailed", toolFailurePayload(task, callID, call, "PERMISSION_DENIED", err.Error(), startedAt, worker.ExecuteToolResponse{}))
		}
		return worker.ExecuteToolResponse{}, err
	}
	beforeState := captureToolWorkspaceState(ctx, task, call)
	decision, err := wfctx.Decide(ctx, "ToolExecuted", func(ctx context.Context) (any, error) {
		return task.Client.ExecuteTool(ctx, worker.ExecuteToolRequest{
			SessionID:    task.SessionID,
			FencingToken: task.CurrentFencingToken(),
			Workspace:    task.Workspace,
			ToolName:     call.ToolName,
			Arguments:    call.Arguments,
		})
	})
	touchedFiles := inferTouchedFiles(ctx, task, call, beforeState)
	if err != nil {
		if traceTool {
			payload := toolFailurePayload(task, callID, call, "EXEC_FAILED", err.Error(), startedAt, worker.ExecuteToolResponse{})
			payload["touched_files"] = touchedFiles
			_ = wfctx.Record(ctx, "ToolCallFailed", payload)
		}
		return worker.ExecuteToolResponse{}, err
	}
	toolResponse, err := coerce[worker.ExecuteToolResponse](decision)
	if err != nil {
		if traceTool {
			payload := toolFailurePayload(task, callID, call, "INVALID_RESULT", err.Error(), startedAt, worker.ExecuteToolResponse{})
			payload["touched_files"] = touchedFiles
			_ = wfctx.Record(ctx, "ToolCallFailed", payload)
		}
		return worker.ExecuteToolResponse{}, err
	}
	if toolResponse.IsError || toolResponse.ExitCode != 0 {
		if traceTool {
			payload := toolFailurePayload(task, callID, call, classifyToolError(toolResponse), toolResponse.Stderr, startedAt, toolResponse)
			payload["touched_files"] = touchedFiles
			if err := wfctx.Record(ctx, "ToolCallFailed", payload); err != nil {
				return worker.ExecuteToolResponse{}, err
			}
		}
		return toolResponse, nil
	}
	if traceTool {
		if err := wfctx.Record(ctx, "ToolCallCompleted", map[string]any{
			"session_id":    task.SessionID,
			"turn_id":       task.TurnID,
			"run_id":        task.RunID,
			"tool_call_id":  callID,
			"tool_name":     call.ToolName,
			"stdout":        toolResponse.Stdout,
			"stderr":        toolResponse.Stderr,
			"exit_code":     toolResponse.ExitCode,
			"duration_ms":   firstDuration(toolResponse.DurationMS, time.Since(startedAt).Milliseconds()),
			"touched_files": touchedFiles,
		}); err != nil {
			return worker.ExecuteToolResponse{}, err
		}
	}
	return toolResponse, nil
}

func toolFailurePayload(task *TaskHandle, callID string, call worker.ToolCall, code, message string, startedAt time.Time, response worker.ExecuteToolResponse) map[string]any {
	if message == "" {
		message = response.Stderr
	}
	return map[string]any{
		"session_id":   task.SessionID,
		"turn_id":      task.TurnID,
		"run_id":       task.RunID,
		"tool_call_id": callID,
		"tool_name":    call.ToolName,
		"error_code":   code,
		"error":        message,
		"stdout":       response.Stdout,
		"stderr":       response.Stderr,
		"exit_code":    response.ExitCode,
		"duration_ms":  firstDuration(response.DurationMS, time.Since(startedAt).Milliseconds()),
	}
}

func classifyToolError(response worker.ExecuteToolResponse) string {
	text := strings.ToLower(response.Stderr + "\n" + response.Stdout)
	switch {
	case strings.Contains(text, "path escapes workspace") || strings.Contains(text, "outside workspace"):
		return "PATH_ESCAPE"
	case strings.Contains(text, "permission"):
		return "PERMISSION_DENIED"
	case strings.Contains(text, "timeout") || strings.Contains(text, "deadline"):
		return "TIMEOUT"
	case strings.Contains(text, "invalid") || strings.Contains(text, "required"):
		return "INVALID_ARGS"
	case strings.Contains(text, "network") || strings.Contains(text, "http"):
		return "NETWORK_FAILED"
	default:
		return "EXEC_FAILED"
	}
}

type toolWorkspaceState struct {
	gitFiles map[string]bool
}

func captureToolWorkspaceState(ctx context.Context, task *TaskHandle, call worker.ToolCall) toolWorkspaceState {
	if task == nil || task.Mode == ContextModeReplay || !toolMayMutateWorkspace(call.ToolName) {
		return toolWorkspaceState{}
	}
	return toolWorkspaceState{gitFiles: gitStatusFiles(ctx, task.Workspace)}
}

func inferTouchedFiles(ctx context.Context, task *TaskHandle, call worker.ToolCall, before toolWorkspaceState) []string {
	files := map[string]bool{}
	for _, file := range inferTouchedFilesFromArgs(call) {
		files[file] = true
	}
	if task != nil && task.Mode != ContextModeReplay && toolMayMutateWorkspace(call.ToolName) {
		after := gitStatusFiles(ctx, task.Workspace)
		for file := range before.gitFiles {
			if !after[file] {
				files[file] = true
			}
		}
		for file := range after {
			if !before.gitFiles[file] {
				files[file] = true
			}
		}
	}
	out := make([]string, 0, len(files))
	for file := range files {
		if strings.TrimSpace(file) != "" {
			out = append(out, file)
		}
	}
	sort.Strings(out)
	return out
}

func inferTouchedFilesFromArgs(call worker.ToolCall) []string {
	args := map[string]any{}
	_ = json.Unmarshal([]byte(call.Arguments), &args)
	switch call.ToolName {
	case "write_file", "append_file", "replace_in_file":
		if path, _ := args["path"].(string); path != "" {
			return []string{path}
		}
	case "apply_patch":
		patchText, _ := args["patch"].(string)
		return patchTouchedFiles(patchText)
	}
	return nil
}

func toolMayMutateWorkspace(toolName string) bool {
	switch toolName {
	case "write_file", "append_file", "replace_in_file", "apply_patch", "shell":
		return true
	default:
		return false
	}
}

func gitStatusFiles(ctx context.Context, workspace string) map[string]bool {
	files := map[string]bool{}
	if strings.TrimSpace(workspace) == "" {
		return files
	}
	statusCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(statusCtx, "git", "status", "--porcelain", "--untracked-files=all")
	cmd.Dir = workspace
	out, err := cmd.Output()
	if err != nil || statusCtx.Err() != nil {
		return files
	}
	for _, line := range strings.Split(string(out), "\n") {
		for _, file := range parseGitStatusLine(line) {
			files[file] = true
		}
	}
	return files
}

func parseGitStatusLine(line string) []string {
	line = strings.TrimRight(line, "\r")
	if len(line) < 4 {
		return nil
	}
	path := strings.TrimSpace(line[3:])
	if path == "" {
		return nil
	}
	if strings.Contains(path, " -> ") {
		parts := strings.Split(path, " -> ")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, unquoteGitStatusPath(part))
			}
		}
		return out
	}
	return []string{unquoteGitStatusPath(path)}
}

func unquoteGitStatusPath(path string) string {
	path = strings.TrimSpace(path)
	if len(path) >= 2 && path[0] == '"' && path[len(path)-1] == '"' {
		var decoded string
		if err := json.Unmarshal([]byte(path), &decoded); err == nil {
			return decoded
		}
	}
	return path
}

func patchTouchedFiles(patchText string) []string {
	files := map[string]bool{}
	for _, line := range strings.Split(patchText, "\n") {
		if !strings.HasPrefix(line, "+++ ") && !strings.HasPrefix(line, "--- ") {
			continue
		}
		parts := strings.Fields(line[4:])
		if len(parts) == 0 || parts[0] == "/dev/null" {
			continue
		}
		path := strings.TrimPrefix(strings.TrimPrefix(parts[0], "a/"), "b/")
		if path != "" {
			files[path] = true
		}
	}
	out := make([]string, 0, len(files))
	for file := range files {
		out = append(out, file)
	}
	sort.Strings(out)
	return out
}

func firstDuration(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
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

func executeDAGSubtasks(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, subtasks []dagSubtask) (map[string]string, error) {
	waves, err := buildDAGWaves(subtasks)
	if err != nil {
		return nil, err
	}
	if err := wfctx.Record(ctx, "DAGExecutionStarted", map[string]any{
		"subtask_count":  len(subtasks),
		"wave_count":     len(waves),
		"failure_policy": "fail_fast",
	}); err != nil {
		return nil, err
	}
	results := make(map[string]string, len(subtasks))
	for waveIndex, wave := range waves {
		ids := make([]string, 0, len(wave))
		for _, subtask := range wave {
			ids = append(ids, subtask.ID)
		}
		if err := wfctx.Record(ctx, "DAGWaveStarted", map[string]any{
			"wave":        waveIndex + 1,
			"subtask_ids": ids,
			"parallel":    len(wave) > 1,
		}); err != nil {
			return nil, err
		}
		for _, subtask := range wave {
			dependencyContext := collectDependencyContext(subtask, results)
			if err := wfctx.Record(ctx, "SubTaskDispatched", map[string]any{
				"subtask_id": subtask.ID,
				"agent_role": subtask.Agent,
				"depends_on": subtask.DependsOn,
				"wave":       waveIndex + 1,
				"run_id":     fmt.Sprintf("%s:subtask:%s", task.RunID, subtask.ID),
			}); err != nil {
				return nil, err
			}
			subtaskPrompt := strings.TrimSpace(dependencyContext + "\n\n" + subtask.Task)
			subtaskResp, err := generateThought(ctx, wfctx, task, "You are executing one DAG subtask. Return concise findings.", []worker.Message{{Role: "user", Content: subtaskPrompt}}, nil)
			if err != nil {
				_ = wfctx.Record(ctx, "SubTaskFailed", map[string]any{
					"subtask_id": subtask.ID,
					"wave":       waveIndex + 1,
					"error":      err.Error(),
				})
				return nil, err
			}
			results[subtask.ID] = subtaskResp.Thought
			if err := wfctx.Record(ctx, "SubTaskCompleted", map[string]any{
				"subtask_id":   subtask.ID,
				"result":       subtaskResp.Thought,
				"key_findings": []string{subtaskResp.Thought},
				"wave":         waveIndex + 1,
			}); err != nil {
				return nil, err
			}
		}
		if err := wfctx.Record(ctx, "DAGWaveCompleted", map[string]any{
			"wave":        waveIndex + 1,
			"subtask_ids": ids,
		}); err != nil {
			return nil, err
		}
	}
	if err := wfctx.Record(ctx, "DAGExecutionCompleted", map[string]any{
		"subtask_count": len(subtasks),
		"wave_count":    len(waves),
	}); err != nil {
		return nil, err
	}
	return results, nil
}

func buildDAGWaves(subtasks []dagSubtask) ([][]dagSubtask, error) {
	byID := make(map[string]dagSubtask, len(subtasks))
	for _, subtask := range subtasks {
		if strings.TrimSpace(subtask.ID) == "" {
			return nil, errors.New("dag subtask id is required")
		}
		if _, exists := byID[subtask.ID]; exists {
			return nil, fmt.Errorf("duplicate dag subtask id %q", subtask.ID)
		}
		byID[subtask.ID] = subtask
	}
	for _, subtask := range subtasks {
		for _, dep := range subtask.DependsOn {
			if _, ok := byID[dep]; !ok {
				return nil, fmt.Errorf("dag subtask %q depends on unknown subtask %q", subtask.ID, dep)
			}
		}
	}
	done := map[string]bool{}
	waves := [][]dagSubtask{}
	for len(done) < len(subtasks) {
		ready := make([]dagSubtask, 0)
		for _, subtask := range subtasks {
			if done[subtask.ID] {
				continue
			}
			if dagDepsSatisfied(subtask, done) {
				ready = append(ready, subtask)
			}
		}
		if len(ready) == 0 {
			return nil, errors.New("dag contains a dependency cycle")
		}
		sort.Slice(ready, func(i, j int) bool { return ready[i].ID < ready[j].ID })
		for _, subtask := range ready {
			done[subtask.ID] = true
		}
		waves = append(waves, ready)
	}
	return waves, nil
}

func dagDepsSatisfied(subtask dagSubtask, done map[string]bool) bool {
	for _, dep := range subtask.DependsOn {
		if !done[dep] {
			return false
		}
	}
	return true
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

func recordUsage(ctx context.Context, wfctx *WorkflowContext, task *TaskHandle, callID string, usage worker.TokenUsage) error {
	if usage.TotalTokens == 0 && usage.PromptTokens == 0 && usage.CompletionTokens == 0 {
		return nil
	}
	total := int64(usage.TotalTokens)
	if total == 0 {
		total = int64(usage.PromptTokens + usage.CompletionTokens)
	}
	if err := wfctx.Record(ctx, "TokenUsed", map[string]any{
		"task_id":           task.StreamID,
		"session_id":        task.SessionID,
		"turn_id":           task.TurnID,
		"run_id":            task.RunID,
		"call_id":           callID,
		"agent":             task.AgentRole,
		"model":             task.Model,
		"prompt_tokens":     usage.PromptTokens,
		"completion_tokens": usage.CompletionTokens,
		"total_tokens":      total,
		"cost_usd":          usage.CostUSD,
	}); err != nil {
		return err
	}
	budget := int64(task.Config.Agent.DefaultTokenBudget)
	if budget <= 0 {
		return nil
	}
	task.tokenMu.Lock()
	task.tokenUsed += total
	used := task.tokenUsed
	task.tokenMu.Unlock()
	if used <= budget {
		return nil
	}
	if err := wfctx.Record(ctx, "TokenBudgetExceeded", map[string]any{
		"task_id":      task.StreamID,
		"session_id":   task.SessionID,
		"turn_id":      task.TurnID,
		"run_id":       task.RunID,
		"call_id":      callID,
		"used_tokens":  used,
		"budget_limit": budget,
	}); err != nil {
		return err
	}
	return fmt.Errorf("token budget exceeded: used=%d budget=%d", used, budget)
}

func nextLLMCallID(task *TaskHandle, wfctx *WorkflowContext) string {
	runID := task.RunID
	if runID == "" {
		runID = task.StreamID
	}
	return fmt.Sprintf("llm:%s:%d", runID, wfctx.HistoryPosition()+1)
}

func llmProvider(task *TaskHandle) string {
	model := strings.ToLower(task.Model)
	switch {
	case strings.Contains(model, "deepseek"):
		return "deepseek"
	case strings.Contains(model, "gpt") || strings.Contains(model, "o3") || strings.Contains(model, "o4"):
		return "openai"
	case task.Model != "":
		return "openai-compatible"
	default:
		return "unknown"
	}
}

func hashText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func llmInputChars(systemPrompt string, messages []worker.Message) int {
	total := len(systemPrompt)
	for _, message := range messages {
		total += len(message.Role) + len(message.Content)
		for _, call := range message.ToolCalls {
			total += len(call.CallID) + len(call.ToolName) + len(call.Arguments)
		}
	}
	return total
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
