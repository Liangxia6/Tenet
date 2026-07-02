package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type LocalToolExecutor struct {
	Workspace         string
	DangerousPatterns []string
}

func BuiltinToolDefinitions() []ToolDefinition {
	return []ToolDefinition{
		{
			Name:        "read_file",
			Description: "Read a file inside the workspace. Arguments: path, optional offset line number, optional limit line count.",
			ParametersSchema: `{
				"type":"object",
				"properties":{
					"path":{"type":"string"},
					"offset":{"type":"integer","minimum":1},
					"limit":{"type":"integer","minimum":1}
				},
				"required":["path"]
			}`,
		},
		{
			Name:        "write_file",
			Description: "Overwrite a file inside the workspace, creating parent directories. Arguments: path, content.",
			ParametersSchema: `{
				"type":"object",
				"properties":{
					"path":{"type":"string"},
					"content":{"type":"string"}
				},
				"required":["path","content"]
			}`,
		},
		{
			Name:        "shell",
			Description: "Run a shell command inside the workspace. Arguments: command, optional timeout_seconds.",
			ParametersSchema: `{
				"type":"object",
				"properties":{
					"command":{"type":"string"},
					"timeout_seconds":{"type":"integer","minimum":1,"maximum":600}
				},
				"required":["command"]
			}`,
		},
		{
			Name:        "web_search",
			Description: "Search the web. Current local MVP returns a TODO stub unless a remote worker implements it.",
			ParametersSchema: `{
				"type":"object",
				"properties":{
					"query":{"type":"string"},
					"limit":{"type":"integer","minimum":1,"maximum":10}
				},
				"required":["query"]
			}`,
		},
	}
}

func NewLocalToolExecutor(workspace string, dangerousPatterns []string) *LocalToolExecutor {
	if workspace == "" {
		workspace = "."
	}
	if len(dangerousPatterns) == 0 {
		dangerousPatterns = []string{"rm -rf /", "mkfs.", "dd if=", ":(){ :|:& };:"}
	}
	return &LocalToolExecutor{
		Workspace:         workspace,
		DangerousPatterns: dangerousPatterns,
	}
}

func (e *LocalToolExecutor) Execute(ctx context.Context, req ExecuteToolRequest) ExecuteToolResponse {
	workspace := req.Workspace
	if workspace == "" {
		workspace = e.Workspace
	}
	start := time.Now()
	result := e.execute(ctx, workspace, req.ToolName, req.Arguments)
	result.DurationMS = time.Since(start).Milliseconds()
	return result
}

func (e *LocalToolExecutor) execute(ctx context.Context, workspace, toolName, rawArgs string) ExecuteToolResponse {
	var args map[string]any
	if strings.TrimSpace(rawArgs) == "" {
		args = map[string]any{}
	} else if err := json.Unmarshal([]byte(rawArgs), &args); err != nil {
		return toolError(fmt.Sprintf("invalid JSON arguments: %v", err), 1)
	}
	switch toolName {
	case "read_file":
		return e.readFile(workspace, args)
	case "write_file":
		return e.writeFile(workspace, args)
	case "shell":
		return e.shell(ctx, workspace, args)
	case "web_search":
		return ExecuteToolResponse{
			Stdout:   `{"status":"TODO","results":[]}`,
			ExitCode: 0,
		}
	default:
		return toolError("unknown tool: "+toolName, 1)
	}
}

func (e *LocalToolExecutor) readFile(workspace string, args map[string]any) ExecuteToolResponse {
	path, _ := args["path"].(string)
	safe, err := safeWorkspacePath(workspace, path, true)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	data, err := os.ReadFile(safe)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	lines := strings.Split(string(data), "\n")
	offset := intArg(args, "offset", 1)
	limit := intArg(args, "limit", 500)
	if offset < 1 {
		offset = 1
	}
	if limit < 1 {
		limit = 500
	}
	start := offset - 1
	if start >= len(lines) {
		return ExecuteToolResponse{Stdout: "", ExitCode: 0}
	}
	end := start + limit
	if end > len(lines) {
		end = len(lines)
	}
	return ExecuteToolResponse{Stdout: strings.Join(lines[start:end], "\n"), ExitCode: 0}
}

func (e *LocalToolExecutor) writeFile(workspace string, args map[string]any) ExecuteToolResponse {
	path, _ := args["path"].(string)
	content, _ := args["content"].(string)
	safe, err := safeWorkspacePath(workspace, path, false)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	if err := os.MkdirAll(filepath.Dir(safe), 0755); err != nil {
		return toolError(err.Error(), 1)
	}
	if err := os.WriteFile(safe, []byte(content), 0644); err != nil {
		return toolError(err.Error(), 1)
	}
	rel, _ := filepath.Rel(mustAbs(workspace), safe)
	return ExecuteToolResponse{Stdout: "wrote " + rel, ExitCode: 0}
}

func (e *LocalToolExecutor) shell(ctx context.Context, workspace string, args map[string]any) ExecuteToolResponse {
	command, _ := args["command"].(string)
	if strings.TrimSpace(command) == "" {
		return toolError("command is required", 1)
	}
	for _, pattern := range e.DangerousPatterns {
		if pattern != "" && strings.Contains(command, pattern) {
			return toolError("blocked dangerous command pattern: "+pattern, 126)
		}
	}
	root, err := canonicalWorkspaceRoot(workspace)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	timeout := intArg(args, "timeout_seconds", 60)
	if timeout < 1 {
		timeout = 60
	}
	if timeout > 600 {
		timeout = 600
	}
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	cmd := exec.CommandContext(runCtx, "/bin/sh", "-c", command)
	cmd.Dir = root
	out, err := cmd.Output()
	stderr := ""
	if exitErr := (*exec.ExitError)(nil); errors.As(err, &exitErr) {
		stderr = string(exitErr.Stderr)
	}
	if runCtx.Err() == context.DeadlineExceeded {
		return toolError("command timed out", 124)
	}
	if err != nil {
		exitCode := 1
		if exitErr := (*exec.ExitError)(nil); errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}
		return ExecuteToolResponse{Stdout: string(out), Stderr: stderr, ExitCode: exitCode, IsError: true}
	}
	return ExecuteToolResponse{Stdout: string(out), ExitCode: 0}
}

func safeWorkspacePath(workspace, userPath string, mustExist bool) (string, error) {
	if strings.TrimSpace(userPath) == "" {
		return "", errors.New("path is required")
	}
	root, err := canonicalWorkspaceRoot(workspace)
	if err != nil {
		return "", err
	}
	cleaned := filepath.Clean(userPath)
	if filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("absolute paths are not allowed: %s", userPath)
	}
	candidate := filepath.Join(root, cleaned)
	if resolved, err := filepath.EvalSymlinks(candidate); err == nil {
		candidate = resolved
	} else if mustExist {
		return "", err
	} else {
		parent := filepath.Dir(candidate)
		if resolvedParent, err := filepath.EvalSymlinks(parent); err == nil {
			candidate = filepath.Join(resolvedParent, filepath.Base(candidate))
		}
	}
	rel, err := filepath.Rel(root, candidate)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("path escapes workspace: %s", userPath)
	}
	return candidate, nil
}

func canonicalWorkspaceRoot(workspace string) (string, error) {
	root, err := filepath.Abs(workspace)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	return root, nil
}

func mustAbs(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

func intArg(args map[string]any, key string, fallback int) int {
	switch value := args[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case json.Number:
		out, err := value.Int64()
		if err == nil {
			return int(out)
		}
	case string:
		var out int
		if _, err := fmt.Sscanf(value, "%d", &out); err == nil {
			return out
		}
	}
	return fallback
}

func toolError(message string, exitCode int) ExecuteToolResponse {
	return ExecuteToolResponse{Stderr: message, ExitCode: exitCode, IsError: true}
}
