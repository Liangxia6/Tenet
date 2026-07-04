package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type LocalToolExecutor struct {
	Workspace         string
	DangerousPatterns []string
	ToolAllowlist     []string
}

func BuiltinToolDefinitions() []ToolDefinition {
	if definitions, err := LoadBuiltinToolDefinitions(); err == nil && len(definitions) > 0 {
		return filterBuiltinToolDefinitions(definitions)
	}
	return filterBuiltinToolDefinitions(staticBuiltinToolDefinitions())
}

func BuiltinToolDefinitionsWithAllowlist(allowlist []string) []ToolDefinition {
	return FilterToolDefinitions(BuiltinToolDefinitions(), allowlist)
}

func FilterToolDefinitions(definitions []ToolDefinition, allowlist []string) []ToolDefinition {
	if !HasToolAllowlist(allowlist) {
		return definitions
	}
	out := make([]ToolDefinition, 0, len(definitions))
	for _, definition := range definitions {
		if ToolAllowed(allowlist, definition.Name) {
			out = append(out, definition)
		}
	}
	return out
}

func HasToolAllowlist(allowlist []string) bool {
	for _, item := range allowlist {
		if strings.TrimSpace(item) != "" {
			return true
		}
	}
	return false
}

func ToolAllowed(allowlist []string, name string) bool {
	if !HasToolAllowlist(allowlist) {
		return true
	}
	name = strings.TrimSpace(name)
	for _, item := range allowlist {
		item = strings.TrimSpace(item)
		if item == "*" || item == name {
			return true
		}
	}
	return false
}

type toolParameterSchema struct {
	Type       string                        `json:"type"`
	Required   []string                      `json:"required"`
	Properties map[string]toolPropertySchema `json:"properties"`
}

type toolPropertySchema struct {
	Type    string   `json:"type"`
	Minimum *float64 `json:"minimum"`
	Maximum *float64 `json:"maximum"`
}

func ValidateToolArguments(toolName string, args map[string]any) error {
	definition, ok := toolDefinitionByName(toolName)
	if !ok || strings.TrimSpace(definition.ParametersSchema) == "" {
		return nil
	}
	var schema toolParameterSchema
	if err := json.Unmarshal([]byte(definition.ParametersSchema), &schema); err != nil {
		return fmt.Errorf("invalid schema for %s: %w", toolName, err)
	}
	if schema.Type != "" && schema.Type != "object" {
		return fmt.Errorf("unsupported root schema type %q", schema.Type)
	}
	for _, required := range schema.Required {
		if _, ok := args[required]; !ok {
			return fmt.Errorf("%s is required", required)
		}
	}
	for name, prop := range schema.Properties {
		value, ok := args[name]
		if !ok || value == nil || prop.Type == "" {
			continue
		}
		if err := validateToolProperty(name, value, prop); err != nil {
			return err
		}
	}
	return nil
}

func toolDefinitionByName(name string) (ToolDefinition, bool) {
	definitions, err := LoadBuiltinToolDefinitions()
	if err != nil || len(definitions) == 0 {
		definitions = staticBuiltinToolDefinitions()
	}
	for _, definition := range definitions {
		if definition.Name == name {
			return definition, true
		}
	}
	return ToolDefinition{}, false
}

func validateToolProperty(name string, value any, schema toolPropertySchema) error {
	switch schema.Type {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s must be a string", name)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s must be a boolean", name)
		}
	case "integer":
		number, ok := numericValue(value)
		if !ok || math.Trunc(number) != number {
			return fmt.Errorf("%s must be an integer", name)
		}
		if schema.Minimum != nil && number < *schema.Minimum {
			return fmt.Errorf("%s must be >= %v", name, trimNumber(*schema.Minimum))
		}
		if schema.Maximum != nil && number > *schema.Maximum {
			return fmt.Errorf("%s must be <= %v", name, trimNumber(*schema.Maximum))
		}
	default:
		return fmt.Errorf("%s has unsupported schema type %q", name, schema.Type)
	}
	return nil
}

func numericValue(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

func trimNumber(value float64) string {
	if math.Trunc(value) == value {
		return fmt.Sprintf("%.0f", value)
	}
	return fmt.Sprintf("%g", value)
}

func staticBuiltinToolDefinitions() []ToolDefinition {
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
			Name:             "append_file",
			Description:      "Append content to a workspace file, creating parent directories. Arguments: path, content.",
			ParametersSchema: `{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"]}`,
		},
		{
			Name:             "replace_in_file",
			Description:      "Replace text in a workspace file. Arguments: path, old, new, optional count.",
			ParametersSchema: `{"type":"object","properties":{"path":{"type":"string"},"old":{"type":"string"},"new":{"type":"string"},"count":{"type":"integer","minimum":1}},"required":["path","old","new"]}`,
		},
		{
			Name:             "list_dir",
			Description:      "List files and directories inside the workspace. Arguments: optional path, recursive, max_entries.",
			ParametersSchema: `{"type":"object","properties":{"path":{"type":"string"},"recursive":{"type":"boolean"},"max_entries":{"type":"integer","minimum":1,"maximum":1000}}}`,
		},
		{
			Name:             "search_files",
			Description:      "Find workspace files by name substring or glob. Arguments: query, optional path, max_results.",
			ParametersSchema: `{"type":"object","properties":{"query":{"type":"string"},"path":{"type":"string"},"max_results":{"type":"integer","minimum":1,"maximum":1000}},"required":["query"]}`,
		},
		{
			Name:             "grep",
			Description:      "Search text within workspace files. Arguments: query, optional path, case_sensitive, max_matches.",
			ParametersSchema: `{"type":"object","properties":{"query":{"type":"string"},"path":{"type":"string"},"case_sensitive":{"type":"boolean"},"max_matches":{"type":"integer","minimum":1,"maximum":1000}},"required":["query"]}`,
		},
		{
			Name:             "file_info",
			Description:      "Return metadata for a workspace file or directory. Arguments: path.",
			ParametersSchema: `{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`,
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
			Name:             "git_status",
			Description:      "Return git status --short inside the workspace.",
			ParametersSchema: `{"type":"object","properties":{}}`,
		},
		{
			Name:             "git_diff",
			Description:      "Return git diff inside the workspace. Arguments: optional path, max_bytes.",
			ParametersSchema: `{"type":"object","properties":{"path":{"type":"string"},"max_bytes":{"type":"integer","minimum":1,"maximum":200000}}}`,
		},
		{
			Name:             "apply_patch",
			Description:      "Apply a unified diff patch inside the workspace. Arguments: patch, optional strip.",
			ParametersSchema: `{"type":"object","properties":{"patch":{"type":"string"},"strip":{"type":"integer","minimum":0,"maximum":10}},"required":["patch"]}`,
		},
		{
			Name:             "git_log",
			Description:      "Return recent git commits. Arguments: optional limit, path, max_bytes.",
			ParametersSchema: `{"type":"object","properties":{"limit":{"type":"integer","minimum":1,"maximum":100},"path":{"type":"string"},"max_bytes":{"type":"integer","minimum":1,"maximum":200000}}}`,
		},
		{
			Name:             "git_show",
			Description:      "Show a git revision or object. Arguments: rev, optional max_bytes.",
			ParametersSchema: `{"type":"object","properties":{"rev":{"type":"string"},"max_bytes":{"type":"integer","minimum":1,"maximum":200000}},"required":["rev"]}`,
		},
		{
			Name:             "git_branch",
			Description:      "Return git branch information. Arguments: optional all.",
			ParametersSchema: `{"type":"object","properties":{"all":{"type":"boolean"}}}`,
		},
		{
			Name:             "http_fetch",
			Description:      "Fetch a URL with GET and return status, content type, and body preview. Arguments: url, optional max_bytes.",
			ParametersSchema: `{"type":"object","properties":{"url":{"type":"string"},"max_bytes":{"type":"integer","minimum":1,"maximum":200000}},"required":["url"]}`,
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

func LoadBuiltinToolDefinitions() ([]ToolDefinition, error) {
	type manifestTool struct {
		Name             string `json:"name"`
		Description      string `json:"description"`
		ParametersSchema string `json:"parameters_schema"`
	}
	var lastErr error
	for _, candidate := range []string{"tools/builtin_tools.json", "../tools/builtin_tools.json", "../../tools/builtin_tools.json", "../../../tools/builtin_tools.json"} {
		data, err := os.ReadFile(candidate)
		if err != nil {
			lastErr = err
			continue
		}
		var manifest []manifestTool
		if err := json.Unmarshal(data, &manifest); err != nil {
			return nil, err
		}
		definitions := make([]ToolDefinition, 0, len(manifest))
		for _, item := range manifest {
			definitions = append(definitions, ToolDefinition{
				Name:             item.Name,
				Description:      item.Description,
				ParametersSchema: item.ParametersSchema,
			})
		}
		return definitions, nil
	}
	return nil, lastErr
}

func filterBuiltinToolDefinitions(definitions []ToolDefinition) []ToolDefinition {
	out := make([]ToolDefinition, 0, len(definitions))
	for _, definition := range definitions {
		if definition.Name == "web_search" && !webSearchDefinitionEnabled() {
			continue
		}
		out = append(out, definition)
	}
	return out
}

func webSearchDefinitionEnabled() bool {
	if strings.EqualFold(os.Getenv("TENET_WEB_SEARCH_ENABLED"), "true") {
		return true
	}
	return os.Getenv("BRAVE_API_KEY") != "" || os.Getenv("TAVILY_API_KEY") != "" || os.Getenv("BING_API_KEY") != ""
}

func NewLocalToolExecutor(workspace string, dangerousPatterns []string) *LocalToolExecutor {
	return NewLocalToolExecutorWithAllowlist(workspace, dangerousPatterns, nil)
}

func NewLocalToolExecutorWithAllowlist(workspace string, dangerousPatterns []string, toolAllowlist []string) *LocalToolExecutor {
	if workspace == "" {
		workspace = "."
	}
	if len(dangerousPatterns) == 0 {
		dangerousPatterns = []string{"rm -rf /", "mkfs.", "dd if=", ":(){ :|:& };:"}
	}
	return &LocalToolExecutor{
		Workspace:         workspace,
		DangerousPatterns: dangerousPatterns,
		ToolAllowlist:     toolAllowlist,
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
	if !ToolAllowed(e.ToolAllowlist, toolName) {
		return toolError("tool not allowed by safety.tool_allowlist: "+toolName, 126)
	}
	if err := ValidateToolArguments(toolName, args); err != nil {
		return toolError("invalid arguments: "+err.Error(), 2)
	}
	switch toolName {
	case "read_file":
		return e.readFile(workspace, args)
	case "write_file":
		return e.writeFile(workspace, args)
	case "append_file":
		return e.appendFile(workspace, args)
	case "replace_in_file":
		return e.replaceInFile(workspace, args)
	case "list_dir":
		return e.listDir(workspace, args)
	case "search_files":
		return e.searchFiles(workspace, args)
	case "grep":
		return e.grep(workspace, args)
	case "file_info":
		return e.fileInfo(workspace, args)
	case "shell":
		return e.shell(ctx, workspace, args)
	case "git_status":
		return e.gitStatus(ctx, workspace)
	case "git_diff":
		return e.gitDiff(ctx, workspace, args)
	case "apply_patch":
		return e.applyPatch(ctx, workspace, args)
	case "git_log":
		return e.gitLog(ctx, workspace, args)
	case "git_show":
		return e.gitShow(ctx, workspace, args)
	case "git_branch":
		return e.gitBranch(ctx, workspace, args)
	case "http_fetch":
		return e.httpFetch(ctx, args)
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

func (e *LocalToolExecutor) appendFile(workspace string, args map[string]any) ExecuteToolResponse {
	path, _ := args["path"].(string)
	content, _ := args["content"].(string)
	safe, err := safeWorkspacePath(workspace, path, false)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	if err := os.MkdirAll(filepath.Dir(safe), 0755); err != nil {
		return toolError(err.Error(), 1)
	}
	file, err := os.OpenFile(safe, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	if _, err := file.WriteString(content); err != nil {
		_ = file.Close()
		return toolError(err.Error(), 1)
	}
	if err := file.Close(); err != nil {
		return toolError(err.Error(), 1)
	}
	rel, _ := filepath.Rel(mustAbs(workspace), safe)
	return ExecuteToolResponse{Stdout: "appended " + rel, ExitCode: 0}
}

func (e *LocalToolExecutor) replaceInFile(workspace string, args map[string]any) ExecuteToolResponse {
	path, _ := args["path"].(string)
	old, _ := args["old"].(string)
	newValue, _ := args["new"].(string)
	if old == "" {
		return toolError("old is required", 1)
	}
	safe, err := safeWorkspacePath(workspace, path, true)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	data, err := os.ReadFile(safe)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	content := string(data)
	count := intArg(args, "count", -1)
	replaced := strings.Count(content, old)
	if count > 0 && replaced > count {
		replaced = count
	}
	if replaced == 0 {
		return toolError("old text not found", 1)
	}
	updated := strings.Replace(content, old, newValue, count)
	if err := os.WriteFile(safe, []byte(updated), 0644); err != nil {
		return toolError(err.Error(), 1)
	}
	return jsonResponse(map[string]any{"replaced": replaced, "path": path})
}

func (e *LocalToolExecutor) listDir(workspace string, args map[string]any) ExecuteToolResponse {
	path, _ := args["path"].(string)
	if path == "" {
		path = "."
	}
	root, err := safeWorkspacePath(workspace, path, true)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	recursive, _ := args["recursive"].(bool)
	maxEntries := intArg(args, "max_entries", 200)
	if maxEntries <= 0 || maxEntries > 1000 {
		maxEntries = 200
	}
	workspaceRoot := mustAbs(workspace)
	entries := []map[string]any{}
	if recursive {
		err = filepath.WalkDir(root, func(current string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if current == root {
				return nil
			}
			if len(entries) >= maxEntries {
				return filepath.SkipAll
			}
			if shouldSkipToolDir(d.Name()) && d.IsDir() {
				return filepath.SkipDir
			}
			info, _ := d.Info()
			rel, _ := filepath.Rel(workspaceRoot, current)
			entries = append(entries, map[string]any{"path": rel, "dir": d.IsDir(), "size": sizeOf(info)})
			return nil
		})
	} else {
		dirEntries, readErr := os.ReadDir(root)
		err = readErr
		for _, d := range dirEntries {
			if len(entries) >= maxEntries {
				break
			}
			info, _ := d.Info()
			rel, _ := filepath.Rel(workspaceRoot, filepath.Join(root, d.Name()))
			entries = append(entries, map[string]any{"path": rel, "dir": d.IsDir(), "size": sizeOf(info)})
		}
	}
	if err != nil {
		return toolError(err.Error(), 1)
	}
	sort.Slice(entries, func(i, j int) bool { return fmt.Sprint(entries[i]["path"]) < fmt.Sprint(entries[j]["path"]) })
	return jsonResponse(map[string]any{"entries": entries, "truncated": len(entries) >= maxEntries})
}

func (e *LocalToolExecutor) searchFiles(workspace string, args map[string]any) ExecuteToolResponse {
	query, _ := args["query"].(string)
	if strings.TrimSpace(query) == "" {
		return toolError("query is required", 1)
	}
	base, _ := args["path"].(string)
	if base == "" {
		base = "."
	}
	root, err := safeWorkspacePath(workspace, base, true)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	maxResults := intArg(args, "max_results", 100)
	if maxResults <= 0 || maxResults > 1000 {
		maxResults = 100
	}
	workspaceRoot := mustAbs(workspace)
	matches := []string{}
	queryLower := strings.ToLower(query)
	err = filepath.WalkDir(root, func(current string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if shouldSkipToolDir(d.Name()) && d.IsDir() {
			return filepath.SkipDir
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		globMatch, _ := filepath.Match(query, name)
		if globMatch || strings.Contains(strings.ToLower(name), queryLower) {
			rel, _ := filepath.Rel(workspaceRoot, current)
			matches = append(matches, rel)
			if len(matches) >= maxResults {
				return filepath.SkipAll
			}
		}
		return nil
	})
	if err != nil {
		return toolError(err.Error(), 1)
	}
	return jsonResponse(map[string]any{"matches": matches, "truncated": len(matches) >= maxResults})
}

func (e *LocalToolExecutor) grep(workspace string, args map[string]any) ExecuteToolResponse {
	query, _ := args["query"].(string)
	if query == "" {
		return toolError("query is required", 1)
	}
	base, _ := args["path"].(string)
	if base == "" {
		base = "."
	}
	root, err := safeWorkspacePath(workspace, base, true)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	caseSensitive, _ := args["case_sensitive"].(bool)
	maxMatches := intArg(args, "max_matches", 100)
	if maxMatches <= 0 || maxMatches > 1000 {
		maxMatches = 100
	}
	needle := query
	if !caseSensitive {
		needle = strings.ToLower(query)
	}
	workspaceRoot := mustAbs(workspace)
	matches := []map[string]any{}
	err = filepath.WalkDir(root, func(current string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if shouldSkipToolDir(d.Name()) && d.IsDir() {
			return filepath.SkipDir
		}
		if d.IsDir() || len(matches) >= maxMatches {
			return nil
		}
		data, err := os.ReadFile(current)
		if err != nil || len(data) > 2*1024*1024 || strings.ContainsRune(string(data[:min(len(data), 8000)]), '\x00') {
			return nil
		}
		lines := strings.Split(string(data), "\n")
		for i, line := range lines {
			haystack := line
			if !caseSensitive {
				haystack = strings.ToLower(line)
			}
			if strings.Contains(haystack, needle) {
				rel, _ := filepath.Rel(workspaceRoot, current)
				matches = append(matches, map[string]any{"path": rel, "line": i + 1, "text": line})
				if len(matches) >= maxMatches {
					return filepath.SkipAll
				}
			}
		}
		return nil
	})
	if err != nil {
		return toolError(err.Error(), 1)
	}
	return jsonResponse(map[string]any{"matches": matches, "truncated": len(matches) >= maxMatches})
}

func (e *LocalToolExecutor) fileInfo(workspace string, args map[string]any) ExecuteToolResponse {
	path, _ := args["path"].(string)
	safe, err := safeWorkspacePath(workspace, path, true)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	info, err := os.Stat(safe)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	rel, _ := filepath.Rel(mustAbs(workspace), safe)
	return jsonResponse(map[string]any{"path": rel, "dir": info.IsDir(), "size": info.Size(), "mode": info.Mode().String(), "modified": info.ModTime().UTC().Format(time.RFC3339)})
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

func (e *LocalToolExecutor) gitStatus(ctx context.Context, workspace string) ExecuteToolResponse {
	return e.shell(ctx, workspace, map[string]any{"command": "git status --short", "timeout_seconds": 30})
}

func (e *LocalToolExecutor) gitDiff(ctx context.Context, workspace string, args map[string]any) ExecuteToolResponse {
	target, _ := args["path"].(string)
	command := "git diff --"
	if target != "" {
		if _, err := safeWorkspacePath(workspace, target, false); err != nil {
			return toolError(err.Error(), 1)
		}
		command = "git diff -- " + shellQuote(target)
	}
	resp := e.shell(ctx, workspace, map[string]any{"command": command, "timeout_seconds": 30})
	maxBytes := intArg(args, "max_bytes", 60000)
	if maxBytes <= 0 || maxBytes > 200000 {
		maxBytes = 60000
	}
	if len(resp.Stdout) > maxBytes {
		resp.Stdout = resp.Stdout[:maxBytes] + "\n...truncated..."
	}
	return resp
}

func (e *LocalToolExecutor) applyPatch(ctx context.Context, workspace string, args map[string]any) ExecuteToolResponse {
	patchText, _ := args["patch"].(string)
	if strings.TrimSpace(patchText) == "" {
		return toolError("patch is required", 1)
	}
	if err := validatePatchPaths(workspace, patchText); err != nil {
		return toolError(err.Error(), 1)
	}
	strip := intArg(args, "strip", 0)
	if strip < 0 || strip > 10 {
		return toolError("strip must be between 0 and 10", 1)
	}
	root, err := canonicalWorkspaceRoot(workspace)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	patchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(patchCtx, "patch", fmt.Sprintf("-p%d", strip), "--forward")
	cmd.Dir = root
	cmd.Stdin = strings.NewReader(patchText)
	out, err := cmd.CombinedOutput()
	if patchCtx.Err() == context.DeadlineExceeded {
		return ExecuteToolResponse{Stdout: string(out), Stderr: "patch timed out", ExitCode: 124, IsError: true}
	}
	if err != nil {
		exitCode := 1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}
		return ExecuteToolResponse{Stdout: string(out), Stderr: string(out), ExitCode: exitCode, IsError: true}
	}
	return ExecuteToolResponse{Stdout: string(out), ExitCode: 0}
}

func (e *LocalToolExecutor) gitLog(ctx context.Context, workspace string, args map[string]any) ExecuteToolResponse {
	limit := intArg(args, "limit", 20)
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	gitArgs := []string{"log", fmt.Sprintf("-%d", limit), "--date=iso", "--pretty=format:%h%x09%ad%x09%s"}
	if target, _ := args["path"].(string); target != "" {
		if _, err := safeWorkspacePath(workspace, target, false); err != nil {
			return toolError(err.Error(), 1)
		}
		gitArgs = append(gitArgs, "--", target)
	}
	return runGit(ctx, workspace, gitArgs, boundedMaxBytes(args))
}

func (e *LocalToolExecutor) gitShow(ctx context.Context, workspace string, args map[string]any) ExecuteToolResponse {
	rev, _ := args["rev"].(string)
	if strings.TrimSpace(rev) == "" {
		return toolError("rev is required", 1)
	}
	if strings.ContainsAny(rev, "\x00\r\n") {
		return toolError("rev contains invalid control characters", 1)
	}
	return runGit(ctx, workspace, []string{"show", "--stat", "--patch", rev}, boundedMaxBytes(args))
}

func (e *LocalToolExecutor) gitBranch(ctx context.Context, workspace string, args map[string]any) ExecuteToolResponse {
	if boolArg(args, "all", false) {
		return runGit(ctx, workspace, []string{"branch", "--all", "--verbose"}, boundedMaxBytes(args))
	}
	return runGit(ctx, workspace, []string{"branch", "--show-current"}, boundedMaxBytes(args))
}

func (e *LocalToolExecutor) httpFetch(ctx context.Context, args map[string]any) ExecuteToolResponse {
	rawURL, _ := args["url"].(string)
	if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
		return toolError("url must start with http:// or https://", 1)
	}
	if err := validateFetchURL(ctx, rawURL); err != nil {
		return toolError(err.Error(), 1)
	}
	maxBytes := intArg(args, "max_bytes", 60000)
	if maxBytes <= 0 || maxBytes > 200000 {
		maxBytes = 60000
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, int64(maxBytes)+1))
	if err != nil {
		return toolError(err.Error(), 1)
	}
	truncated := len(data) > maxBytes
	if truncated {
		data = data[:maxBytes]
	}
	return jsonResponse(map[string]any{"status": resp.StatusCode, "content_type": resp.Header.Get("Content-Type"), "body": string(data), "truncated": truncated})
}

func validateFetchURL(ctx context.Context, rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	host := parsed.Hostname()
	if host == "" {
		return errors.New("url host is required")
	}
	if strings.EqualFold(host, "localhost") {
		return errors.New("refusing to fetch localhost URL")
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return err
	}
	for _, ip := range ips {
		if blockedFetchIP(ip) {
			return fmt.Errorf("refusing to fetch private or local address: %s", ip.String())
		}
	}
	return nil
}

func blockedFetchIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsUnspecified()
}

func runGit(ctx context.Context, workspace string, args []string, maxBytes int) ExecuteToolResponse {
	root, err := canonicalWorkspaceRoot(workspace)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	if maxBytes <= 0 || maxBytes > 200000 {
		maxBytes = 60000
	}
	gitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(gitCtx, "git", args...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	stdout := string(out)
	if len(stdout) > maxBytes {
		stdout = stdout[:maxBytes] + "\n...truncated..."
	}
	if gitCtx.Err() == context.DeadlineExceeded {
		return ExecuteToolResponse{Stdout: stdout, Stderr: "git command timed out", ExitCode: 124, IsError: true}
	}
	if err != nil {
		exitCode := 1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}
		return ExecuteToolResponse{Stdout: stdout, Stderr: stdout, ExitCode: exitCode, IsError: true}
	}
	return ExecuteToolResponse{Stdout: stdout, ExitCode: 0}
}

func validatePatchPaths(workspace, patchText string) error {
	for _, line := range strings.Split(patchText, "\n") {
		if !strings.HasPrefix(line, "+++ ") && !strings.HasPrefix(line, "--- ") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(strings.TrimPrefix(line, "+++ "), "--- "))
		if len(fields) == 0 || fields[0] == "/dev/null" {
			continue
		}
		path := strings.TrimPrefix(fields[0], "a/")
		path = strings.TrimPrefix(path, "b/")
		if _, err := safeWorkspacePath(workspace, path, false); err != nil {
			return err
		}
	}
	return nil
}

func boundedMaxBytes(args map[string]any) int {
	maxBytes := intArg(args, "max_bytes", 60000)
	if maxBytes <= 0 || maxBytes > 200000 {
		return 60000
	}
	return maxBytes
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

func boolArg(args map[string]any, key string, fallback bool) bool {
	switch value := args[key].(type) {
	case bool:
		return value
	case string:
		return value == "true" || value == "1" || value == "yes"
	default:
		return fallback
	}
}

func jsonResponse(payload any) ExecuteToolResponse {
	data, err := json.Marshal(payload)
	if err != nil {
		return toolError(err.Error(), 1)
	}
	return ExecuteToolResponse{Stdout: string(data), ExitCode: 0}
}

func shouldSkipToolDir(name string) bool {
	switch name {
	case ".git", "node_modules", ".venv", "dist", "build", "__pycache__":
		return true
	default:
		return false
	}
}

func sizeOf(info os.FileInfo) int64 {
	if info == nil {
		return 0
	}
	return info.Size()
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func toolError(message string, exitCode int) ExecuteToolResponse {
	return ExecuteToolResponse{Stderr: message, ExitCode: exitCode, IsError: true}
}
