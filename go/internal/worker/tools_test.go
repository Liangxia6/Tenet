package worker

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalToolExecutorReadWriteFile(t *testing.T) {
	workspace := t.TempDir()
	executor := NewLocalToolExecutor(workspace, nil)

	writeResp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "write_file",
		Arguments: `{"path":"notes/todo.txt","content":"line 1\nline 2\nline 3"}`,
	})
	if writeResp.IsError {
		t.Fatalf("write_file failed: %s", writeResp.Stderr)
	}
	if !strings.Contains(writeResp.Stdout, filepath.Join("notes", "todo.txt")) {
		t.Fatalf("write stdout = %q, want written path", writeResp.Stdout)
	}

	readResp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "read_file",
		Arguments: `{"path":"notes/todo.txt","offset":2,"limit":1}`,
	})
	if readResp.IsError {
		t.Fatalf("read_file failed: %s", readResp.Stderr)
	}
	if strings.TrimSpace(readResp.Stdout) != "line 2" {
		t.Fatalf("read stdout = %q, want line 2", readResp.Stdout)
	}
}

func TestBuiltinToolDefinitionsLoadSharedManifest(t *testing.T) {
	definitions, err := LoadBuiltinToolDefinitions()
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if len(definitions) == 0 {
		t.Fatalf("expected definitions")
	}
	names := map[string]bool{}
	for _, definition := range definitions {
		names[definition.Name] = true
		if definition.ParametersSchema == "" {
			t.Fatalf("missing schema for %s", definition.Name)
		}
	}
	for _, want := range []string{"apply_patch", "git_log", "git_show", "git_branch"} {
		if !names[want] {
			t.Fatalf("missing %s in manifest definitions", want)
		}
	}
}

func TestBuiltinToolDefinitionsGateWebSearch(t *testing.T) {
	t.Setenv("TENET_WEB_SEARCH_ENABLED", "")
	t.Setenv("BRAVE_API_KEY", "")
	t.Setenv("TAVILY_API_KEY", "")
	t.Setenv("BING_API_KEY", "")
	definitions := BuiltinToolDefinitions()
	for _, definition := range definitions {
		if definition.Name == "web_search" {
			t.Fatalf("web_search should be hidden without provider key")
		}
	}
	t.Setenv("BRAVE_API_KEY", "test-key")
	definitions = BuiltinToolDefinitions()
	if !toolDefinitionNames(definitions)["web_search"] {
		t.Fatalf("web_search should be exposed when provider key exists")
	}
}

func TestBuiltinToolDefinitionsWithAllowlist(t *testing.T) {
	t.Setenv("TENET_WEB_SEARCH_ENABLED", "true")
	definitions := BuiltinToolDefinitionsWithAllowlist([]string{"read_file", "git_status"})
	names := toolDefinitionNames(definitions)
	if len(names) != 2 {
		t.Fatalf("definitions = %v, want exactly allowlisted tools", names)
	}
	for _, want := range []string{"read_file", "git_status"} {
		if !names[want] {
			t.Fatalf("missing allowlisted tool %s in %v", want, names)
		}
	}
	if names["shell"] || names["web_search"] {
		t.Fatalf("non-allowlisted tools leaked through: %v", names)
	}
}

func TestLocalToolExecutorBlocksDisallowedTool(t *testing.T) {
	executor := NewLocalToolExecutorWithAllowlist(t.TempDir(), nil, []string{"read_file"})
	resp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "shell",
		Arguments: `{"command":"pwd"}`,
	})
	if !resp.IsError {
		t.Fatalf("expected disallowed shell to fail")
	}
	if resp.ExitCode != 126 {
		t.Fatalf("exit code = %d, want 126", resp.ExitCode)
	}
	if !strings.Contains(resp.Stderr, "tool not allowed") {
		t.Fatalf("stderr = %q, want allowlist error", resp.Stderr)
	}
}

func TestValidateToolArguments(t *testing.T) {
	if err := ValidateToolArguments("read_file", map[string]any{"path": "README.md", "offset": float64(1)}); err != nil {
		t.Fatalf("valid args rejected: %v", err)
	}
	for name, tc := range map[string]struct {
		tool string
		args map[string]any
		want string
	}{
		"missing required": {
			tool: "read_file",
			args: map[string]any{},
			want: "path is required",
		},
		"wrong type": {
			tool: "read_file",
			args: map[string]any{"path": 123},
			want: "path must be a string",
		},
		"integer minimum": {
			tool: "read_file",
			args: map[string]any{"path": "README.md", "offset": float64(0)},
			want: "offset must be >= 1",
		},
		"integer maximum": {
			tool: "web_search",
			args: map[string]any{"query": "tenet", "limit": float64(99)},
			want: "limit must be <= 10",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidateToolArguments(tc.tool, tc.args)
			if err == nil {
				t.Fatalf("expected validation error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want %q", err.Error(), tc.want)
			}
		})
	}
}

func TestLocalToolExecutorValidatesArgumentsBeforeHandler(t *testing.T) {
	executor := NewLocalToolExecutor(t.TempDir(), nil)
	resp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "write_file",
		Arguments: `{"path":"notes.txt"}`,
	})
	if !resp.IsError {
		t.Fatalf("expected missing content to fail")
	}
	if resp.ExitCode != 2 {
		t.Fatalf("exit code = %d, want 2", resp.ExitCode)
	}
	if !strings.Contains(resp.Stderr, "content is required") {
		t.Fatalf("stderr = %q, want required content error", resp.Stderr)
	}
}

func toolDefinitionNames(definitions []ToolDefinition) map[string]bool {
	names := map[string]bool{}
	for _, definition := range definitions {
		names[definition.Name] = true
	}
	return names
}

func TestLocalToolExecutorBlocksPathEscape(t *testing.T) {
	workspace := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0644); err != nil {
		t.Fatalf("write outside fixture: %v", err)
	}
	link := filepath.Join(workspace, "link.txt")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	executor := NewLocalToolExecutor(workspace, nil)
	resp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "read_file",
		Arguments: `{"path":"link.txt"}`,
	})
	if !resp.IsError {
		t.Fatalf("expected symlink escape to be blocked, got stdout=%q", resp.Stdout)
	}
	if !strings.Contains(resp.Stderr, "escapes workspace") {
		t.Fatalf("stderr = %q, want workspace escape error", resp.Stderr)
	}
}

func TestLocalToolExecutorShellRunsInWorkspace(t *testing.T) {
	workspace := t.TempDir()
	executor := NewLocalToolExecutor(workspace, nil)

	resp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "shell",
		Arguments: `{"command":"pwd && printf ok","timeout_seconds":5}`,
	})
	if resp.IsError {
		t.Fatalf("shell failed: stderr=%q stdout=%q", resp.Stderr, resp.Stdout)
	}
	if !strings.Contains(resp.Stdout, workspace) || !strings.Contains(resp.Stdout, "ok") {
		t.Fatalf("stdout = %q, want workspace pwd and ok marker", resp.Stdout)
	}
}

func TestLocalToolExecutorBlocksDangerousShellPattern(t *testing.T) {
	executor := NewLocalToolExecutor(t.TempDir(), []string{"rm -rf"})
	resp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "shell",
		Arguments: `{"command":"rm -rf ./tmp"}`,
	})
	if !resp.IsError {
		t.Fatalf("expected dangerous command to be blocked")
	}
	if resp.ExitCode != 126 {
		t.Fatalf("exit code = %d, want 126", resp.ExitCode)
	}
}

func TestLocalToolExecutorShellTimeout(t *testing.T) {
	executor := NewLocalToolExecutor(t.TempDir(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp := executor.Execute(ctx, ExecuteToolRequest{
		ToolName:  "shell",
		Arguments: `{"command":"sleep 2","timeout_seconds":1}`,
	})
	if !resp.IsError || resp.ExitCode != 124 {
		t.Fatalf("response = %+v, want timeout error", resp)
	}
}

func TestLocalToolExecutorStructuredFileTools(t *testing.T) {
	workspace := t.TempDir()
	executor := NewLocalToolExecutor(workspace, nil)
	mustWrite := func(path, content string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(workspace, path)), 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(workspace, path), []byte(content), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	mustWrite("src/main.go", "package main\nfunc main(){println(\"hello\")}\n")
	mustWrite("README.md", "hello Tenet\n")

	appendResp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "append_file",
		Arguments: `{"path":"README.md","content":"more\n"}`,
	})
	if appendResp.IsError {
		t.Fatalf("append_file: %s", appendResp.Stderr)
	}
	replaceResp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "replace_in_file",
		Arguments: `{"path":"README.md","old":"Tenet","new":"Agent"}`,
	})
	if replaceResp.IsError {
		t.Fatalf("replace_in_file: %s", replaceResp.Stderr)
	}
	data, _ := os.ReadFile(filepath.Join(workspace, "README.md"))
	if !strings.Contains(string(data), "hello Agent") {
		t.Fatalf("README = %q", string(data))
	}

	listResp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "list_dir",
		Arguments: `{"recursive":true}`,
	})
	var listPayload struct {
		Entries []struct {
			Path string `json:"path"`
		} `json:"entries"`
	}
	if err := json.Unmarshal([]byte(listResp.Stdout), &listPayload); err != nil {
		t.Fatalf("decode list_dir: %v stdout=%s", err, listResp.Stdout)
	}
	if len(listPayload.Entries) < 2 {
		t.Fatalf("entries = %+v", listPayload.Entries)
	}

	searchResp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "search_files",
		Arguments: `{"query":"main"}`,
	})
	if !strings.Contains(searchResp.Stdout, "src/main.go") {
		t.Fatalf("search stdout = %s", searchResp.Stdout)
	}
	grepResp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "grep",
		Arguments: `{"query":"println"}`,
	})
	if !strings.Contains(grepResp.Stdout, "println") {
		t.Fatalf("grep stdout = %s", grepResp.Stdout)
	}
	infoResp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "file_info",
		Arguments: `{"path":"README.md"}`,
	})
	if !strings.Contains(infoResp.Stdout, "README.md") {
		t.Fatalf("info stdout = %s", infoResp.Stdout)
	}
}

func TestLocalToolExecutorApplyPatch(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "README.md"), []byte("hello Tenet\n"), 0644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	executor := NewLocalToolExecutor(workspace, nil)
	resp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "apply_patch",
		Arguments: `{"patch":"--- README.md\n+++ README.md\n@@ -1 +1 @@\n-hello Tenet\n+hello Agent\n"}`,
	})
	if resp.IsError {
		t.Fatalf("apply_patch failed: stdout=%q stderr=%q", resp.Stdout, resp.Stderr)
	}
	data, _ := os.ReadFile(filepath.Join(workspace, "README.md"))
	if string(data) != "hello Agent\n" {
		t.Fatalf("README = %q", string(data))
	}
}

func TestLocalToolExecutorGitTools(t *testing.T) {
	workspace := t.TempDir()
	runTestGit(t, workspace, "init")
	runTestGit(t, workspace, "config", "user.email", "test@example.com")
	runTestGit(t, workspace, "config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(workspace, "README.md"), []byte("hello\n"), 0644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	runTestGit(t, workspace, "add", "README.md")
	runTestGit(t, workspace, "commit", "-m", "initial commit")

	executor := NewLocalToolExecutor(workspace, nil)
	logResp := executor.Execute(context.Background(), ExecuteToolRequest{ToolName: "git_log", Arguments: `{"limit":1}`})
	if logResp.IsError || !strings.Contains(logResp.Stdout, "initial commit") {
		t.Fatalf("git_log = %+v", logResp)
	}
	showResp := executor.Execute(context.Background(), ExecuteToolRequest{ToolName: "git_show", Arguments: `{"rev":"HEAD","max_bytes":20000}`})
	if showResp.IsError || !strings.Contains(showResp.Stdout, "initial commit") {
		t.Fatalf("git_show = %+v", showResp)
	}
	branchResp := executor.Execute(context.Background(), ExecuteToolRequest{ToolName: "git_branch", Arguments: `{}`})
	if branchResp.IsError || strings.TrimSpace(branchResp.Stdout) == "" {
		t.Fatalf("git_branch = %+v", branchResp)
	}
}

func TestLocalToolExecutorHTTPFetchBlocksLocalhost(t *testing.T) {
	executor := NewLocalToolExecutor(t.TempDir(), nil)
	resp := executor.Execute(context.Background(), ExecuteToolRequest{
		ToolName:  "http_fetch",
		Arguments: `{"url":"http://127.0.0.1:12345"}`,
	})
	if !resp.IsError {
		t.Fatalf("expected localhost fetch to be blocked")
	}
	if !strings.Contains(resp.Stderr, "private or local") && !strings.Contains(resp.Stderr, "localhost") {
		t.Fatalf("stderr = %q", resp.Stderr)
	}
}

func runTestGit(t *testing.T, workspace string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = workspace
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}
