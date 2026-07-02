package worker

import (
	"context"
	"os"
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
