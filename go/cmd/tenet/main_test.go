package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/storage"
	"github.com/tenet/orchestrator/internal/worker"
)

func TestBuildTaskClientDeepSeekDefaults(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "deepseek-key")

	cfg := config.Default()
	client, model, err := buildTaskClient("deepseek", "", "", "", "", t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("buildTaskClient() error = %v", err)
	}
	if client == nil {
		t.Fatalf("expected client")
	}
	if model != worker.DefaultDeepSeekModel {
		t.Fatalf("model = %q, want %q", model, worker.DefaultDeepSeekModel)
	}
}

func TestBuildTaskClientDeepSeekMissingKey(t *testing.T) {
	cfg := config.Default()
	_, _, err := buildTaskClient("deepseek", "", "", "", "", t.TempDir(), cfg)
	if err == nil {
		t.Fatalf("expected missing key error")
	}
	if !strings.Contains(err.Error(), "DEEPSEEK_API_KEY") {
		t.Fatalf("error = %q, want DEEPSEEK_API_KEY hint", err.Error())
	}
}

func TestBuildTaskClientDeepSeekProviderOverride(t *testing.T) {
	cfg := config.Default()
	cfg.LLM = config.LLMProviders{{
		Name:         "deepseek",
		Adapter:      "openai",
		BaseURL:      "https://proxy.example/v1",
		APIKey:       "provider-key",
		DefaultModel: "deepseek-v4-pro",
	}}

	client, model, err := buildTaskClient("deepseek", "", "", "", "", t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("buildTaskClient() error = %v", err)
	}
	if client == nil {
		t.Fatalf("expected client")
	}
	if model != "deepseek-v4-pro" {
		t.Fatalf("model = %q, want deepseek-v4-pro", model)
	}
}

func TestBuildTaskClientAppliesToolAllowlistToLocalAgent(t *testing.T) {
	cfg := config.Default()
	cfg.Safety.ToolAllowlist = []string{"read_file"}
	client, _, err := buildTaskClient("echo", "", "", "", "", t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("buildTaskClient() error = %v", err)
	}
	local, ok := client.(*worker.LocalAgentClient)
	if !ok {
		t.Fatalf("client type = %T, want *LocalAgentClient", client)
	}
	resp, err := local.ExecuteTool(context.Background(), worker.ExecuteToolRequest{
		ToolName:  "shell",
		Arguments: `{"command":"pwd"}`,
	})
	if err != nil {
		t.Fatalf("ExecuteTool returned transport error: %v", err)
	}
	if !resp.IsError || !strings.Contains(resp.Stderr, "tool not allowed") {
		t.Fatalf("response = %+v, want allowlist denial", resp)
	}
}

func TestHTTPAPITaskLifecycle(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "tenet.db")
	cfg.Workspace.BasePath = t.TempDir()
	cfg.GRPC.ExecuteTimeoutSeconds = 30
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: 16})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	server := httptest.NewServer(newAPIHandler(store, cfg))
	defer server.Close()

	createResp := postJSON(t, server.URL+"/tasks", map[string]any{
		"query":     "http task",
		"workspace": t.TempDir(),
		"worker":    "echo",
		"workflow":  "simple",
	})
	var created map[string]any
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode created: %v", err)
	}
	_ = createResp.Body.Close()
	taskID, _ := created["task_id"].(string)
	if taskID == "" {
		t.Fatalf("created = %+v", created)
	}

	statusResp, err := http.Get(server.URL + "/tasks/" + url.PathEscape(taskID))
	if err != nil {
		t.Fatalf("get status: %v", err)
	}
	var status map[string]any
	if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	_ = statusResp.Body.Close()
	if status["status"] != "COMPLETED" {
		t.Fatalf("status = %+v", status)
	}

	messageResp := postJSON(t, server.URL+"/tasks/"+url.PathEscape(taskID)+"/messages", map[string]any{
		"message":   "continue this session",
		"workspace": t.TempDir(),
		"worker":    "echo",
		"workflow":  "simple",
	})
	var continued map[string]any
	if err := json.NewDecoder(messageResp.Body).Decode(&continued); err != nil {
		t.Fatalf("decode continued: %v", err)
	}
	_ = messageResp.Body.Close()
	if continued["task_id"] != taskID {
		t.Fatalf("continued = %+v", continued)
	}

	statusResp2, err := http.Get(server.URL + "/tasks/" + url.PathEscape(taskID))
	if err != nil {
		t.Fatalf("get continued status: %v", err)
	}
	var status2 map[string]any
	if err := json.NewDecoder(statusResp2.Body).Decode(&status2); err != nil {
		t.Fatalf("decode continued status: %v", err)
	}
	_ = statusResp2.Body.Close()
	turns, _ := status2["turns"].([]any)
	runs, _ := status2["runs"].([]any)
	if len(turns) != 2 || len(runs) != 2 {
		t.Fatalf("continued status turns=%v runs=%v status=%+v", turns, runs, status2)
	}

	eventsResp, err := http.Get(server.URL + "/tasks/" + url.PathEscape(taskID) + "/events")
	if err != nil {
		t.Fatalf("get events: %v", err)
	}
	var events []map[string]any
	if err := json.NewDecoder(eventsResp.Body).Decode(&events); err != nil {
		t.Fatalf("decode events: %v", err)
	}
	_ = eventsResp.Body.Close()
	if len(events) == 0 {
		t.Fatalf("expected events")
	}
	counts := map[string]int{}
	for _, event := range events {
		eventType, _ := event["event_type"].(string)
		counts[eventType]++
	}
	if counts["SessionCreated"] != 1 || counts["TurnCreated"] != 2 || counts["RunStarted"] != 2 || counts["RunCompleted"] != 2 {
		t.Fatalf("event counts = %+v", counts)
	}

	resumeResp := postJSON(t, server.URL+"/tasks/"+url.PathEscape(taskID)+"/resume", map[string]any{"note": "continue"})
	if resumeResp.StatusCode != http.StatusOK {
		t.Fatalf("resume status = %d", resumeResp.StatusCode)
	}
	_ = resumeResp.Body.Close()

	forkResp := postJSON(t, server.URL+"/tasks/"+url.PathEscape(taskID)+"/fork", map[string]any{
		"seq":               2,
		"query":             "branch",
		"restore_workspace": false,
	})
	var fork map[string]any
	if err := json.NewDecoder(forkResp.Body).Decode(&fork); err != nil {
		t.Fatalf("decode fork: %v", err)
	}
	_ = forkResp.Body.Close()
	if fork["stream_id"] == "" {
		t.Fatalf("fork = %+v", fork)
	}
}

func TestHTTPAPIVersionedRoutesAndStructuredErrors(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "tenet.db")
	cfg.Workspace.BasePath = t.TempDir()
	cfg.GRPC.ExecuteTimeoutSeconds = 30
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: 16})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	server := httptest.NewServer(newAPIHandler(store, cfg))
	defer server.Close()

	createResp := postJSON(t, server.URL+"/api/v1/tasks", map[string]any{
		"query":     "versioned task",
		"workspace": t.TempDir(),
		"worker":    "echo",
		"workflow":  "simple",
	})
	var created map[string]any
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode created: %v", err)
	}
	_ = createResp.Body.Close()
	if created["task_id"] == "" {
		t.Fatalf("created = %+v", created)
	}

	badResp, err := http.Post(server.URL+"/api/v1/tasks", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("post bad request: %v", err)
	}
	defer badResp.Body.Close()
	if badResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad status = %d", badResp.StatusCode)
	}
	var payload map[string]any
	if err := json.NewDecoder(badResp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	errObj, _ := payload["error"].(map[string]any)
	if errObj["code"] != "BAD_REQUEST" || errObj["message"] == "" {
		t.Fatalf("error payload = %+v", payload)
	}
}

func TestHTTPAPISkillsDiscovery(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "tenet.db")
	cfg.Skills.Path = t.TempDir()
	cfg.Skills.AutoDiscover = true
	if err := os.WriteFile(filepath.Join(cfg.Skills.Path, "demo.skill.json"), []byte(`{
		"name": "demo",
		"tools": [{"name": "demo_tool", "parameters_schema": "{\"type\":\"object\"}"}],
		"mcp_servers": [{"name": "demo_mcp", "command": "demo"}]
	}`), 0644); err != nil {
		t.Fatalf("write skill manifest: %v", err)
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: 16})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	server := httptest.NewServer(newAPIHandler(store, cfg))
	defer server.Close()

	resp, err := http.Get(server.URL + "/api/v1/skills")
	if err != nil {
		t.Fatalf("get skills: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode skills: %v", err)
	}
	if len(payload["skills"].([]any)) != 1 || len(payload["tools"].([]any)) != 1 || len(payload["mcp_servers"].([]any)) != 1 {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestSkillsListCommandJSON(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "demo.skill.json"), []byte(`{
		"name": "demo",
		"tools": [{"name": "demo_tool", "parameters_schema": "{\"type\":\"object\"}"}],
		"mcp_servers": [{"name": "demo_mcp", "command": "demo"}]
	}`), 0644); err != nil {
		t.Fatalf("write skill manifest: %v", err)
	}
	configPath := filepath.Join(t.TempDir(), "tenet.yaml")
	if err := os.WriteFile(configPath, []byte("skills:\n  skills_path: "+strconv.Quote(root)+"\n  skills_auto_discover: true\n"), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = writer
	runErr := skillsCmd([]string{"list", "--config", configPath, "--output", "json"})
	_ = writer.Close()
	os.Stdout = oldStdout
	if runErr != nil {
		t.Fatalf("skillsCmd: %v", runErr)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("decode stdout %q: %v", string(data), err)
	}
	if len(payload["skills"].([]any)) != 1 || len(payload["tools"].([]any)) != 1 || len(payload["mcp_servers"].([]any)) != 1 {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestHTTPAPIOpenAPIContract(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "tenet.db")
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: 16})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	server := httptest.NewServer(newAPIHandler(store, cfg))
	defer server.Close()

	resp, err := http.Get(server.URL + "/api/v1/openapi.json")
	if err != nil {
		t.Fatalf("get openapi: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var spec map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
		t.Fatalf("decode openapi: %v", err)
	}
	if spec["openapi"] != "3.0.3" {
		t.Fatalf("spec = %+v", spec)
	}
	paths, _ := spec["paths"].(map[string]any)
	for _, want := range []string{"/tasks", "/tasks/{task_id}/messages", "/workspace/snapshot", "/events", "/skills"} {
		if _, ok := paths[want]; !ok {
			t.Fatalf("missing path %s in %+v", want, paths)
		}
	}
}

func TestDueTimerScannerResumesDueTimer(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "tenet.db")
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: 16})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Now().UTC()
	if _, err := store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  "task:scanner",
		EventType: "TimerScheduled",
		Payload: map[string]any{
			"timer_id": "resume:scanner",
			"due_at":   now.Add(-time.Second).Format(time.RFC3339Nano),
			"note":     "scanner wake",
		},
	}); err != nil {
		t.Fatalf("append timer: %v", err)
	}
	stop := startDueTimerScanner(ctx, store, 10*time.Millisecond)
	defer stop()
	deadline := time.After(time.Second)
	for {
		events, err := store.Read("task:scanner", 1)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		seen := map[string]bool{}
		for _, event := range events {
			seen[event.EventType] = true
		}
		if seen["TimerFired"] && seen["TaskResumed"] {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timer scanner did not resume; events=%+v", events)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestConversationFromEventsInjectsSummaryMemory(t *testing.T) {
	events := []storage.Event{
		{EventType: "TaskCreated", Payload: `{"query":"first"}`},
		{EventType: "TaskCompleted", Payload: `{"final_answer":"answer"}`},
		{EventType: "SessionSummaryCreated", Payload: `{"summary":"session summary"}`},
		{EventType: "WorkspaceSummaryCreated", Payload: `{"summary":"workspace summary"}`},
	}
	messages := conversationFromEvents(events)
	if len(messages) < 4 {
		t.Fatalf("messages = %+v", messages)
	}
	if messages[0].Role != "system" || !strings.Contains(messages[0].Content, "session summary") {
		t.Fatalf("first message = %+v", messages[0])
	}
	if messages[1].Role != "system" || !strings.Contains(messages[1].Content, "workspace summary") {
		t.Fatalf("second message = %+v", messages[1])
	}
}

func postJSON(t *testing.T, target string, payload any) *http.Response {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	resp, err := http.Post(target, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("post %s: %v", target, err)
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		var body bytes.Buffer
		_, _ = body.ReadFrom(resp.Body)
		t.Fatalf("post %s status=%d body=%s", target, resp.StatusCode, body.String())
	}
	return resp
}
