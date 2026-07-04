package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/eventrouter"
	"github.com/tenet/orchestrator/internal/projection"
	"github.com/tenet/orchestrator/internal/skills"
	"github.com/tenet/orchestrator/internal/storage"
	timerpkg "github.com/tenet/orchestrator/internal/timer"
	"github.com/tenet/orchestrator/internal/worker"
	"github.com/tenet/orchestrator/internal/workflow"
	workspacepkg "github.com/tenet/orchestrator/internal/workspace"
)

type createTaskRequest struct {
	Query         string `json:"query"`
	Message       string `json:"message"`
	Workspace     string `json:"workspace"`
	Workflow      string `json:"workflow"`
	Worker        string `json:"worker"`
	WorkerAddress string `json:"worker_address"`
	Model         string `json:"model"`
	BaseURL       string `json:"base_url"`
	APIKeyEnv     string `json:"api_key_env"`
	APIKey        string `json:"api_key"`
	MaxSteps      int    `json:"max_steps"`
	Scheduled     bool   `json:"scheduled"`
}

type taskActionRequest struct {
	Reason           string `json:"reason"`
	Note             string `json:"note"`
	After            string `json:"after"`
	Query            string `json:"query"`
	Seq              int64  `json:"seq"`
	RestoreWorkspace *bool  `json:"restore_workspace"`
}

type workspaceSnapshotRequest struct {
	Path      string `json:"path"`
	SessionID string `json:"session_id"`
	StreamID  string `json:"stream_id"`
	Seq       int64  `json:"seq"`
}

type workspaceRestoreRequest struct {
	Archive string `json:"archive"`
	Dest    string `json:"dest"`
}

func newAPIHandler(store storage.Store, cfg *config.RuntimeConfig) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/", func(w http.ResponseWriter, r *http.Request) {
		r2 := r.Clone(r.Context())
		r2.URL.Path = strings.TrimPrefix(r.URL.Path, "/api/v1")
		if r2.URL.Path == "" {
			r2.URL.Path = "/"
		}
		mux.ServeHTTP(w, r2)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "version": version})
	})
	mux.HandleFunc("/openapi.json", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, openAPISpec())
	})
	mux.HandleFunc("/config", func(w http.ResponseWriter, _ *http.Request) {
		out := *cfg
		maskSecrets(&out)
		writeJSON(w, http.StatusOK, out)
	})
	mux.HandleFunc("/workers", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, []map[string]any{{
			"id":       "local",
			"status":   "available",
			"modes":    []string{"echo", "openai", "deepseek", "grpc"},
			"grpcPort": cfg.GRPC.WorkerPort,
		}})
	})
	mux.HandleFunc("/skills", func(w http.ResponseWriter, _ *http.Request) {
		registry, err := skills.Discover(cfg)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "SKILL_DISCOVERY_FAILED", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"skills":      registry.Skills,
			"tools":       registry.ToolDefinitions(),
			"mcp_servers": registry.MCPServers(),
		})
	})
	mux.HandleFunc("/tasks", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleHTTPTaskList(w, r, store, cfg)
		case http.MethodPost:
			handleHTTPTaskCreate(w, r, store, cfg)
		default:
			writeAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	})
	mux.HandleFunc("/tasks/", func(w http.ResponseWriter, r *http.Request) {
		handleHTTPTaskRoute(w, r, store, cfg)
	})
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		handleHTTPSSEEvents(w, r, store)
	})
	mux.HandleFunc("/workspace/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		var req workspaceSnapshotRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		if req.Seq <= 0 {
			req.Seq = 1
		}
		if req.Path == "" || req.SessionID == "" {
			writeAPIError(w, http.StatusBadRequest, "BAD_REQUEST", "path and session_id are required")
			return
		}
		manager := workspacepkg.NewManager(cfg)
		if req.StreamID != "" {
			result, err := manager.CaptureSnapshot(r.Context(), store, req.StreamID, req.Path, req.SessionID, req.Seq, map[string]any{"session_id": req.SessionID}, nil, zeroFencingLease())
			if err != nil {
				writeError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, result)
			return
		}
		snapshot, err := manager.Snapshot(r.Context(), req.Path, req.SessionID, req.Seq, nil, zeroFencingLease())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, snapshot)
	})
	mux.HandleFunc("/workspace/restore", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		var req workspaceRestoreRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		if req.Archive == "" || req.Dest == "" {
			writeAPIError(w, http.StatusBadRequest, "BAD_REQUEST", "archive and dest are required")
			return
		}
		err := workspacepkg.NewManager(cfg).Restore(r.Context(), workspacepkg.Snapshot{Type: "archive", Ref: req.Archive}, req.Dest, nil, zeroFencingLease())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"restored": true, "dest": req.Dest})
	})
	return mux
}

func handleHTTPTaskList(w http.ResponseWriter, r *http.Request, store storage.Store, cfg *config.RuntimeConfig) {
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	streams, err := store.ListStreams(limit)
	if err != nil {
		writeError(w, err)
		return
	}
	engine := projection.NewEngine(store, cfg)
	items := make([]map[string]any, 0, len(streams))
	for _, stream := range streams {
		view, _ := engine.ProjectTask(stream.StreamID)
		items = append(items, map[string]any{
			"stream_id":  stream.StreamID,
			"status":     view.Status,
			"workflow":   view.WorkflowType,
			"latest_seq": stream.LatestSeq,
			"last_event": stream.EventType,
			"query":      view.Query,
			"tokens":     view.Tokens.TotalTokens,
			"phase":      view.CurrentPhase,
			"updated_at": stream.Timestamp,
		})
	}
	writeJSON(w, http.StatusOK, items)
}

func handleHTTPTaskCreate(w http.ResponseWriter, r *http.Request, store storage.Store, cfg *config.RuntimeConfig) {
	var req createTaskRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Query) == "" {
		writeAPIError(w, http.StatusBadRequest, "BAD_REQUEST", "query is required")
		return
	}
	result, err := runHTTPTask(r.Context(), store, cfg, req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func runHTTPTask(ctx context.Context, store storage.Store, cfg *config.RuntimeConfig, req createTaskRequest) (map[string]any, error) {
	runCfg := *cfg
	if req.MaxSteps > 0 {
		runCfg.Agent.DefaultMaxSteps = req.MaxSteps
	}
	workspacePath := strings.TrimSpace(req.Workspace)
	if workspacePath == "" {
		workspacePath = "."
	}
	workspaceAbs, _ := pathAbs(workspacePath)
	workerMode := firstNonEmpty(req.Worker, "echo")
	route := workflow.Route(req.Query, firstNonEmpty(req.Workflow, "auto"))
	streamID := "task:" + newID()
	turnID := "turn:" + newID()
	runID := "run:" + newID()
	client, taskModel, err := buildTaskClientWithKey(workerMode, req.Model, req.BaseURL, req.APIKeyEnv, req.APIKey, req.WorkerAddress, workspaceAbs, &runCfg)
	if err != nil {
		return nil, err
	}
	if closer, ok := client.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	if _, err := store.AppendEvents(ctx, []storage.AppendEvent{
		{StreamID: streamID, EventType: "SessionCreated", Payload: map[string]any{
			"session_id":    streamID,
			"query":         req.Query,
			"workspace":     workspaceAbs,
			"workflow_type": route.Workflow,
		}},
		{StreamID: streamID, EventType: "TurnCreated", Payload: map[string]any{
			"session_id": streamID,
			"turn_id":    turnID,
			"query":      req.Query,
		}},
		{StreamID: streamID, EventType: "TaskCreated", Payload: map[string]any{
			"session_id":    streamID,
			"turn_id":       turnID,
			"run_id":        runID,
			"query":         req.Query,
			"workspace":     workspaceAbs,
			"workflow_type": route.Workflow,
		}},
		{StreamID: streamID, EventType: "ComplexityAnalyzed", Payload: map[string]any{
			"complexity_score":  route.ComplexityScore,
			"reason":            route.Reason,
			"selected_workflow": route.Workflow,
			"task_type":         route.TaskType,
			"required_tools":    route.RequiredTools,
			"risk_level":        route.RiskLevel,
		}},
	}); err != nil {
		return nil, err
	}
	task := &workflow.TaskHandle{
		StreamID:     streamID,
		Mode:         workflow.ContextModeExecution,
		WorkflowType: route.Workflow,
		SessionID:    streamID,
		TurnID:       turnID,
		RunID:        runID,
		Query:        req.Query,
		Workspace:    workspaceAbs,
		SystemPrompt: defaultAgentSystemPrompt(workspaceAbs),
		Model:        taskModel,
		AgentRole:    "default",
		Tools:        worker.BuiltinToolDefinitionsWithAllowlist(runCfg.Safety.ToolAllowlist),
		Config:       &runCfg,
		Client:       client,
	}
	taskCtx, cancel := context.WithTimeout(ctx, time.Duration(runCfg.GRPC.ExecuteTimeoutSeconds)*time.Second)
	defer cancel()
	var taskResult *workflow.TaskResult
	var runErr error
	if req.Scheduled {
		taskResult, runErr = executeScheduled(taskCtx, store, &runCfg, task)
	} else {
		taskResult, runErr = workflow.Execute(taskCtx, store, workflow.NewRegistry(), task)
	}
	if runErr != nil {
		if errors.Is(runErr, workflow.ErrWorkflowSuspended) {
			return map[string]any{
				"task_id":          streamID,
				"status":           "PAUSED",
				"workflow":         task.WorkflowType,
				"complexity_score": route.ComplexityScore,
				"result":           nil,
				"error":            runErr.Error(),
			}, nil
		}
		return nil, runErr
	}
	return map[string]any{
		"task_id":          streamID,
		"status":           "COMPLETED",
		"workflow":         taskResult.Workflow,
		"complexity_score": route.ComplexityScore,
		"result":           taskResult.Result,
	}, nil
}

func handleHTTPTaskRoute(w http.ResponseWriter, r *http.Request, store storage.Store, cfg *config.RuntimeConfig) {
	streamID, action, ok := parseTaskPath(r.URL.Path)
	if !ok {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "task route not found")
		return
	}
	switch {
	case action == "" && r.Method == http.MethodGet:
		view, err := projection.NewEngine(store, cfg).ProjectTask(streamID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, view)
	case action == "events" && r.Method == http.MethodGet:
		fromSeq := int64(1)
		if raw := r.URL.Query().Get("from"); raw != "" {
			if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
				fromSeq = parsed
			}
		}
		events, err := store.Read(streamID, fromSeq)
		if err != nil {
			writeError(w, err)
			return
		}
		out := make([]eventrouter.StreamEvent, 0, len(events))
		for _, event := range events {
			out = append(out, eventrouter.FromStorageEvent(event))
		}
		writeJSON(w, http.StatusOK, out)
	case action == "cancel" && r.Method == http.MethodPost:
		var req taskActionRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		event, err := store.AppendEvent(r.Context(), storage.AppendEvent{StreamID: streamID, EventType: "TaskCancelled", Payload: map[string]any{"reason": firstNonEmpty(req.Reason, "cancelled by user")}})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, eventrouter.FromStorageEvent(event))
	case action == "resume" && r.Method == http.MethodPost:
		handleHTTPTaskResume(w, r, store, streamID)
	case action == "messages" && r.Method == http.MethodPost:
		handleHTTPTaskMessage(w, r, store, cfg, streamID)
	case action == "fork" && r.Method == http.MethodPost:
		handleHTTPTaskFork(w, r, store, cfg, streamID)
	case action == "lineage" && r.Method == http.MethodGet:
		lineage, err := store.GetLineage(streamID)
		if err != nil {
			writeError(w, err)
			return
		}
		children, err := store.GetChildStreams(streamID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"stream_id": streamID, "lineage": lineage, "children": children})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func handleHTTPTaskMessage(w http.ResponseWriter, r *http.Request, store storage.Store, cfg *config.RuntimeConfig, streamID string) {
	var req createTaskRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	prompt := firstNonEmpty(req.Message, req.Query)
	if strings.TrimSpace(prompt) == "" {
		writeAPIError(w, http.StatusBadRequest, "BAD_REQUEST", "message is required")
		return
	}
	events, err := store.Read(streamID, 1)
	if err != nil {
		writeError(w, err)
		return
	}
	req.Query = prompt
	if req.Workspace == "" {
		req.Workspace = workspaceFromEvents(events)
	}
	if req.Workflow == "" {
		req.Workflow = "auto"
	}
	if req.Worker == "" {
		req.Worker = "echo"
	}
	result, err := continueHTTPTask(r.Context(), store, cfg, streamID, req, events)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func continueHTTPTask(ctx context.Context, store storage.Store, cfg *config.RuntimeConfig, streamID string, req createTaskRequest, events []storage.Event) (map[string]any, error) {
	runCfg := *cfg
	if req.MaxSteps > 0 {
		runCfg.Agent.DefaultMaxSteps = req.MaxSteps
	}
	workspacePath := strings.TrimSpace(req.Workspace)
	if workspacePath == "" {
		workspacePath = "."
	}
	workspaceAbs, _ := pathAbs(workspacePath)
	workerMode := firstNonEmpty(req.Worker, "echo")
	route := workflow.Route(req.Query, firstNonEmpty(req.Workflow, "auto"))
	turnID := "turn:" + newID()
	runID := "run:" + newID()
	client, taskModel, err := buildTaskClientWithKey(workerMode, req.Model, req.BaseURL, req.APIKeyEnv, req.APIKey, req.WorkerAddress, workspaceAbs, &runCfg)
	if err != nil {
		return nil, err
	}
	if closer, ok := client.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	messages := conversationFromEvents(events)
	messages = append(messages, worker.Message{Role: "user", Content: req.Query})
	if _, err := store.AppendEvents(ctx, []storage.AppendEvent{
		{StreamID: streamID, EventType: "TurnCreated", Payload: map[string]any{
			"session_id": streamID,
			"turn_id":    turnID,
			"query":      req.Query,
		}},
		{StreamID: streamID, EventType: "UserMessage", Payload: map[string]any{
			"session_id": streamID,
			"turn_id":    turnID,
			"content":    req.Query,
		}},
		{StreamID: streamID, EventType: "ComplexityAnalyzed", Payload: map[string]any{
			"complexity_score":  route.ComplexityScore,
			"reason":            route.Reason,
			"selected_workflow": route.Workflow,
			"task_type":         route.TaskType,
			"required_tools":    route.RequiredTools,
			"risk_level":        route.RiskLevel,
		}},
	}); err != nil {
		return nil, err
	}
	task := &workflow.TaskHandle{
		StreamID:     streamID,
		Mode:         workflow.ContextModeExecution,
		WorkflowType: route.Workflow,
		SessionID:    streamID,
		TurnID:       turnID,
		RunID:        runID,
		Query:        req.Query,
		Workspace:    workspaceAbs,
		SystemPrompt: defaultAgentSystemPrompt(workspaceAbs),
		Messages:     messages,
		Model:        taskModel,
		AgentRole:    "default",
		Tools:        worker.BuiltinToolDefinitionsWithAllowlist(runCfg.Safety.ToolAllowlist),
		Config:       &runCfg,
		Client:       client,
	}
	taskCtx, cancel := context.WithTimeout(ctx, time.Duration(runCfg.GRPC.ExecuteTimeoutSeconds)*time.Second)
	defer cancel()
	taskResult, err := workflow.Execute(taskCtx, store, workflow.NewRegistry(), task)
	if err != nil {
		if errors.Is(err, workflow.ErrWorkflowSuspended) {
			return map[string]any{
				"task_id":          streamID,
				"status":           "PAUSED",
				"workflow":         task.WorkflowType,
				"complexity_score": route.ComplexityScore,
				"result":           nil,
				"error":            err.Error(),
			}, nil
		}
		return nil, err
	}
	return map[string]any{
		"task_id":          streamID,
		"status":           "COMPLETED",
		"workflow":         taskResult.Workflow,
		"complexity_score": route.ComplexityScore,
		"result":           taskResult.Result,
	}, nil
}

func handleHTTPTaskResume(w http.ResponseWriter, r *http.Request, store storage.Store, streamID string) {
	var req taskActionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	note := firstNonEmpty(req.Note, "resume requested")
	after := time.Duration(0)
	if req.After != "" {
		parsed, err := time.ParseDuration(req.After)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid after duration")
			return
		}
		after = parsed
	}
	if after <= 0 {
		event, err := store.AppendEvent(r.Context(), storage.AppendEvent{StreamID: streamID, EventType: "TaskResumed", Payload: map[string]any{"note": note}})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, eventrouter.FromStorageEvent(event))
		return
	}
	service := timerpkg.NewService(store)
	timerID := fmt.Sprintf("resume:%d", time.Now().UTC().UnixNano())
	done, err := service.Schedule(r.Context(), timerpkg.ScheduleRequest{
		StreamID:           streamID,
		TimerID:            timerID,
		Delay:              after,
		ScheduledEventType: "TaskResumeScheduled",
		FiredEventType:     "TimerFired",
		Payload:            map[string]any{"note": note},
	})
	if err != nil {
		writeError(w, err)
		return
	}
	result := <-done
	if result.Err != nil {
		writeError(w, result.Err)
		return
	}
	event, err := store.AppendEvent(r.Context(), storage.AppendEvent{StreamID: streamID, EventType: "TaskResumed", Payload: map[string]any{"note": note, "timer_id": timerID}})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"scheduled": eventrouter.FromStorageEvent(result.Scheduled),
		"fired":     eventrouter.FromStorageEvent(result.Fired),
		"resumed":   eventrouter.FromStorageEvent(event),
	})
}

func handleHTTPTaskFork(w http.ResponseWriter, r *http.Request, store storage.Store, cfg *config.RuntimeConfig, streamID string) {
	var req taskActionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Seq <= 0 {
		writeAPIError(w, http.StatusBadRequest, "BAD_REQUEST", "seq must be positive")
		return
	}
	restoreWorkspace := true
	if req.RestoreWorkspace != nil {
		restoreWorkspace = *req.RestoreWorkspace
	}
	if restoreWorkspace {
		result, err := workspacepkg.NewManager(cfg).ForkWorkspace(r.Context(), store, streamID, req.Seq, req.Query, nil, zeroFencingLease())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	childID, err := store.ForkStream(r.Context(), streamID, req.Seq, req.Query)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"stream_id": childID, "parent_id": streamID, "fork_at_seq": req.Seq})
}

func handleHTTPSSEEvents(w http.ResponseWriter, r *http.Request, store storage.Store) {
	streamID := r.URL.Query().Get("stream_id")
	if streamID == "" {
		writeAPIError(w, http.StatusBadRequest, "BAD_REQUEST", "stream_id is required")
		return
	}
	fromSeq := int64(1)
	if raw := r.URL.Query().Get("from"); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			fromSeq = parsed
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher, _ := w.(http.Flusher)
	nextSeq := fromSeq
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		events, err := store.Read(streamID, nextSeq)
		if err != nil {
			fmt.Fprintf(w, "event: error\ndata: %q\n\n", err.Error())
			if flusher != nil {
				flusher.Flush()
			}
			return
		}
		for _, evt := range events {
			data, _ := json.Marshal(eventrouter.FromStorageEvent(evt))
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.EventType, data)
			nextSeq = evt.StreamSeq + 1
		}
		if flusher != nil {
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func parseTaskPath(raw string) (string, string, bool) {
	clean := strings.TrimPrefix(path.Clean(raw), "/tasks/")
	if clean == "." || clean == "" {
		return "", "", false
	}
	parts := strings.Split(clean, "/")
	streamID, err := url.PathUnescape(parts[0])
	if err != nil || streamID == "" {
		return "", "", false
	}
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}
	return streamID, action, true
}

func conversationFromEvents(events []storage.Event) []worker.Message {
	messages := make([]worker.Message, 0)
	sessionSummary := ""
	workspaceSummary := ""
	for _, event := range events {
		payload := map[string]any{}
		_ = json.Unmarshal([]byte(event.Payload), &payload)
		switch event.EventType {
		case "TaskCreated":
			if content := stringFromPayload(payload, "query"); content != "" {
				messages = append(messages, worker.Message{Role: "user", Content: content})
			}
		case "UserMessage":
			if content := stringFromPayload(payload, "content"); content != "" {
				messages = append(messages, worker.Message{Role: "user", Content: content})
			}
		case "TaskCompleted":
			if content := firstNonEmpty(stringFromPayload(payload, "final_answer"), stringFromPayload(payload, "result")); content != "" {
				messages = append(messages, worker.Message{Role: "assistant", Content: content})
			}
		case "SessionSummaryCreated":
			sessionSummary = stringFromPayload(payload, "summary")
		case "WorkspaceSummaryCreated":
			workspaceSummary = stringFromPayload(payload, "summary")
		}
	}
	if len(messages) > 24 {
		messages = messages[len(messages)-24:]
	}
	memory := memoryMessages(sessionSummary, workspaceSummary)
	if len(memory) > 0 {
		return append(memory, messages...)
	}
	return messages
}

func memoryMessages(sessionSummary, workspaceSummary string) []worker.Message {
	messages := []worker.Message{}
	if strings.TrimSpace(sessionSummary) != "" {
		messages = append(messages, worker.Message{Role: "system", Content: "Session memory:\n" + strings.TrimSpace(sessionSummary)})
	}
	if strings.TrimSpace(workspaceSummary) != "" {
		messages = append(messages, worker.Message{Role: "system", Content: "Workspace memory:\n" + strings.TrimSpace(workspaceSummary)})
	}
	return messages
}

func workspaceFromEvents(events []storage.Event) string {
	for _, event := range events {
		if event.EventType != "TaskCreated" && event.EventType != "TaskStarted" {
			continue
		}
		payload := map[string]any{}
		_ = json.Unmarshal([]byte(event.Payload), &payload)
		if workspace := stringFromPayload(payload, "workspace"); workspace != "" {
			return workspace
		}
	}
	return "."
}

func stringFromPayload(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		data, _ := json.Marshal(typed)
		return string(data)
	}
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_JSON", "invalid json: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func openAPISpec() map[string]any {
	errorResponse := map[string]any{
		"description": "Structured API error",
		"content": map[string]any{"application/json": map[string]any{"schema": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"data": map[string]any{"nullable": true},
				"error": map[string]any{"type": "object", "properties": map[string]any{
					"code":    map[string]any{"type": "string"},
					"message": map[string]any{"type": "string"},
				}},
			},
		}}},
	}
	jsonOK := func(description string) map[string]any {
		return map[string]any{"description": description, "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"type": "object"}}}}
	}
	jsonArrayOK := func(description string) map[string]any {
		return map[string]any{"description": description, "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"type": "array"}}}}
	}
	return map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":   "Tenet API",
			"version": version,
		},
		"servers": []map[string]any{{"url": "/api/v1"}},
		"paths": map[string]any{
			"/healthz": map[string]any{"get": map[string]any{"responses": map[string]any{"200": jsonOK("Health status")}}},
			"/config":  map[string]any{"get": map[string]any{"responses": map[string]any{"200": jsonOK("Runtime configuration with secrets masked")}}},
			"/workers": map[string]any{"get": map[string]any{"responses": map[string]any{"200": jsonArrayOK("Available workers")}}},
			"/skills":  map[string]any{"get": map[string]any{"responses": map[string]any{"200": jsonOK("Discovered skills, MCP servers, and skill tool definitions"), "500": errorResponse}}},
			"/tasks": map[string]any{
				"get": map[string]any{"parameters": []map[string]any{{"name": "limit", "in": "query", "schema": map[string]any{"type": "integer"}}}, "responses": map[string]any{"200": jsonArrayOK("Task list"), "500": errorResponse}},
				"post": map[string]any{
					"requestBody": map[string]any{"required": true, "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/CreateTaskRequest"}}}},
					"responses":   map[string]any{"200": jsonOK("Created task result"), "400": errorResponse, "500": errorResponse},
				},
			},
			"/tasks/{task_id}":          map[string]any{"get": map[string]any{"parameters": pathTaskIDParam(), "responses": map[string]any{"200": jsonOK("Projected task state"), "404": errorResponse, "500": errorResponse}}},
			"/tasks/{task_id}/events":   map[string]any{"get": map[string]any{"parameters": append(pathTaskIDParam(), map[string]any{"name": "from", "in": "query", "schema": map[string]any{"type": "integer"}}), "responses": map[string]any{"200": jsonArrayOK("Event stream slice"), "500": errorResponse}}},
			"/tasks/{task_id}/messages": map[string]any{"post": map[string]any{"parameters": pathTaskIDParam(), "requestBody": jsonRequestRef("#/components/schemas/CreateTaskRequest"), "responses": map[string]any{"200": jsonOK("Continuation result"), "400": errorResponse, "500": errorResponse}}},
			"/tasks/{task_id}/resume":   map[string]any{"post": map[string]any{"parameters": pathTaskIDParam(), "requestBody": jsonRequestRef("#/components/schemas/TaskActionRequest"), "responses": map[string]any{"200": jsonOK("Resume event or timer result"), "400": errorResponse, "500": errorResponse}}},
			"/tasks/{task_id}/cancel":   map[string]any{"post": map[string]any{"parameters": pathTaskIDParam(), "requestBody": jsonRequestRef("#/components/schemas/TaskActionRequest"), "responses": map[string]any{"200": jsonOK("Cancel event"), "500": errorResponse}}},
			"/tasks/{task_id}/fork":     map[string]any{"post": map[string]any{"parameters": pathTaskIDParam(), "requestBody": jsonRequestRef("#/components/schemas/TaskActionRequest"), "responses": map[string]any{"200": jsonOK("Fork result"), "400": errorResponse, "500": errorResponse}}},
			"/tasks/{task_id}/lineage":  map[string]any{"get": map[string]any{"parameters": pathTaskIDParam(), "responses": map[string]any{"200": jsonOK("Lineage result"), "500": errorResponse}}},
			"/events":                   map[string]any{"get": map[string]any{"parameters": []map[string]any{{"name": "stream_id", "in": "query", "required": true, "schema": map[string]any{"type": "string"}}, {"name": "from", "in": "query", "schema": map[string]any{"type": "integer"}}}, "responses": map[string]any{"200": map[string]any{"description": "Server-sent events"}, "400": errorResponse}}},
			"/workspace/snapshot":       map[string]any{"post": map[string]any{"requestBody": jsonRequestRef("#/components/schemas/WorkspaceSnapshotRequest"), "responses": map[string]any{"200": jsonOK("Workspace snapshot"), "400": errorResponse, "500": errorResponse}}},
			"/workspace/restore":        map[string]any{"post": map[string]any{"requestBody": jsonRequestRef("#/components/schemas/WorkspaceRestoreRequest"), "responses": map[string]any{"200": jsonOK("Workspace restore result"), "400": errorResponse, "500": errorResponse}}},
			"/openapi.json":             map[string]any{"get": map[string]any{"responses": map[string]any{"200": jsonOK("OpenAPI document")}}},
		},
		"components": map[string]any{"schemas": map[string]any{
			"CreateTaskRequest": map[string]any{"type": "object", "properties": map[string]any{
				"query":          map[string]any{"type": "string"},
				"message":        map[string]any{"type": "string"},
				"workspace":      map[string]any{"type": "string"},
				"workflow":       map[string]any{"type": "string"},
				"worker":         map[string]any{"type": "string", "enum": []string{"echo", "openai", "deepseek", "grpc"}},
				"worker_address": map[string]any{"type": "string"},
				"model":          map[string]any{"type": "string"},
				"base_url":       map[string]any{"type": "string"},
				"api_key_env":    map[string]any{"type": "string"},
				"api_key":        map[string]any{"type": "string", "writeOnly": true},
				"max_steps":      map[string]any{"type": "integer"},
				"scheduled":      map[string]any{"type": "boolean"},
			}},
			"TaskActionRequest": map[string]any{"type": "object", "properties": map[string]any{
				"reason":            map[string]any{"type": "string"},
				"note":              map[string]any{"type": "string"},
				"after":             map[string]any{"type": "string"},
				"query":             map[string]any{"type": "string"},
				"seq":               map[string]any{"type": "integer"},
				"restore_workspace": map[string]any{"type": "boolean"},
			}},
			"WorkspaceSnapshotRequest": map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}, "session_id": map[string]any{"type": "string"}, "stream_id": map[string]any{"type": "string"}, "seq": map[string]any{"type": "integer"}}},
			"WorkspaceRestoreRequest":  map[string]any{"type": "object", "properties": map[string]any{"archive": map[string]any{"type": "string"}, "dest": map[string]any{"type": "string"}}},
		}},
	}
}

func pathTaskIDParam() []map[string]any {
	return []map[string]any{{"name": "task_id", "in": "path", "required": true, "schema": map[string]any{"type": "string"}}}
}

func jsonRequestRef(ref string) map[string]any {
	return map[string]any{"required": true, "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": ref}}}}
}

func writeAPIError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"data": nil,
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
}

func writeError(w http.ResponseWriter, err error) {
	writeAPIError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
}

func pathAbs(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		value = "."
	}
	return filepath.Abs(value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
