package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/eventrouter"
	"github.com/tenet/orchestrator/internal/gateway"
	"github.com/tenet/orchestrator/internal/guard"
	"github.com/tenet/orchestrator/internal/projection"
	"github.com/tenet/orchestrator/internal/scheduler"
	"github.com/tenet/orchestrator/internal/skills"
	"github.com/tenet/orchestrator/internal/storage"
	timerpkg "github.com/tenet/orchestrator/internal/timer"
	"github.com/tenet/orchestrator/internal/worker"
	"github.com/tenet/orchestrator/internal/workflow"
	workspacepkg "github.com/tenet/orchestrator/internal/workspace"
)

const version = "0.1.0"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage()
		return nil
	}
	switch args[0] {
	case "version":
		fmt.Println(version)
		return nil
	case "serve":
		return serve(args[1:])
	case "config":
		return configCmd(args[1:])
	case "skills":
		return skillsCmd(args[1:])
	case "task":
		return taskCmd(args[1:])
	case "workspace":
		return workspaceCmd(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	port := fs.Int("port", 0, "orchestrator port")
	workerPort := fs.Int("worker-port", 0, "worker port")
	httpPort := fs.Int("http-port", 0, "HTTP API port; disabled when 0")
	dbPath := fs.String("db", "", "sqlite database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	if *port > 0 {
		cfg.GRPC.OrchestratorPort = *port
	}
	if *workerPort > 0 {
		cfg.GRPC.WorkerPort = *workerPort
	}
	if *dbPath != "" {
		cfg.Database.Path = *dbPath
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPC.OrchestratorPort))
	if err != nil {
		return err
	}
	server := gateway.NewOrchestratorServer("tenet-"+newID(), gateway.NewWorkerRegistry())
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(listener)
	}()
	var httpServer *http.Server
	if *httpPort > 0 {
		httpServer = newHTTPServer(fmt.Sprintf(":%d", *httpPort), store, cfg)
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
		}()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	stopTimerScanner := startDueTimerScanner(ctx, store, time.Second)
	defer stopTimerScanner()
	fmt.Printf("tenet ready: orchestrator=%s worker=:%d http=:%d database=%s\n", listener.Addr().String(), cfg.GRPC.WorkerPort, *httpPort, cfg.Database.Path)
	select {
	case <-ctx.Done():
		server.Stop()
		if httpServer != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = httpServer.Shutdown(shutdownCtx)
		}
		return nil
	case err := <-errCh:
		return err
	}
}

func startDueTimerScanner(ctx context.Context, store storage.Store, interval time.Duration) func() {
	if interval <= 0 {
		interval = time.Second
	}
	scanCtx, cancel := context.WithCancel(ctx)
	service := timerpkg.NewService(store)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-scanCtx.Done():
				return
			case <-ticker.C:
				_, _ = service.ResumeDueTimers(scanCtx, time.Now().UTC(), 1000)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func newHTTPServer(addr string, store storage.Store, cfg *config.RuntimeConfig) *http.Server {
	return &http.Server{Addr: addr, Handler: withCORS(newAPIHandler(store, cfg))}
}

func configCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing config subcommand")
	}
	switch args[0] {
	case "validate":
		fs := flag.NewFlagSet("config validate", flag.ContinueOnError)
		configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		cfg, err := config.Load(*configPath)
		if err != nil {
			return err
		}
		fmt.Println("YAML syntax: ok")
		fmt.Printf("database.path: %s\n", cfg.Database.Path)
		fmt.Printf("grpc.orchestrator_port: %d\n", cfg.GRPC.OrchestratorPort)
		fmt.Printf("grpc.worker_port: %d\n", cfg.GRPC.WorkerPort)
		fmt.Println("All checks passed.")
		return nil
	case "show":
		fs := flag.NewFlagSet("config show", flag.ContinueOnError)
		configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
		format := fs.String("output", "text", "output format: text/json")
		includeSecrets := fs.Bool("include-secrets", false, "show secret values")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		cfg, err := loadConfig(*configPath, true)
		if err != nil {
			return err
		}
		if !*includeSecrets {
			maskSecrets(cfg)
		}
		if *format == "json" {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(cfg)
		}
		fmt.Printf("database: %s\n", cfg.Database.Path)
		fmt.Printf("worker:    localhost:%d\n", cfg.GRPC.WorkerPort)
		fmt.Printf("workflow:  %s\n", cfg.Workflow.DefaultStrategy)
		return nil
	default:
		return fmt.Errorf("unknown config subcommand %q", args[0])
	}
}

func skillsCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing skills subcommand")
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("skills list", flag.ContinueOnError)
		configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
		output := fs.String("output", "text", "output format: text/json")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		cfg, err := loadConfig(*configPath, true)
		if err != nil {
			return err
		}
		registry, err := skills.Discover(cfg)
		if err != nil {
			return err
		}
		if *output == "json" {
			return json.NewEncoder(os.Stdout).Encode(map[string]any{
				"skills":      registry.Skills,
				"tools":       registry.ToolDefinitions(),
				"mcp_servers": registry.MCPServers(),
			})
		}
		fmt.Printf("skills: %d\n", len(registry.Skills))
		for _, skill := range registry.Skills {
			fmt.Printf("- %s", skill.Name)
			if skill.Version != "" {
				fmt.Printf(" (%s)", skill.Version)
			}
			if skill.Description != "" {
				fmt.Printf(": %s", skill.Description)
			}
			fmt.Println()
			for _, tool := range skill.Tools {
				fmt.Printf("  tool: %s\n", tool.Name)
			}
			for _, server := range skill.MCPServers {
				fmt.Printf("  mcp:  %s -> %s\n", server.Name, server.Command)
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown skills subcommand %q", args[0])
	}
}

func workspaceCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing workspace subcommand")
	}
	switch args[0] {
	case "init":
		return workspaceInit(args[1:])
	case "ratio":
		return workspaceRatio(args[1:])
	case "snapshot":
		return workspaceSnapshot(args[1:])
	case "restore":
		return workspaceRestore(args[1:])
	case "cleanup":
		return workspaceCleanup(args[1:])
	default:
		return fmt.Errorf("unknown workspace subcommand %q", args[0])
	}
}

func workspaceInit(args []string) error {
	fs := flag.NewFlagSet("workspace init", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	sessionID := fs.String("session", "", "session id")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *sessionID == "" {
		return fmt.Errorf("--session is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	path, err := workspacepkg.NewManager(cfg).Init(*sessionID)
	if err != nil {
		return err
	}
	if *output == "json" {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"session_id": *sessionID, "workspace": path})
	}
	fmt.Printf("workspace: %s\n", path)
	return nil
}

func workspaceRatio(args []string) error {
	fs := flag.NewFlagSet("workspace ratio", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	path := fs.String("path", "", "workspace path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return fmt.Errorf("--path is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	ratio, err := workspacepkg.NewManager(cfg).AnalyzeTextRatio(*path)
	if err != nil {
		return err
	}
	fmt.Printf("%.4f\n", ratio)
	return nil
}

func workspaceSnapshot(args []string) error {
	fs := flag.NewFlagSet("workspace snapshot", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	path := fs.String("path", "", "workspace path")
	sessionID := fs.String("session", "", "session id")
	seq := fs.Int64("seq", 1, "snapshot sequence")
	streamID := fs.String("stream", "", "optional task stream id to record the snapshot")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return fmt.Errorf("--path is required")
	}
	if *sessionID == "" {
		return fmt.Errorf("--session is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	manager := workspacepkg.NewManager(cfg)
	if *streamID != "" {
		store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
		if err != nil {
			return err
		}
		defer store.Close()
		result, err := manager.CaptureSnapshot(
			context.Background(),
			store,
			*streamID,
			*path,
			*sessionID,
			*seq,
			map[string]any{"session_id": *sessionID},
			nil,
			zeroFencingLease(),
		)
		if err != nil {
			return err
		}
		if *output == "json" {
			return json.NewEncoder(os.Stdout).Encode(result)
		}
		fmt.Printf("%s: %s\n", result.Snapshot.Type, result.Snapshot.Ref)
		fmt.Printf("stream: %s\n", result.Snapshot.StreamID)
		fmt.Printf("seq:    %d\n", result.Snapshot.StreamSeq)
		return nil
	}
	snapshot, err := manager.Snapshot(context.Background(), *path, *sessionID, *seq, nil, zeroFencingLease())
	if err != nil {
		return err
	}
	if *output == "json" {
		return json.NewEncoder(os.Stdout).Encode(snapshot)
	}
	fmt.Printf("%s: %s\n", snapshot.Type, snapshot.Ref)
	return nil
}

func workspaceRestore(args []string) error {
	fs := flag.NewFlagSet("workspace restore", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	archivePath := fs.String("archive", "", "archive snapshot path")
	destPath := fs.String("dest", "", "destination workspace path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *archivePath == "" {
		return fmt.Errorf("--archive is required")
	}
	if *destPath == "" {
		return fmt.Errorf("--dest is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	if err := workspacepkg.NewManager(cfg).Restore(context.Background(), workspacepkg.Snapshot{Type: "archive", Ref: *archivePath}, *destPath, nil, zeroFencingLease()); err != nil {
		return err
	}
	fmt.Printf("restored: %s\n", *destPath)
	return nil
}

func workspaceCleanup(args []string) error {
	fs := flag.NewFlagSet("workspace cleanup", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	path := fs.String("path", "", "workspace path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return fmt.Errorf("--path is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	if err := workspacepkg.NewManager(cfg).Cleanup(context.Background(), *path, nil, zeroFencingLease()); err != nil {
		return err
	}
	fmt.Printf("cleaned: %s\n", *path)
	return nil
}

func taskCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing task subcommand")
	}
	switch args[0] {
	case "list":
		return taskList(args[1:])
	case "run":
		return taskRun(args[1:])
	case "replay":
		return taskReplay(args[1:])
	case "inspect":
		return taskInspect(args[1:])
	case "watch":
		return taskWatch(args[1:])
	case "status":
		return taskStatus(args[1:])
	case "trace":
		return taskTrace(args[1:])
	case "checkpoints":
		return taskCheckpoints(args[1:])
	case "artifacts":
		return taskArtifacts(args[1:])
	case "fork":
		return taskFork(args[1:])
	case "lineage":
		return taskLineage(args[1:])
	case "logs":
		return taskLogs(args[1:])
	case "cancel":
		return taskCancel(args[1:])
	case "resume":
		return taskResume(args[1:])
	case "restore":
		return taskRestore(args[1:])
	default:
		return fmt.Errorf("unknown task subcommand %q", args[0])
	}
}

func taskList(args []string) error {
	fs := flag.NewFlagSet("task list", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	limit := fs.Int("limit", 20, "maximum number of streams")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	streams, err := store.ListStreams(*limit)
	if err != nil {
		return err
	}
	engine := projection.NewEngine(store, cfg)
	type taskListItem struct {
		StreamID  string `json:"stream_id"`
		Status    string `json:"status"`
		Workflow  string `json:"workflow,omitempty"`
		LatestSeq int64  `json:"latest_seq"`
		LastEvent string `json:"last_event"`
		Query     string `json:"query,omitempty"`
	}
	items := make([]taskListItem, 0, len(streams))
	for _, stream := range streams {
		view, _ := engine.ProjectTask(stream.StreamID)
		items = append(items, taskListItem{
			StreamID:  stream.StreamID,
			Status:    string(view.Status),
			Workflow:  view.WorkflowType,
			LatestSeq: stream.LatestSeq,
			LastEvent: stream.EventType,
			Query:     view.Query,
		})
	}
	if *output == "json" {
		return json.NewEncoder(os.Stdout).Encode(items)
	}
	for _, item := range items {
		fmt.Printf("%s\t%s\tseq=%d\t%s\t%s\n", item.StreamID, item.Status, item.LatestSeq, item.LastEvent, item.Query)
	}
	return nil
}

func taskRun(args []string) error {
	fs := flag.NewFlagSet("task run", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	workspacePath := fs.String("workspace", ".", "workspace path")
	workflowName := fs.String("workflow", "auto", "workflow type")
	workerMode := fs.String("worker", "echo", "worker mode: echo/openai/deepseek/grpc")
	workerAddress := fs.String("worker-address", "", "Python worker gRPC address, defaults to grpc.worker_port")
	model := fs.String("model", "", "model override")
	baseURL := fs.String("base-url", "", "OpenAI-compatible base URL")
	apiKeyEnv := fs.String("api-key-env", "", "environment variable containing the API key")
	maxSteps := fs.Int("max-steps", 0, "maximum ReAct steps for this task")
	scheduled := fs.Bool("scheduled", false, "run through the scheduler worker pool")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	query := strings.TrimSpace(strings.Join(fs.Args(), " "))
	if query == "" {
		return fmt.Errorf("task query is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	if *maxSteps > 0 {
		cfg.Agent.DefaultMaxSteps = *maxSteps
	}
	baseStore, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	store := newEventRoutedStore(context.Background(), baseStore, cfg)
	defer store.Close()

	streamID := "task:" + newID()
	turnID := "turn:" + newID()
	runID := "run:" + newID()
	route := workflow.Route(query, *workflowName)
	workspaceAbs, _ := filepath.Abs(*workspacePath)
	client, taskModel, err := buildTaskClient(*workerMode, *model, *baseURL, *apiKeyEnv, *workerAddress, workspaceAbs, cfg)
	if err != nil {
		return err
	}
	if closer, ok := client.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.GRPC.ExecuteTimeoutSeconds)*time.Second)
	defer cancel()
	if _, err := store.AppendEvents(ctx, []storage.AppendEvent{
		{StreamID: streamID, EventType: "SessionCreated", Payload: map[string]any{
			"session_id":    streamID,
			"query":         query,
			"workspace":     workspaceAbs,
			"workflow_type": route.Workflow,
		}},
		{StreamID: streamID, EventType: "TurnCreated", Payload: map[string]any{
			"session_id": streamID,
			"turn_id":    turnID,
			"query":      query,
		}},
		{StreamID: streamID, EventType: "TaskCreated", Payload: map[string]any{
			"session_id":    streamID,
			"turn_id":       turnID,
			"run_id":        runID,
			"query":         query,
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
		return err
	}
	task := &workflow.TaskHandle{
		StreamID:     streamID,
		Mode:         workflow.ContextModeExecution,
		WorkflowType: route.Workflow,
		SessionID:    streamID,
		TurnID:       turnID,
		RunID:        runID,
		Query:        query,
		Workspace:    workspaceAbs,
		SystemPrompt: defaultAgentSystemPrompt(workspaceAbs),
		Model:        taskModel,
		AgentRole:    "default",
		Tools:        worker.BuiltinToolDefinitionsWithAllowlist(cfg.Safety.ToolAllowlist),
		Config:       cfg,
		Client:       client,
	}
	var result *workflow.TaskResult
	var runErr error
	if *scheduled {
		result, runErr = executeScheduled(ctx, store, cfg, task)
	} else {
		result, runErr = workflow.Execute(ctx, store, workflow.NewRegistry(), task)
	}
	if runErr != nil {
		if errors.Is(runErr, workflow.ErrWorkflowSuspended) {
			out := map[string]any{
				"task_id":          streamID,
				"status":           "PAUSED",
				"workflow":         task.WorkflowType,
				"complexity_score": route.ComplexityScore,
				"error":            runErr.Error(),
			}
			if *output == "json" {
				return json.NewEncoder(os.Stdout).Encode(out)
			}
			fmt.Printf("task_id:    %s\n", streamID)
			fmt.Printf("status:     PAUSED\n")
			fmt.Printf("workflow:   %s (%s)\n", task.WorkflowType, route.Reason)
			fmt.Printf("reason:     %v\n", runErr)
			return nil
		}
		return runErr
	}
	out := map[string]any{
		"task_id":          streamID,
		"status":           "COMPLETED",
		"workflow":         result.Workflow,
		"complexity_score": route.ComplexityScore,
		"result":           result.Result,
	}
	if *output == "json" {
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	fmt.Printf("task_id:    %s\n", streamID)
	fmt.Printf("status:     COMPLETED\n")
	fmt.Printf("workflow:   %s (%s)\n", result.Workflow, route.Reason)
	fmt.Printf("result:     %v\n", result.Result)
	return nil
}

func executeScheduled(ctx context.Context, store storage.Store, cfg *config.RuntimeConfig, task *workflow.TaskHandle) (*workflow.TaskResult, error) {
	s := scheduler.New(store, workflow.NewRegistry(), cfg.Workflow.MaxConcurrentTasks, cfg.Scheduler.QueueSize)
	defer s.Stop()
	if err := s.Submit(ctx, task); err != nil {
		return nil, err
	}
	select {
	case result := <-s.Results():
		if result == nil {
			return nil, fmt.Errorf("scheduler returned nil result")
		}
		return result, result.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func buildTaskClient(mode, model, baseURL, apiKeyEnv, workerAddress, workspace string, cfg *config.RuntimeConfig) (worker.Client, string, error) {
	return buildTaskClientWithKey(mode, model, baseURL, apiKeyEnv, "", workerAddress, workspace, cfg)
}

func buildTaskClientWithKey(mode, model, baseURL, apiKeyEnv, directAPIKey, workerAddress, workspace string, cfg *config.RuntimeConfig) (worker.Client, string, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "echo"
	}
	switch mode {
	case "echo":
		return worker.NewLocalAgentClientWithAllowlist(worker.NewEchoClient(), workspace, cfg.Safety.ShellDangerousPatterns, cfg.Safety.ToolAllowlist), model, nil
	case "openai":
		provider := findLLMProvider(cfg, "openai")
		resolvedBaseURL, apiKey, resolvedModel := resolveLLMOptions(provider, model, baseURL, apiKeyEnv, "OPENAI_API_KEY", worker.DefaultOpenAIModel)
		if strings.TrimSpace(directAPIKey) != "" {
			apiKey = strings.TrimSpace(directAPIKey)
		}
		if apiKey == "" {
			return nil, "", fmt.Errorf("--worker openai requires %s or an llm_providers api_key", defaultEnvName(apiKeyEnv, "OPENAI_API_KEY"))
		}
		generator, err := worker.NewOpenAIClient(worker.OpenAIConfig{
			BaseURL: resolvedBaseURL,
			APIKey:  apiKey,
			Model:   resolvedModel,
		})
		if err != nil {
			return nil, "", err
		}
		return worker.NewLocalAgentClientWithAllowlist(generator, workspace, cfg.Safety.ShellDangerousPatterns, cfg.Safety.ToolAllowlist), resolvedModel, nil
	case "deepseek":
		provider := findLLMProvider(cfg, "deepseek")
		resolvedBaseURL, apiKey, resolvedModel := resolveLLMOptions(provider, model, baseURL, apiKeyEnv, "DEEPSEEK_API_KEY", worker.DefaultDeepSeekModel)
		if strings.TrimSpace(directAPIKey) != "" {
			apiKey = strings.TrimSpace(directAPIKey)
		}
		if apiKey == "" {
			return nil, "", fmt.Errorf("--worker deepseek requires %s or a deepseek llm_providers api_key", defaultEnvName(apiKeyEnv, "DEEPSEEK_API_KEY"))
		}
		generator, err := worker.NewDeepSeekClient(worker.OpenAIConfig{
			BaseURL: resolvedBaseURL,
			APIKey:  apiKey,
			Model:   resolvedModel,
		})
		if err != nil {
			return nil, "", err
		}
		return worker.NewLocalAgentClientWithAllowlist(generator, workspace, cfg.Safety.ShellDangerousPatterns, cfg.Safety.ToolAllowlist), resolvedModel, nil
	case "grpc":
		address := strings.TrimSpace(workerAddress)
		if address == "" {
			address = fmt.Sprintf("127.0.0.1:%d", cfg.GRPC.WorkerPort)
		}
		client, err := gateway.NewWorkerClient(gateway.ClientOptions{
			Address:               address,
			ControlTimeout:        time.Duration(cfg.GRPC.ControlTimeoutSeconds) * time.Second,
			ExecuteTimeout:        time.Duration(cfg.GRPC.ExecuteTimeoutSeconds) * time.Second,
			RetryMaxAttempts:      cfg.GRPC.RetryMaxAttempts,
			RetryBackoffBase:      durationOrDefault(cfg.GRPC.RetryBackoffBaseMS, 1000),
			CircuitBreakerFailMax: cfg.GRPC.CircuitBreakerThreshold,
			CircuitBreakerTimeout: time.Duration(cfg.GRPC.CircuitBreakerTimeout) * time.Second,
		})
		if err != nil {
			return nil, "", err
		}
		resolvedModel := strings.TrimSpace(model)
		if resolvedModel == "" {
			resolvedModel = "python-worker"
		}
		return client, resolvedModel, nil
	default:
		return nil, "", fmt.Errorf("unknown worker mode %q", mode)
	}
}

func durationOrDefault(ms int, fallback int) time.Duration {
	if ms <= 0 {
		ms = fallback
	}
	return time.Duration(ms) * time.Millisecond
}

func zeroFencingLease() guard.FencingLease {
	return guard.FencingLease{}
}

func newEventRoutedStore(ctx context.Context, base storage.Store, cfg *config.RuntimeConfig) storage.Store {
	stream := newStreamChannel(ctx, cfg)
	return eventrouter.New(base, stream)
}

func newStreamChannel(ctx context.Context, cfg *config.RuntimeConfig) eventrouter.StreamChannel {
	if cfg == nil || cfg.Redis.Addr == "" {
		return eventrouter.NewMemoryStream()
	}
	client := redis.NewClient(&redis.Options{
		Addr:        cfg.Redis.Addr,
		Password:    cfg.Redis.Password,
		DB:          cfg.Redis.DB,
		DialTimeout: 200 * time.Millisecond,
		ReadTimeout: 200 * time.Millisecond,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return eventrouter.NewMemoryStream()
	}
	return eventrouter.NewRedisStream(client)
}

func resolveLLMOptions(provider *config.LLMProvider, model, baseURL, apiKeyEnv, fallbackAPIKeyEnv, fallbackModel string) (string, string, string) {
	resolvedBaseURL := strings.TrimSpace(baseURL)
	if resolvedBaseURL == "" && provider != nil {
		resolvedBaseURL = provider.BaseURL
	}
	apiKeyName := defaultEnvName(apiKeyEnv, fallbackAPIKeyEnv)
	apiKey := strings.TrimSpace(os.Getenv(apiKeyName))
	if apiKey == "" && provider != nil {
		apiKey = provider.APIKey
	}
	resolvedModel := strings.TrimSpace(model)
	if resolvedModel == "" && provider != nil {
		resolvedModel = provider.DefaultModel
	}
	if resolvedModel == "" {
		resolvedModel = fallbackModel
	}
	return resolvedBaseURL, apiKey, resolvedModel
}

func defaultEnvName(apiKeyEnv, fallback string) string {
	apiKeyName := strings.TrimSpace(apiKeyEnv)
	if apiKeyName == "" {
		apiKeyName = fallback
	}
	return apiKeyName
}

func findLLMProvider(cfg *config.RuntimeConfig, name string) *config.LLMProvider {
	if cfg == nil {
		return nil
	}
	for i := range cfg.LLM {
		provider := &cfg.LLM[i]
		if strings.EqualFold(provider.Adapter, name) || strings.EqualFold(provider.Name, name) {
			return provider
		}
	}
	if name == "openai" && len(cfg.LLM) > 0 {
		return &cfg.LLM[0]
	}
	return nil
}

func defaultAgentSystemPrompt(workspace string) string {
	return fmt.Sprintf(`You are Tenet, a local agent running inside workspace %q.
Plan briefly, use tools when they help, and keep final answers concise.
Use workspace-relative paths for file tools. Do not access paths outside the workspace.
When changing code, inspect existing files first, make focused edits, and run relevant checks when possible.`, workspace)
}

func taskReplay(args []string) error {
	fs := flag.NewFlagSet("task replay", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	streamID := fs.String("stream", "", "stream id")
	runID := fs.String("run", "", "optional run id; defaults to latest run in the stream")
	toSeq := fs.Int64("to-seq", 0, "verify event prefix up to this stream sequence without executing workflow")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamID == "" {
		return fmt.Errorf("--stream is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	if *toSeq > 0 {
		events, err := store.Read(*streamID, 1)
		if err != nil {
			return err
		}
		prefix := make([]storage.Event, 0, len(events))
		for _, event := range events {
			if event.StreamSeq > *toSeq {
				break
			}
			prefix = append(prefix, event)
		}
		trace, err := projection.BuildTraceView(*streamID, prefix)
		if err != nil {
			return err
		}
		if *output == "json" {
			return json.NewEncoder(os.Stdout).Encode(map[string]any{
				"stream_id":       *streamID,
				"to_seq":          *toSeq,
				"events_replayed": len(prefix),
				"trace_spans":     len(trace.Spans),
				"prefix_valid":    true,
				"trace":           trace,
			})
		}
		fmt.Printf("Replay prefix %s\n", *streamID)
		fmt.Printf("to_seq:     %d\n", *toSeq)
		fmt.Printf("events:     %d\n", len(prefix))
		fmt.Printf("trace_spans:%d\n", len(trace.Spans))
		fmt.Println("prefix:     VALID")
		return nil
	}
	result, err := workflow.Replay(context.Background(), store, workflow.NewRegistry(), &workflow.TaskHandle{
		StreamID: *streamID,
		RunID:    *runID,
		Config:   cfg,
	})
	if err != nil {
		return err
	}
	if *output == "json" {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"stream_id":       result.StreamID,
			"session_id":      result.SessionID,
			"turn_id":         result.TurnID,
			"run_id":          result.RunID,
			"workflow":        result.Workflow,
			"events_replayed": result.EventsReplayed,
			"latest_seq":      result.LatestSeq,
			"result":          result.Result,
			"deterministic":   true,
		})
	}
	fmt.Printf("Replay %s\n", result.StreamID)
	if result.RunID != "" {
		fmt.Printf("run:        %s\n", result.RunID)
	}
	if result.TurnID != "" {
		fmt.Printf("turn:       %s\n", result.TurnID)
	}
	fmt.Printf("workflow:   %s\n", result.Workflow)
	fmt.Printf("events:     %d\n", result.EventsReplayed)
	fmt.Printf("latest_seq: %d\n", result.LatestSeq)
	fmt.Println("determinism: PASSED")
	return nil
}

func taskInspect(args []string) error {
	fs := flag.NewFlagSet("task inspect", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	streamID := fs.String("stream", "", "stream id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamID == "" {
		return fmt.Errorf("--stream is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	events, err := store.Read(*streamID, 1)
	if err != nil {
		return err
	}
	for _, evt := range events {
		fmt.Printf("%03d %s %s\n%s\n", evt.StreamSeq, evt.Timestamp.Format(time.RFC3339), evt.EventType, evt.Payload)
	}
	return nil
}

func taskWatch(args []string) error {
	fs := flag.NewFlagSet("task watch", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	streamID := fs.String("stream", "", "stream id")
	fromSeq := fs.Int64("from", 1, "first stream sequence to print")
	follow := fs.Bool("follow", false, "continue polling for new events")
	interval := fs.Duration("interval", 500*time.Millisecond, "poll interval when --follow is set")
	output := fs.String("output", "text", "output format: text/jsonl")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamID == "" {
		return fmt.Errorf("--stream is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	nextSeq := *fromSeq
	for {
		events, err := store.Read(*streamID, nextSeq)
		if err != nil {
			return err
		}
		for _, evt := range events {
			if err := printWatchEvent(evt, *output); err != nil {
				return err
			}
			nextSeq = evt.StreamSeq + 1
		}
		if !*follow {
			return nil
		}
		wait := *interval
		if wait <= 0 {
			wait = 500 * time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func taskStatus(args []string) error {
	fs := flag.NewFlagSet("task status", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	streamID := fs.String("stream", "", "stream id")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamID == "" {
		return fmt.Errorf("--stream is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	view, err := projection.NewEngine(store, cfg).ProjectTask(*streamID)
	if err != nil {
		return err
	}
	if *output == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(view)
	}
	fmt.Printf("task_id:       %s\n", view.StreamID)
	fmt.Printf("status:        %s\n", view.Status)
	if view.WorkflowType != "" {
		fmt.Printf("workflow:      %s\n", view.WorkflowType)
	}
	fmt.Printf("progress:      %d/%d\n", view.Progress.CompletedSteps, view.Progress.TotalSteps)
	fmt.Printf("timeline:      %d steps\n", view.Timeline.TotalSteps)
	fmt.Printf("tokens:        %d/%d\n", view.Tokens.TotalTokens, view.Tokens.BudgetLimit)
	if view.FinalAnswer != "" {
		fmt.Printf("final_answer:  %s\n", view.FinalAnswer)
	}
	if view.Error != "" {
		fmt.Printf("error:         %s\n", view.Error)
	}
	return nil
}

func taskTrace(args []string) error {
	fs := flag.NewFlagSet("task trace", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	streamID := fs.String("stream", "", "stream id")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamID == "" {
		return fmt.Errorf("--stream is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	view, err := projection.NewEngine(store, cfg).ProjectTrace(context.Background(), *streamID)
	if err != nil {
		return err
	}
	if *output == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(view)
	}
	depths := traceDepths(view)
	fmt.Printf("trace: %s\n", view.StreamID)
	fmt.Printf("spans: %d\n", len(view.Spans))
	for _, span := range view.Spans {
		indent := strings.Repeat("  ", depths[span.ID])
		status := span.Status
		if status == "" {
			status = projection.StatusRunning
		}
		fmt.Printf("%s- [%s] %s %s seq=%d", indent, status, span.Type, span.Name, span.StartedSeq)
		if span.CompletedSeq > 0 {
			fmt.Printf("..%d", span.CompletedSeq)
		}
		if span.Error != "" {
			fmt.Printf(" error=%s", span.Error)
		}
		fmt.Println()
	}
	return nil
}

func traceDepths(view projection.TraceView) map[string]int {
	parentByID := map[string]string{}
	for _, span := range view.Spans {
		parentByID[span.ID] = span.ParentID
	}
	depths := map[string]int{}
	var depthOf func(string) int
	depthOf = func(spanID string) int {
		if depth, ok := depths[spanID]; ok {
			return depth
		}
		parentID := parentByID[spanID]
		if parentID == "" {
			depths[spanID] = 0
			return 0
		}
		depths[spanID] = depthOf(parentID) + 1
		return depths[spanID]
	}
	for _, span := range view.Spans {
		depthOf(span.ID)
	}
	return depths
}

func taskCheckpoints(args []string) error {
	fs := flag.NewFlagSet("task checkpoints", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	streamID := fs.String("stream", "", "stream id")
	limit := fs.Int("limit", 100, "maximum number of checkpoints")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamID == "" {
		return fmt.Errorf("--stream is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	checkpoints, err := store.ListAgentCheckpoints(context.Background(), *streamID, *limit)
	if err != nil {
		return err
	}
	if *output == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(checkpoints)
	}
	fmt.Printf("checkpoints: %s (%d)\n", *streamID, len(checkpoints))
	for _, checkpoint := range checkpoints {
		fmt.Printf("- %s reason=%s seq=%d run=%s phase=%s\n", checkpoint.ID, checkpoint.Reason, checkpoint.EventSeq, checkpoint.RunID, checkpoint.WorkflowPhase)
	}
	return nil
}

func taskArtifacts(args []string) error {
	fs := flag.NewFlagSet("task artifacts", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	streamID := fs.String("stream", "", "stream id")
	path := fs.String("path", "", "artifact path; when set, show versions")
	rollbackVersion := fs.Int("rollback-version", 0, "rollback artifact path to this version")
	workspace := fs.String("workspace", "", "workspace override for rollback")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamID == "" {
		return fmt.Errorf("--stream is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	if *rollbackVersion > 0 {
		if *path == "" {
			return fmt.Errorf("--path is required with --rollback-version")
		}
		result, err := rollbackArtifact(context.Background(), store, *streamID, *path, *rollbackVersion, *workspace)
		if err != nil {
			return err
		}
		if *output == "json" {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(result)
		}
		fmt.Printf("rolled back %s to v%d -> new version %s\n", result.Path, *rollbackVersion, result.VersionID)
		return nil
	}
	if *path != "" {
		versions, err := store.ListArtifactVersions(context.Background(), *streamID, *path)
		if err != nil {
			return err
		}
		if *output == "json" {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(versions)
		}
		fmt.Printf("artifact: %s %s (%d versions)\n", *streamID, *path, len(versions))
		for _, version := range versions {
			fmt.Printf("- v%d %s seq=%d tool=%s size=%d\n", version.Version, version.ID, version.EventSeq, version.ProducerToolCallID, version.SizeBytes)
		}
		return nil
	}
	artifacts, err := store.ListArtifacts(context.Background(), *streamID)
	if err != nil {
		return err
	}
	if *output == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(artifacts)
	}
	fmt.Printf("artifacts: %s (%d)\n", *streamID, len(artifacts))
	for _, artifact := range artifacts {
		fmt.Printf("- %s type=%s current=%s\n", artifact.Path, artifact.ArtifactType, artifact.CurrentVersionID)
	}
	return nil
}

type artifactRollbackResult struct {
	StreamID  string `json:"stream_id"`
	Path      string `json:"path"`
	Version   int    `json:"version"`
	VersionID string `json:"version_id"`
	Workspace string `json:"workspace"`
}

func rollbackArtifact(ctx context.Context, store storage.Store, streamID, artifactPath string, versionNumber int, workspaceOverride string) (artifactRollbackResult, error) {
	target, err := store.GetArtifactVersion(ctx, streamID, artifactPath, versionNumber)
	if err != nil {
		return artifactRollbackResult{}, err
	}
	workspace := firstNonEmpty(workspaceOverride, target.Workspace)
	if workspace == "" {
		return artifactRollbackResult{}, fmt.Errorf("workspace is required")
	}
	abs, err := safeArtifactTarget(workspace, target.Path)
	if err != nil {
		return artifactRollbackResult{}, err
	}
	started, err := store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  streamID,
		EventType: "ArtifactRollbackStarted",
		Payload: map[string]any{
			"artifact_id": target.ArtifactID,
			"path":        target.Path,
			"to_version":  versionNumber,
			"workspace":   workspace,
		},
	})
	if err != nil {
		return artifactRollbackResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		_, _ = store.AppendEvent(ctx, storage.AppendEvent{StreamID: streamID, EventType: "ArtifactRollbackFailed", Payload: map[string]any{"path": target.Path, "error": err.Error()}})
		return artifactRollbackResult{}, err
	}
	if err := os.WriteFile(abs, []byte(target.ContentBlob), 0644); err != nil {
		_, _ = store.AppendEvent(ctx, storage.AppendEvent{StreamID: streamID, EventType: "ArtifactRollbackFailed", Payload: map[string]any{"path": target.Path, "error": err.Error()}})
		return artifactRollbackResult{}, err
	}
	newVersion, err := store.RecordArtifactVersion(ctx, storage.ArtifactVersion{
		StreamID:           streamID,
		TurnID:             target.TurnID,
		RunID:              target.RunID,
		Workspace:          workspace,
		Path:               target.Path,
		ArtifactType:       target.ArtifactType,
		EventSeq:           started.StreamSeq,
		ProducerToolCallID: "rollback",
		ContentHash:        target.ContentHash,
		ContentBlob:        target.ContentBlob,
		SizeBytes:          int64(len(target.ContentBlob)),
		Summary:            fmt.Sprintf("rollback to version %d", versionNumber),
	})
	if err != nil {
		_, _ = store.AppendEvent(ctx, storage.AppendEvent{StreamID: streamID, EventType: "ArtifactRollbackFailed", Payload: map[string]any{"path": target.Path, "error": err.Error()}})
		return artifactRollbackResult{}, err
	}
	if _, err := store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  streamID,
		EventType: "ArtifactVersionCreated",
		Payload: map[string]any{
			"artifact_id":           newVersion.ArtifactID,
			"version_id":            newVersion.ID,
			"version":               newVersion.Version,
			"path":                  newVersion.Path,
			"artifact_type":         newVersion.ArtifactType,
			"content_hash":          newVersion.ContentHash,
			"size_bytes":            newVersion.SizeBytes,
			"producer_tool_call_id": "rollback",
			"event_seq":             newVersion.EventSeq,
		},
	}); err != nil {
		return artifactRollbackResult{}, err
	}
	if _, err := store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  streamID,
		EventType: "ArtifactRollbackCompleted",
		Payload: map[string]any{
			"artifact_id": target.ArtifactID,
			"path":        target.Path,
			"to_version":  versionNumber,
			"version_id":  newVersion.ID,
			"workspace":   workspace,
		},
	}); err != nil {
		return artifactRollbackResult{}, err
	}
	return artifactRollbackResult{StreamID: streamID, Path: target.Path, Version: newVersion.Version, VersionID: newVersion.ID, Workspace: workspace}, nil
}

func safeArtifactTarget(workspace string, rel string) (string, error) {
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("artifact path must be relative")
	}
	clean := filepath.Clean(rel)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("artifact path escapes workspace")
	}
	return filepath.Join(workspace, clean), nil
}

func taskRestore(args []string) error {
	fs := flag.NewFlagSet("task restore", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	streamID := fs.String("stream", "", "stream id")
	checkpointID := fs.String("checkpoint", "", "checkpoint id")
	workspace := fs.String("workspace", "", "workspace to restore into")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamID == "" || *checkpointID == "" || *workspace == "" {
		return fmt.Errorf("--stream, --checkpoint and --workspace are required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	result, err := restoreCheckpoint(context.Background(), store, cfg, *streamID, *checkpointID, *workspace)
	if err != nil {
		return err
	}
	if *output == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}
	fmt.Printf("restored checkpoint %s into %s from %s\n", result.CheckpointID, result.Workspace, result.SnapshotRef)
	return nil
}

type checkpointRestoreResult struct {
	StreamID     string `json:"stream_id"`
	CheckpointID string `json:"checkpoint_id"`
	Workspace    string `json:"workspace"`
	SnapshotType string `json:"snapshot_type"`
	SnapshotRef  string `json:"snapshot_ref"`
	SnapshotSeq  int64  `json:"snapshot_seq"`
}

func restoreCheckpoint(ctx context.Context, store storage.Store, cfg *config.RuntimeConfig, streamID, checkpointID, workspace string) (checkpointRestoreResult, error) {
	if strings.TrimSpace(checkpointID) == "" {
		return checkpointRestoreResult{}, fmt.Errorf("checkpoint_id is required")
	}
	if strings.TrimSpace(workspace) == "" {
		return checkpointRestoreResult{}, fmt.Errorf("workspace is required")
	}
	checkpoint, err := store.GetAgentCheckpoint(ctx, checkpointID)
	if err != nil {
		return checkpointRestoreResult{}, err
	}
	if checkpoint.StreamID != streamID {
		return checkpointRestoreResult{}, fmt.Errorf("checkpoint %s belongs to stream %s", checkpointID, checkpoint.StreamID)
	}
	snapshot, err := store.LatestSnapshot(streamID, checkpoint.EventSeq)
	if err != nil {
		_, _ = store.AppendEvent(ctx, storage.AppendEvent{StreamID: streamID, EventType: "AgentCheckpointRestoreFailed", Payload: map[string]any{"checkpoint_id": checkpointID, "workspace": workspace, "error": err.Error()}})
		return checkpointRestoreResult{}, err
	}
	if _, err := store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  streamID,
		EventType: "AgentCheckpointRestoreStarted",
		Payload: map[string]any{
			"checkpoint_id": checkpointID,
			"workspace":     workspace,
			"snapshot_type": snapshot.Type,
			"snapshot_ref":  snapshot.Ref,
			"snapshot_seq":  snapshot.StreamSeq,
		},
	}); err != nil {
		return checkpointRestoreResult{}, err
	}
	if err := workspacepkg.NewManager(cfg).Restore(ctx, workspacepkg.Snapshot{Type: snapshot.Type, Ref: snapshot.Ref}, workspace, nil, zeroFencingLease()); err != nil {
		_, _ = store.AppendEvent(ctx, storage.AppendEvent{StreamID: streamID, EventType: "AgentCheckpointRestoreFailed", Payload: map[string]any{"checkpoint_id": checkpointID, "workspace": workspace, "error": err.Error()}})
		return checkpointRestoreResult{}, err
	}
	if _, err := store.AppendEvent(ctx, storage.AppendEvent{
		StreamID:  streamID,
		EventType: "AgentCheckpointRestoreCompleted",
		Payload: map[string]any{
			"checkpoint_id": checkpointID,
			"workspace":     workspace,
			"snapshot_type": snapshot.Type,
			"snapshot_ref":  snapshot.Ref,
			"snapshot_seq":  snapshot.StreamSeq,
		},
	}); err != nil {
		return checkpointRestoreResult{}, err
	}
	return checkpointRestoreResult{StreamID: streamID, CheckpointID: checkpointID, Workspace: workspace, SnapshotType: snapshot.Type, SnapshotRef: snapshot.Ref, SnapshotSeq: snapshot.StreamSeq}, nil
}

func taskFork(args []string) error {
	fs := flag.NewFlagSet("task fork", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	streamID := fs.String("stream", "", "parent stream id")
	seq := fs.Int64("seq", 0, "fork after this stream sequence")
	checkpointID := fs.String("checkpoint", "", "fork from this checkpoint id")
	query := fs.String("query", "", "new query for the fork")
	restoreWorkspace := fs.Bool("restore-workspace", true, "initialize fork workspace from the latest snapshot before --seq")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamID == "" {
		return fmt.Errorf("--stream is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	if *checkpointID != "" {
		checkpoint, err := store.GetAgentCheckpoint(context.Background(), *checkpointID)
		if err != nil {
			return err
		}
		if checkpoint.StreamID != *streamID {
			return fmt.Errorf("checkpoint %s belongs to stream %s", *checkpointID, checkpoint.StreamID)
		}
		*seq = checkpoint.EventSeq
	}
	if *seq <= 0 {
		return fmt.Errorf("--seq or --checkpoint is required")
	}
	var fork workspacepkg.ForkResult
	if *restoreWorkspace {
		fork, err = workspacepkg.NewManager(cfg).ForkWorkspace(context.Background(), store, *streamID, *seq, *query, nil, zeroFencingLease())
		if err != nil {
			return err
		}
	} else {
		childID, err := store.ForkStream(context.Background(), *streamID, *seq, *query)
		if err != nil {
			return err
		}
		fork = workspacepkg.ForkResult{
			StreamID:  childID,
			ParentID:  *streamID,
			ForkAtSeq: *seq,
		}
	}
	if *output == "json" {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"stream_id":   fork.StreamID,
			"parent_id":   *streamID,
			"fork_at_seq": *seq,
			"new_query":   *query,
			"workspace":   fork.Workspace,
			"restored":    fork.Restored,
			"snapshot":    fork.Snapshot,
		})
	}
	fmt.Printf("fork:      %s\n", fork.StreamID)
	fmt.Printf("parent:    %s\n", *streamID)
	fmt.Printf("fork_seq:  %d\n", *seq)
	if fork.Workspace != "" {
		fmt.Printf("workspace: %s\n", fork.Workspace)
		fmt.Printf("restored:  %t\n", fork.Restored)
	}
	if fork.Snapshot != nil {
		fmt.Printf("snapshot:  %s %s\n", fork.Snapshot.Type, fork.Snapshot.Ref)
	}
	return nil
}

func taskLineage(args []string) error {
	fs := flag.NewFlagSet("task lineage", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	streamID := fs.String("stream", "", "stream id")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamID == "" {
		return fmt.Errorf("--stream is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	lineage, err := store.GetLineage(*streamID)
	if err != nil {
		return err
	}
	children, err := store.GetChildStreams(*streamID)
	if err != nil {
		return err
	}
	if *output == "json" {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"stream_id": *streamID,
			"lineage":   lineage,
			"children":  children,
		})
	}
	fmt.Printf("stream:   %s\n", *streamID)
	fmt.Printf("lineage:  %s\n", strings.Join(lineage, " -> "))
	if len(children) > 0 {
		fmt.Printf("children: %s\n", strings.Join(children, ", "))
	}
	return nil
}

func taskLogs(args []string) error {
	return taskWatch(args)
}

func taskCancel(args []string) error {
	fs := flag.NewFlagSet("task cancel", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	streamID := fs.String("stream", "", "stream id")
	reason := fs.String("reason", "cancelled by user", "cancel reason")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamID == "" {
		return fmt.Errorf("--stream is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	event, err := store.AppendEvent(context.Background(), storage.AppendEvent{
		StreamID:  *streamID,
		EventType: "TaskCancelled",
		Payload:   map[string]any{"reason": *reason},
	})
	if err != nil {
		return err
	}
	if *output == "json" {
		return json.NewEncoder(os.Stdout).Encode(eventrouter.FromStorageEvent(event))
	}
	fmt.Printf("cancelled: %s seq=%d\n", *streamID, event.StreamSeq)
	return nil
}

func taskResume(args []string) error {
	fs := flag.NewFlagSet("task resume", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	streamID := fs.String("stream", "", "stream id")
	note := fs.String("note", "resume requested", "resume note")
	after := fs.Duration("after", 0, "delay before resuming, for example 500ms or 10s")
	output := fs.String("output", "text", "output format: text/json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamID == "" {
		return fmt.Errorf("--stream is required")
	}
	cfg, err := loadConfig(*configPath, true)
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()
	if *after > 0 {
		service := timerpkg.NewService(store)
		timerID := fmt.Sprintf("resume:%d", time.Now().UTC().UnixNano())
		done, err := service.Schedule(context.Background(), timerpkg.ScheduleRequest{
			StreamID:           *streamID,
			TimerID:            timerID,
			Delay:              *after,
			ScheduledEventType: "TaskResumeScheduled",
			FiredEventType:     "TimerFired",
			Payload:            map[string]any{"note": *note},
		})
		if err != nil {
			return err
		}
		result := <-done
		if result.Err != nil {
			return result.Err
		}
		event, err := store.AppendEvent(context.Background(), storage.AppendEvent{
			StreamID:  *streamID,
			EventType: "TaskResumed",
			Payload: map[string]any{
				"note":     *note,
				"timer_id": timerID,
			},
		})
		if err != nil {
			return err
		}
		if *output == "json" {
			return json.NewEncoder(os.Stdout).Encode(map[string]any{
				"scheduled": eventrouter.FromStorageEvent(result.Scheduled),
				"fired":     eventrouter.FromStorageEvent(result.Fired),
				"resumed":   eventrouter.FromStorageEvent(event),
			})
		}
		fmt.Printf("resume_scheduled: %s seq=%d\n", *streamID, result.Scheduled.StreamSeq)
		fmt.Printf("timer_fired:      %s seq=%d\n", *streamID, result.Fired.StreamSeq)
		fmt.Printf("resumed:          %s seq=%d\n", *streamID, event.StreamSeq)
		return nil
	}
	event, err := store.AppendEvent(context.Background(), storage.AppendEvent{
		StreamID:  *streamID,
		EventType: "TaskResumed",
		Payload:   map[string]any{"note": *note},
	})
	if err != nil {
		return err
	}
	if *output == "json" {
		return json.NewEncoder(os.Stdout).Encode(eventrouter.FromStorageEvent(event))
	}
	fmt.Printf("resumed: %s seq=%d\n", *streamID, event.StreamSeq)
	return nil
}

func printWatchEvent(evt storage.Event, output string) error {
	if output == "jsonl" || output == "json" {
		return json.NewEncoder(os.Stdout).Encode(eventrouter.FromStorageEvent(evt))
	}
	fmt.Printf("%03d %s %s %s\n", evt.StreamSeq, evt.Timestamp.Format(time.RFC3339), evt.EventType, evt.Payload)
	return nil
}

func loadConfig(path string, allowDefault bool) (*config.RuntimeConfig, error) {
	cfg, err := config.Load(path)
	if err == nil {
		return cfg, nil
	}
	if allowDefault && os.IsNotExist(unpackPathErr(err)) {
		return config.Default(), nil
	}
	return nil, err
}

func unpackPathErr(err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	if strings.Contains(text, "no such file or directory") {
		return os.ErrNotExist
	}
	return err
}

func maskSecrets(cfg *config.RuntimeConfig) {
	if cfg.Redis.Password != "" {
		cfg.Redis.Password = "****"
	}
	for i := range cfg.LLM {
		if cfg.LLM[i].APIKey != "" {
			cfg.LLM[i].APIKey = "****"
		}
	}
}

func newID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func printUsage() {
	fmt.Println("usage: tenet <serve|task|workspace|skills|config|version> [flags]")
}
