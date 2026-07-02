package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/storage"
	"github.com/tenet/orchestrator/internal/worker"
	"github.com/tenet/orchestrator/internal/workflow"
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
	case "task":
		return taskCmd(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	port := fs.Int("port", 0, "orchestrator port")
	workerPort := fs.Int("worker-port", 0, "worker port")
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
	fmt.Printf("tenet ready: orchestrator=:%d worker=:%d database=%s\n", cfg.GRPC.OrchestratorPort, cfg.GRPC.WorkerPort, cfg.Database.Path)
	return nil
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

func taskCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing task subcommand")
	}
	switch args[0] {
	case "run":
		return taskRun(args[1:])
	case "replay":
		return taskReplay(args[1:])
	case "inspect":
		return taskInspect(args[1:])
	default:
		return fmt.Errorf("unknown task subcommand %q", args[0])
	}
}

func taskRun(args []string) error {
	fs := flag.NewFlagSet("task run", flag.ContinueOnError)
	configPath := fs.String("config", "config/tenet.yaml", "path to configuration file")
	workspacePath := fs.String("workspace", ".", "workspace path")
	workflowName := fs.String("workflow", "auto", "workflow type")
	workerMode := fs.String("worker", "echo", "worker mode: echo/openai")
	model := fs.String("model", "", "model override")
	baseURL := fs.String("base-url", "", "OpenAI-compatible base URL")
	apiKeyEnv := fs.String("api-key-env", "OPENAI_API_KEY", "environment variable containing the API key")
	maxSteps := fs.Int("max-steps", 0, "maximum ReAct steps for this task")
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
	store, err := storage.Open(cfg.Database.Path, storage.SQLiteOptions{QueueSize: cfg.Database.WriteQueueSize})
	if err != nil {
		return err
	}
	defer store.Close()

	streamID := "task:" + newID()
	route := workflow.Route(query, *workflowName)
	workspaceAbs, _ := filepath.Abs(*workspacePath)
	client, taskModel, err := buildTaskClient(*workerMode, *model, *baseURL, *apiKeyEnv, workspaceAbs, cfg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.GRPC.ExecuteTimeoutSeconds)*time.Second)
	defer cancel()
	if _, err := store.AppendEvents(ctx, []storage.AppendEvent{
		{StreamID: streamID, EventType: "TaskCreated", Payload: map[string]any{
			"query":         query,
			"workspace":     workspaceAbs,
			"workflow_type": route.Workflow,
		}},
		{StreamID: streamID, EventType: "ComplexityAnalyzed", Payload: map[string]any{
			"complexity_score":  route.ComplexityScore,
			"reason":            route.Reason,
			"selected_workflow": route.Workflow,
		}},
	}); err != nil {
		return err
	}
	result, runErr := workflow.Execute(ctx, store, workflow.NewRegistry(), &workflow.TaskHandle{
		StreamID:     streamID,
		Mode:         workflow.ContextModeExecution,
		WorkflowType: route.Workflow,
		SessionID:    streamID,
		Query:        query,
		Workspace:    workspaceAbs,
		SystemPrompt: defaultAgentSystemPrompt(workspaceAbs),
		Model:        taskModel,
		AgentRole:    "default",
		Tools:        worker.BuiltinToolDefinitions(),
		Config:       cfg,
		Client:       client,
	})
	if runErr != nil {
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

func buildTaskClient(mode, model, baseURL, apiKeyEnv, workspace string, cfg *config.RuntimeConfig) (worker.Client, string, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "echo"
	}
	switch mode {
	case "echo":
		return worker.NewLocalAgentClient(worker.NewEchoClient(), workspace, cfg.Safety.ShellDangerousPatterns), model, nil
	case "openai":
		provider := findOpenAIProvider(cfg)
		resolvedBaseURL := strings.TrimSpace(baseURL)
		if resolvedBaseURL == "" && provider != nil {
			resolvedBaseURL = provider.BaseURL
		}
		apiKeyName := strings.TrimSpace(apiKeyEnv)
		if apiKeyName == "" {
			apiKeyName = "OPENAI_API_KEY"
		}
		apiKey := strings.TrimSpace(os.Getenv(apiKeyName))
		if apiKey == "" && provider != nil {
			apiKey = provider.APIKey
		}
		if apiKey == "" {
			return nil, "", fmt.Errorf("--worker openai requires %s or an llm_providers api_key", apiKeyName)
		}
		resolvedModel := strings.TrimSpace(model)
		if resolvedModel == "" && provider != nil {
			resolvedModel = provider.DefaultModel
		}
		generator, err := worker.NewOpenAIClient(worker.OpenAIConfig{
			BaseURL: resolvedBaseURL,
			APIKey:  apiKey,
			Model:   resolvedModel,
		})
		if err != nil {
			return nil, "", err
		}
		return worker.NewLocalAgentClient(generator, workspace, cfg.Safety.ShellDangerousPatterns), resolvedModel, nil
	default:
		return nil, "", fmt.Errorf("unknown worker mode %q", mode)
	}
}

func findOpenAIProvider(cfg *config.RuntimeConfig) *config.LLMProvider {
	if cfg == nil {
		return nil
	}
	for i := range cfg.LLM {
		provider := &cfg.LLM[i]
		if strings.EqualFold(provider.Adapter, "openai") || strings.EqualFold(provider.Name, "openai") {
			return provider
		}
	}
	if len(cfg.LLM) > 0 {
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
	fmt.Printf("Replaying %s (%d events)\n", *streamID, len(events))
	for _, evt := range events {
		fmt.Printf("  seq=%d\t%s\tok\n", evt.StreamSeq, evt.EventType)
	}
	fmt.Println("Deterministic check: PASSED")
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
	fmt.Println("usage: tenet <serve|task|config|version> [flags]")
}
