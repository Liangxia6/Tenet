package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type StaticClass string

const (
	StaticImmutable StaticClass = "Static-Immutable"
	StaticHotReload StaticClass = "Static-HotReload"
	Dynamic         StaticClass = "Dynamic"
)

type RuntimeConfig struct {
	Database    DatabaseConfig    `yaml:"database"`
	Redis       RedisConfig       `yaml:"redis"`
	Scheduler   SchedulerConfig   `yaml:"scheduler"`
	GRPC        GRPCConfig        `yaml:"grpc"`
	Workflow    WorkflowConfig    `yaml:"workflow"`
	Workspace   WorkspaceConfig   `yaml:"workspace"`
	Skills      SkillsConfig      `yaml:"skills"`
	Agent       AgentConfig       `yaml:"agent"`
	Context     ContextConfig     `yaml:"context"`
	Memory      MemoryConfig      `yaml:"memory"`
	Safety      SafetyConfig      `yaml:"safety"`
	Interactive InteractiveConfig `yaml:"interactive"`
	RateLimits  RateLimitConfig   `yaml:"rate_limits"`
	LLM         LLMProviders      `yaml:"llm_providers"`
	Coding      CodingConfig      `yaml:"coding"`
	Logging     LoggingConfig     `yaml:"logging"`
}

func Load(path string) (*RuntimeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := &RuntimeConfig{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.resolveEnv(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func Default() *RuntimeConfig {
	cfg := &RuntimeConfig{}
	cfg.applyDefaults()
	return cfg
}

func (c *RuntimeConfig) resolveEnv() error {
	resolver := func(val *string) error {
		if val == nil {
			return nil
		}
		if strings.HasPrefix(*val, "env:") {
			key := strings.TrimPrefix(*val, "env:")
			resolved := os.Getenv(key)
			if resolved == "" {
				return fmt.Errorf("missing environment variable %q", key)
			}
			*val = resolved
		}
		return nil
	}

	if err := resolver(&c.Redis.Password); err != nil {
		return err
	}
	for i := range c.LLM {
		if err := resolver(&c.LLM[i].APIKey); err != nil {
			return err
		}
	}
	return nil
}

func (c *RuntimeConfig) Validate() error {
	if c.Database.Path == "" {
		return errors.New("database.path is required")
	}
	if c.Database.MaxOpenConns != 1 {
		return errors.New("database.max_open_conns must be 1")
	}
	if c.Redis.SessionLockTTLSeconds <= 0 {
		return errors.New("redis.session_lock_ttl_seconds must be positive")
	}
	if c.Redis.SessionHeartbeatSeconds <= 0 {
		return errors.New("redis.session_heartbeat_seconds must be positive")
	}
	if c.Redis.SessionLockTTLSeconds <= c.Redis.SessionHeartbeatSeconds {
		return errors.New("redis.session_lock_ttl_seconds must be greater than session_heartbeat_seconds")
	}
	if c.Scheduler.QueueSize <= 0 {
		return errors.New("scheduler.queue_size must be positive")
	}
	if c.GRPC.OrchestratorPort <= 0 {
		return errors.New("grpc.orchestrator_port must be positive")
	}
	if c.GRPC.WorkerPort <= 0 {
		return errors.New("grpc.worker_port must be positive")
	}
	if c.GRPC.ExecuteTimeoutSeconds <= c.GRPC.ControlTimeoutSeconds {
		return errors.New("grpc.execute_timeout_seconds must be greater than control_timeout_seconds")
	}
	if c.Workflow.RecordBatchSize <= 0 {
		return errors.New("workflow.record_batch_size must be positive")
	}
	if c.Workflow.MaxConcurrentTasks <= 0 {
		return errors.New("workflow.max_concurrent_tasks must be positive")
	}
	return nil
}

func (c *RuntimeConfig) applyDefaults() {
	if c.Database.Path == "" {
		c.Database.Path = "data/tenet.db"
	}
	if c.Database.MaxOpenConns == 0 {
		c.Database.MaxOpenConns = 1
	}
	if c.Database.WriteQueueSize == 0 {
		c.Database.WriteQueueSize = 1024
	}
	if c.Redis.Addr == "" {
		c.Redis.Addr = "127.0.0.1:6379"
	}
	if c.Redis.SessionLockTTLSeconds == 0 {
		c.Redis.SessionLockTTLSeconds = 30
	}
	if c.Redis.SessionHeartbeatSeconds == 0 {
		c.Redis.SessionHeartbeatSeconds = 10
	}
	if c.Scheduler.QueueSize == 0 {
		c.Scheduler.QueueSize = 100
	}
	if c.GRPC.OrchestratorPort == 0 {
		c.GRPC.OrchestratorPort = 50051
	}
	if c.GRPC.WorkerPort == 0 {
		c.GRPC.WorkerPort = 50052
	}
	if c.GRPC.ControlTimeoutSeconds == 0 {
		c.GRPC.ControlTimeoutSeconds = 60
	}
	if c.GRPC.ExecuteTimeoutSeconds == 0 {
		c.GRPC.ExecuteTimeoutSeconds = 300
	}
	if c.Workflow.MaxConcurrentTasks == 0 {
		c.Workflow.MaxConcurrentTasks = 4
	}
	if c.Workflow.DefaultStrategy == "" {
		c.Workflow.DefaultStrategy = "auto"
	}
	if c.Workflow.ComplexityThresholdDAG == 0 {
		c.Workflow.ComplexityThresholdDAG = 0.3
	}
	if c.Workflow.RecordBatchSize == 0 {
		c.Workflow.RecordBatchSize = 20
	}
	if c.Workspace.BasePath == "" {
		c.Workspace.BasePath = "workspaces"
	}
	if c.Agent.DefaultMaxSteps == 0 {
		c.Agent.DefaultMaxSteps = 50
	}
	if c.Agent.DefaultTemperature == 0 {
		c.Agent.DefaultTemperature = 0.7
	}
	if c.Agent.ConvergenceNoToolCalls == 0 {
		c.Agent.ConvergenceNoToolCalls = 1
	}
	if c.Agent.DefaultTokenBudget == 0 {
		c.Agent.DefaultTokenBudget = 100000
	}
	if c.Context.HistoryWindowDefault == 0 {
		c.Context.HistoryWindowDefault = 24
	}
	if c.Context.HistoryWindowDebugging == 0 {
		c.Context.HistoryWindowDebugging = 75
	}
	if c.Context.PrimersCount == 0 {
		c.Context.PrimersCount = 3
	}
	if c.Context.RecentsCount == 0 {
		c.Context.RecentsCount = 20
	}
	if c.Context.CompressionTriggerRatio == 0 {
		c.Context.CompressionTriggerRatio = 0.75
	}
	if c.Context.CompressionTargetRatio == 0 {
		c.Context.CompressionTargetRatio = 0.375
	}
	if c.Context.MaxContextTokens == 0 {
		c.Context.MaxContextTokens = c.Agent.DefaultTokenBudget
	}
	if c.Context.MaxMemoryTokens == 0 {
		c.Context.MaxMemoryTokens = 8000
	}
	if c.Context.MaxToolResultTokens == 0 {
		c.Context.MaxToolResultTokens = 12000
	}
	if c.Memory.RedisTTLHours == 0 {
		c.Memory.RedisTTLHours = 720
	}
	if c.Memory.VectorProvider == "" {
		c.Memory.VectorProvider = "qdrant"
	}
	if c.Memory.QdrantURL == "" {
		c.Memory.QdrantURL = "http://127.0.0.1:6333"
	}
	if c.Memory.EmbeddingProvider == "" {
		c.Memory.EmbeddingProvider = "openai"
	}
	if c.Memory.EmbeddingModel == "" {
		c.Memory.EmbeddingModel = "text-embedding-3-small"
	}
	if c.Memory.MaxRetrievedMemories == 0 {
		c.Memory.MaxRetrievedMemories = 8
	}
	if c.Memory.MMRLambda == 0 {
		c.Memory.MMRLambda = 0.7
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "text"
	}
	if c.Logging.Output == "" {
		c.Logging.Output = "stdout"
	}
}

type DatabaseConfig struct {
	Path           string `yaml:"path"`
	MaxOpenConns   int    `yaml:"max_open_conns"`
	BusyTimeoutMS  int    `yaml:"busy_timeout_ms"`
	WriteQueueSize int    `yaml:"write_queue_size"`
	WALMode        bool   `yaml:"wal_mode"`
}

type RedisConfig struct {
	Addr                    string `yaml:"addr"`
	Password                string `yaml:"password"`
	DB                      int    `yaml:"db"`
	SessionLockTTLSeconds   int    `yaml:"session_lock_ttl_seconds"`
	SessionHeartbeatSeconds int    `yaml:"session_heartbeat_seconds"`
}

type SchedulerConfig struct {
	QueueSize int `yaml:"queue_size"`
}

type GRPCConfig struct {
	OrchestratorPort        int `yaml:"orchestrator_port"`
	WorkerPort              int `yaml:"worker_port"`
	ControlTimeoutSeconds   int `yaml:"control_timeout_seconds"`
	ExecuteTimeoutSeconds   int `yaml:"execute_timeout_seconds"`
	RetryMaxAttempts        int `yaml:"retry_max_attempts"`
	RetryBackoffBaseMS      int `yaml:"retry_backoff_base_ms"`
	CircuitBreakerThreshold int `yaml:"circuit_breaker_threshold"`
	CircuitBreakerTimeout   int `yaml:"circuit_breaker_timeout_seconds"`
}

type WorkflowConfig struct {
	MaxConcurrentTasks       int     `yaml:"max_concurrent_tasks"`
	DefaultStrategy          string  `yaml:"default_strategy"`
	ComplexityThresholdDAG   float64 `yaml:"complexity_threshold_dag"`
	SnapshotEventInterval    int     `yaml:"snapshot_event_interval"`
	SnapshotTimeIntervalSecs int     `yaml:"snapshot_time_interval_seconds"`
	RecordBatchSize          int     `yaml:"record_batch_size"`
}

type WorkspaceConfig struct {
	BasePath             string   `yaml:"base_path"`
	SnapshotDriver       string   `yaml:"snapshot_driver"`
	ExcludePatterns      []string `yaml:"exclude_patterns"`
	BackupEnabled        bool     `yaml:"backup_enabled"`
	BackupRetentionCount int      `yaml:"backup_retention_count"`
	CleanupOnSessionEnd  bool     `yaml:"cleanup_on_session_end"`
}

type SkillsConfig struct {
	Path         string `yaml:"skills_path"`
	AutoDiscover bool   `yaml:"skills_auto_discover"`
}

type AgentConfig struct {
	DefaultMaxSteps              int     `yaml:"default_max_steps"`
	DefaultTemperature           float64 `yaml:"default_temperature"`
	ConvergenceNoToolCalls       int     `yaml:"convergence_no_tool_calls"`
	LoopDetectionRepeatThreshold int     `yaml:"loop_detection_repeat_threshold"`
	DefaultTokenBudget           int     `yaml:"default_token_budget"`
}

type ContextConfig struct {
	HistoryWindowDefault    int     `yaml:"history_window_default"`
	HistoryWindowDebugging  int     `yaml:"history_window_debugging"`
	PrimersCount            int     `yaml:"primers_count"`
	RecentsCount            int     `yaml:"recents_count"`
	CompressionTriggerRatio float64 `yaml:"compression_trigger_ratio"`
	CompressionTargetRatio  float64 `yaml:"compression_target_ratio"`
	MaxContextTokens        int     `yaml:"max_context_tokens"`
	MaxMemoryTokens         int     `yaml:"max_memory_tokens"`
	MaxToolResultTokens     int     `yaml:"max_tool_result_tokens"`
	EnableVectorMemory      bool    `yaml:"enable_vector_memory"`
}

type MemoryConfig struct {
	RedisTTLHours         int     `yaml:"redis_ttl_hours"`
	SQLiteFTSEnabled      bool    `yaml:"sqlite_fts_enabled"`
	VectorProvider        string  `yaml:"vector_provider"`
	QdrantURL             string  `yaml:"qdrant_url"`
	EmbeddingProvider     string  `yaml:"embedding_provider"`
	EmbeddingModel        string  `yaml:"embedding_model"`
	MaxRetrievedMemories  int     `yaml:"max_retrieved_memories"`
	MMRLambda             float64 `yaml:"mmr_lambda"`
	CrossSessionEnabled   bool    `yaml:"cross_session_enabled"`
	CrossWorkspaceEnabled bool    `yaml:"cross_workspace_enabled"`
	RedactBeforeWrite     bool    `yaml:"redact_before_write"`
	DefaultTTLHours       int     `yaml:"default_ttl_hours"`
}

type SafetyConfig struct {
	RequireApproval        []string `yaml:"require_approval"`
	ToolAllowlist          []string `yaml:"tool_allowlist"`
	MaxAutoFixRetries      int      `yaml:"max_auto_fix_retries"`
	ShellDangerousPatterns []string `yaml:"shell_dangerous_patterns"`
}

type InteractiveConfig struct {
	HumanTimeoutSeconds int    `yaml:"human_timeout_seconds"`
	InjectPrefix        string `yaml:"inject_prefix"`
}

type RateLimitConfig struct {
	Shell struct {
		MaxPerMinute int `yaml:"max_per_minute"`
		MaxPerSecond int `yaml:"max_per_second"`
	} `yaml:"shell"`
	WebSearch struct {
		MaxPerMinute int `yaml:"max_per_minute"`
	} `yaml:"web_search"`
	WriteFile struct {
		MaxPerSecond int `yaml:"max_per_second"`
	} `yaml:"write_file"`
	LLMCall struct {
		MaxPerMinute int `yaml:"max_per_minute"`
	} `yaml:"llm_call"`
}

type LLMProvider struct {
	Name           string   `yaml:"name"`
	Adapter        string   `yaml:"adapter"`
	BaseURL        string   `yaml:"base_url"`
	APIKey         string   `yaml:"api_key"`
	DefaultModel   string   `yaml:"default_model"`
	Models         []string `yaml:"models"`
	MaxConcurrency int      `yaml:"max_concurrency"`
	TimeoutSeconds int      `yaml:"timeout_seconds"`
}

type LLMProviders []LLMProvider

type CodingConfig struct {
	StaticCheckCmd    string `yaml:"static_check_cmd"`
	TestCmd           string `yaml:"test_cmd"`
	AutoFixMaxRetries int    `yaml:"auto_fix_max_retries"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	Output string `yaml:"output"`
}
