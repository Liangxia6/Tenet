package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tenet.yaml")
	if err := os.WriteFile(path, []byte(`
        database:
          path: data/tenet.db
          max_open_conns: 1
          busy_timeout_ms: 5000
          write_queue_size: 1000
          wal_mode: true
        redis:
          addr: 127.0.0.1:6379
          password: ""
          db: 0
          session_lock_ttl_seconds: 30
          session_heartbeat_seconds: 10
        scheduler:
          queue_size: 100
        grpc:
          orchestrator_port: 50051
          worker_port: 50052
          control_timeout_seconds: 60
          execute_timeout_seconds: 300
          retry_max_attempts: 3
          retry_backoff_base_ms: 1000
          circuit_breaker_threshold: 5
          circuit_breaker_timeout_seconds: 30
        workflow:
          max_concurrent_tasks: 10
          default_strategy: auto
          complexity_threshold_dag: 0.3
          snapshot_event_interval: 50
          snapshot_time_interval_seconds: 300
          record_batch_size: 20
        workspace:
          base_path: workspaces/
          snapshot_driver: auto
          exclude_patterns: []
          backup_enabled: true
          backup_retention_count: 3
          cleanup_on_session_end: true
        skills:
          skills_path: config/skills/
          skills_auto_discover: true
        agent:
          default_max_steps: 50
          default_temperature: 0.7
          convergence_no_tool_calls: 3
          loop_detection_repeat_threshold: 3
          default_token_budget: 100000
        safety:
          require_approval: ["shell"]
          max_auto_fix_retries: 3
          shell_dangerous_patterns: ["rm -rf /"]
        interactive:
          human_timeout_seconds: 300
          inject_prefix: "[Human Feedback]\n"
        rate_limits:
          shell:
            max_per_minute: 30
            max_per_second: 5
          web_search:
            max_per_minute: 10
          write_file:
            max_per_second: 20
          llm_call:
            max_per_minute: 60
        llm_providers:
          - name: openai
            adapter: openai
            base_url: https://api.openai.com/v1
            api_key: env:OPENAI_API_KEY
            default_model: gpt-4o
            models: [gpt-4o]
            max_concurrency: 5
            timeout_seconds: 120
        coding:
          static_check_cmd: "go vet ./..."
          test_cmd: "go test ./..."
          auto_fix_max_retries: 3
        logging:
          level: info
          format: json
          output: stdout
    `), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	t.Setenv("OPENAI_API_KEY", "dummy-key")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Database.Path != "data/tenet.db" {
		t.Errorf("unexpected database path: %s", cfg.Database.Path)
	}
	if cfg.Redis.Password != "" {
		t.Errorf("expected empty redis password, got %q", cfg.Redis.Password)
	}
	if cfg.LLM[0].APIKey != "dummy-key" {
		t.Errorf("expected env resolved api key, got %q", cfg.LLM[0].APIKey)
	}
}

func TestLoadExampleConfig(t *testing.T) {
	path := filepath.Join("..", "..", "..", "config", "tenet.example.yaml")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load example config: %v", err)
	}
	if cfg.Database.Path == "" || cfg.Workspace.BasePath == "" {
		t.Fatalf("example config missing required paths: %+v", cfg)
	}
	if len(cfg.LLM) < 2 {
		t.Fatalf("example config should include deepseek and openai providers")
	}
}

func TestLoadMissingEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tenet.yaml")
	if err := os.WriteFile(path, []byte(`
        database:
          path: data/tenet.db
          max_open_conns: 1
          busy_timeout_ms: 5000
          write_queue_size: 1000
          wal_mode: true
        redis:
          addr: 127.0.0.1:6379
          password: env:REDIS_PASSWORD
          db: 0
          session_lock_ttl_seconds: 30
          session_heartbeat_seconds: 10
        scheduler:
          queue_size: 1
        grpc:
          orchestrator_port: 50051
          worker_port: 50052
          control_timeout_seconds: 60
          execute_timeout_seconds: 300
          retry_max_attempts: 3
          retry_backoff_base_ms: 1000
          circuit_breaker_threshold: 5
          circuit_breaker_timeout_seconds: 30
        workflow:
          max_concurrent_tasks: 10
          default_strategy: auto
          complexity_threshold_dag: 0.3
          snapshot_event_interval: 50
          snapshot_time_interval_seconds: 300
          record_batch_size: 20
        workspace:
          base_path: workspaces/
          snapshot_driver: auto
          exclude_patterns: []
          backup_enabled: true
          backup_retention_count: 3
          cleanup_on_session_end: true
        skills:
          skills_path: config/skills/
          skills_auto_discover: true
        agent:
          default_max_steps: 50
          default_temperature: 0.7
          convergence_no_tool_calls: 3
          loop_detection_repeat_threshold: 3
          default_token_budget: 100000
        safety:
          require_approval: []
          max_auto_fix_retries: 3
          shell_dangerous_patterns: []
        interactive:
          human_timeout_seconds: 300
          inject_prefix: ""
        rate_limits:
          shell:
            max_per_minute: 30
            max_per_second: 5
          web_search:
            max_per_minute: 10
          write_file:
            max_per_second: 20
          llm_call:
            max_per_minute: 60
        llm_providers: []
        coding:
          static_check_cmd: "go vet ./..."
          test_cmd: "go test ./..."
          auto_fix_max_retries: 3
        logging:
          level: info
          format: json
          output: stdout
    `), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatalf("expected error for missing env var")
	}
}
