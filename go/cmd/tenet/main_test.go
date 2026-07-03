package main

import (
	"strings"
	"testing"

	"github.com/tenet/orchestrator/internal/config"
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
