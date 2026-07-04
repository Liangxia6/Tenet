package skills

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tenet/orchestrator/internal/config"
)

func TestLoadDirDiscoversSkillManifests(t *testing.T) {
	root := t.TempDir()
	manifestPath := filepath.Join(root, "code.skill.json")
	if err := os.WriteFile(manifestPath, []byte(`{
		"name": "code",
		"description": "Code tools",
		"version": "0.1.0",
		"tools": [
			{
				"name": "symbol_search",
				"description": "Search symbols",
				"parameters_schema": "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\"}},\"required\":[\"query\"]}"
			}
		],
		"mcp_servers": [
			{"name": "filesystem", "command": "npx", "args": ["-y", "@modelcontextprotocol/server-filesystem"]}
		]
	}`), 0644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	registry, err := LoadDir(root)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if len(registry.Skills) != 1 {
		t.Fatalf("skills = %+v", registry.Skills)
	}
	if registry.Skills[0].Path != manifestPath {
		t.Fatalf("path = %q, want %q", registry.Skills[0].Path, manifestPath)
	}
	tools := registry.ToolDefinitions()
	if len(tools) != 1 || tools[0].Name != "symbol_search" {
		t.Fatalf("tools = %+v", tools)
	}
	servers := registry.MCPServers()
	if len(servers) != 1 || servers[0].Name != "filesystem" || servers[0].Command != "npx" {
		t.Fatalf("servers = %+v", servers)
	}
}

func TestLoadDirRejectsInvalidToolSchema(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "bad.skill.json"), []byte(`{
		"name": "bad",
		"tools": [{"name": "broken", "parameters_schema": "{"}]
	}`), 0644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	_, err := LoadDir(root)
	if err == nil {
		t.Fatalf("expected invalid schema error")
	}
	if !strings.Contains(err.Error(), "invalid parameters_schema") {
		t.Fatalf("error = %v", err)
	}
}

func TestDiscoverHonorsAutoDiscover(t *testing.T) {
	cfg := config.Default()
	cfg.Skills.Path = t.TempDir()
	cfg.Skills.AutoDiscover = false
	registry, err := Discover(cfg)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(registry.Skills) != 0 {
		t.Fatalf("skills = %+v, want empty", registry.Skills)
	}
}
