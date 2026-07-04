package skills

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/worker"
)

type Registry struct {
	Skills []SkillManifest `json:"skills"`
}

type SkillManifest struct {
	Name        string              `json:"name"`
	Description string              `json:"description,omitempty"`
	Version     string              `json:"version,omitempty"`
	Tools       []SkillTool         `json:"tools,omitempty"`
	MCPServers  []MCPServerManifest `json:"mcp_servers,omitempty"`
	Path        string              `json:"path,omitempty"`
}

type SkillTool struct {
	Name             string `json:"name"`
	Description      string `json:"description,omitempty"`
	ParametersSchema string `json:"parameters_schema,omitempty"`
}

type MCPServerManifest struct {
	Name        string            `json:"name"`
	Command     string            `json:"command"`
	Args        []string          `json:"args,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	Description string            `json:"description,omitempty"`
}

func Discover(cfg *config.RuntimeConfig) (*Registry, error) {
	if cfg == nil {
		cfg = config.Default()
	}
	if !cfg.Skills.AutoDiscover {
		return &Registry{}, nil
	}
	return LoadDir(cfg.Skills.Path)
}

func LoadDir(root string) (*Registry, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return &Registry{}, nil
	}
	info, err := os.Stat(root)
	if errors.Is(err, os.ErrNotExist) {
		return &Registry{}, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("skills path is not a directory: %s", root)
	}
	registry := &Registry{}
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "node_modules" || entry.Name() == "__pycache__" {
				return filepath.SkipDir
			}
			return nil
		}
		name := strings.ToLower(entry.Name())
		if name != "skill.json" && name != "manifest.json" && !strings.HasSuffix(name, ".skill.json") {
			return nil
		}
		manifest, err := loadManifest(path)
		if err != nil {
			return err
		}
		manifest.Path = path
		registry.Skills = append(registry.Skills, manifest)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(registry.Skills, func(i, j int) bool {
		return registry.Skills[i].Name < registry.Skills[j].Name
	})
	return registry, nil
}

func (r *Registry) ToolDefinitions() []worker.ToolDefinition {
	if r == nil {
		return nil
	}
	definitions := []worker.ToolDefinition{}
	for _, skill := range r.Skills {
		for _, tool := range skill.Tools {
			definitions = append(definitions, worker.ToolDefinition{
				Name:             tool.Name,
				Description:      tool.Description,
				ParametersSchema: tool.ParametersSchema,
			})
		}
	}
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].Name < definitions[j].Name })
	return definitions
}

func (r *Registry) MCPServers() []MCPServerManifest {
	if r == nil {
		return nil
	}
	servers := []MCPServerManifest{}
	for _, skill := range r.Skills {
		servers = append(servers, skill.MCPServers...)
	}
	sort.Slice(servers, func(i, j int) bool { return servers[i].Name < servers[j].Name })
	return servers
}

func loadManifest(path string) (SkillManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SkillManifest{}, err
	}
	var manifest SkillManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return SkillManifest{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if strings.TrimSpace(manifest.Name) == "" {
		return SkillManifest{}, fmt.Errorf("%s: skill name is required", path)
	}
	seenTools := map[string]bool{}
	for _, tool := range manifest.Tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			return SkillManifest{}, fmt.Errorf("%s: tool name is required", path)
		}
		if seenTools[name] {
			return SkillManifest{}, fmt.Errorf("%s: duplicate tool %q", path, name)
		}
		seenTools[name] = true
		if strings.TrimSpace(tool.ParametersSchema) != "" && !json.Valid([]byte(tool.ParametersSchema)) {
			return SkillManifest{}, fmt.Errorf("%s: tool %q has invalid parameters_schema", path, name)
		}
	}
	seenServers := map[string]bool{}
	for _, server := range manifest.MCPServers {
		name := strings.TrimSpace(server.Name)
		if name == "" {
			return SkillManifest{}, fmt.Errorf("%s: mcp server name is required", path)
		}
		if seenServers[name] {
			return SkillManifest{}, fmt.Errorf("%s: duplicate mcp server %q", path, name)
		}
		seenServers[name] = true
		if strings.TrimSpace(server.Command) == "" {
			return SkillManifest{}, fmt.Errorf("%s: mcp server %q command is required", path, name)
		}
	}
	return manifest, nil
}
