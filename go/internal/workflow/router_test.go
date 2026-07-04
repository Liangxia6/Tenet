package workflow

import "testing"

func TestRouteFixtures(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		workflow string
	}{
		{"simple answer", "What is Tenet?", "simple"},
		{"simple chinese", "解释一下这个概念", "simple"},
		{"coding bug", "Fix the failing unit test in this Go package", "coding"},
		{"coding chinese", "请修复这个项目里的 bug 并运行测试", "coding"},
		{"coding refactor", "Refactor the API handler and update tests", "coding"},
		{"coding patch", "Implement a patch for the parser error", "coding"},
		{"dag compare", "Compare three architecture options and produce a roadmap", "dag"},
		{"dag plan", "请系统性分析并拆解多个模块的实施方案", "dag"},
		{"dag design", "Design a multi-stage migration plan", "dag"},
		{"scientific research", "Research evidence for this hypothesis and validate it statistically", "scientific"},
		{"scientific chinese", "基于证据和假设做一次实验设计", "scientific"},
		{"scientific paper", "Analyze this paper and build a chain of reasoning", "scientific"},
		{"interactive approval", "Draft a plan but ask me for approval before editing", "interactive"},
		{"interactive chinese", "先让我选择方案，确认后再继续", "interactive"},
		{"react file", "Read file README.md and summarize it", "react"},
		{"react search", "Search the workspace with grep for database usages", "react"},
		{"react tool chinese", "调用工具查找配置文件", "react"},
		{"coding beats dag", "Plan and implement code changes to fix tests", "coding"},
		{"scientific beats dag", "Analyze multiple experiments and hypothesis evidence", "scientific"},
		{"long task", "Analyze the product requirements, split the work into multiple phases, compare risks, plan rollout, identify dependencies, and produce a detailed implementation roadmap with owners and milestones", "dag"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Route(tt.query, "auto")
			if got.Workflow != tt.workflow {
				t.Fatalf("workflow = %s, want %s: %+v", got.Workflow, tt.workflow, got)
			}
			if got.TaskType == "" || got.RiskLevel == "" {
				t.Fatalf("missing metadata: %+v", got)
			}
		})
	}
}

func TestRouteOverride(t *testing.T) {
	got := Route("Fix tests", "simple")
	if got.Workflow != "simple" || got.Reason != "explicit workflow override" {
		t.Fatalf("override route = %+v", got)
	}
}
