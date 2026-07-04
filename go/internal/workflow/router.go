package workflow

import "strings"

type RouteResult struct {
	Workflow        string
	ComplexityScore float64
	Reason          string
	TaskType        string
	RequiredTools   []string
	RiskLevel       string
}

func Route(query, override string) RouteResult {
	override = strings.ToLower(strings.TrimSpace(override))
	if override != "" && override != "auto" {
		return RouteResult{Workflow: override, ComplexityScore: 0, Reason: "explicit workflow override", TaskType: override, RiskLevel: "manual"}
	}
	lower := strings.ToLower(query)
	scores := map[string]float64{
		"simple":      0.2,
		"react":       0,
		"coding":      0,
		"dag":         0,
		"scientific":  0,
		"interactive": 0,
	}
	requiredTools := []string{}
	if containsAny(lower, "code", "代码", "实现", "修复", "bug", "test", "测试", "refactor", "重构", "patch", "failing", "单元测试", "编译") {
		scores["coding"] += 0.65
		requiredTools = append(requiredTools, "read_file", "grep", "apply_patch", "run_tests")
	}
	if containsAny(lower, "compare", "分析", "design", "架构", "plan", "规划", "multi", "多个", "拆解", "roadmap", "方案", "系统性") {
		scores["dag"] += 0.55
	}
	if containsAny(lower, "research", "scientific", "实验", "假设", "推理链", "证据", "论文", "统计", "验证", "hypothesis", "evidence", "paper", "chain of reasoning") {
		scores["scientific"] += 0.75
		requiredTools = append(requiredTools, "web_search", "http_fetch")
	}
	if containsAny(lower, "ask me", "confirm", "approval", "human", "交互", "确认", "审批", "让我选择", "需要我") {
		scores["interactive"] += 0.7
	}
	if containsAny(lower, "read file", "读取文件", "search", "查找", "grep", "fetch", "调用工具", "tool", "工具", "数据库", "sqlite") {
		scores["react"] += 0.5
		requiredTools = append(requiredTools, "read_file", "grep")
	}
	if len(strings.Fields(lower)) > 40 {
		scores["dag"] += 0.15
	}
	workflow := "simple"
	best := scores["simple"]
	for _, candidate := range []string{"coding", "scientific", "interactive", "dag", "react"} {
		if scores[candidate] > best {
			workflow = candidate
			best = scores[candidate]
		}
	}
	reason := workflow + " score selected"
	if workflow == "simple" {
		reason = "simple query fallback"
	}
	return RouteResult{
		Workflow:        workflow,
		ComplexityScore: clampScore(best),
		Reason:          reason,
		TaskType:        workflow,
		RequiredTools:   uniqueStrings(requiredTools),
		RiskLevel:       riskLevel(workflow, best),
	}
}

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}

func clampScore(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func riskLevel(workflow string, score float64) string {
	switch {
	case workflow == "coding" || workflow == "scientific" || score >= 0.75:
		return "high"
	case workflow == "dag" || workflow == "interactive" || score >= 0.5:
		return "medium"
	default:
		return "low"
	}
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
