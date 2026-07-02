package workflow

import "strings"

type RouteResult struct {
	Workflow        string
	ComplexityScore float64
	Reason          string
}

func Route(query, override string) RouteResult {
	override = strings.ToLower(strings.TrimSpace(override))
	if override != "" && override != "auto" {
		return RouteResult{Workflow: override, ComplexityScore: 0, Reason: "explicit workflow override"}
	}
	lower := strings.ToLower(query)
	if containsAny(lower, "code", "代码", "实现", "修复", "bug", "test", "测试", "refactor", "重构") {
		return RouteResult{Workflow: "coding", ComplexityScore: 0.6, Reason: "coding keywords detected"}
	}
	if containsAny(lower, "compare", "分析", "design", "架构", "plan", "multi", "多个") {
		return RouteResult{Workflow: "dag", ComplexityScore: 0.5, Reason: "multi-step analysis keywords detected"}
	}
	if containsAny(lower, "research", "scientific", "实验", "假设", "推理链") {
		return RouteResult{Workflow: "scientific", ComplexityScore: 0.8, Reason: "high-complexity reasoning keywords detected"}
	}
	return RouteResult{Workflow: "simple", ComplexityScore: 0.2, Reason: "simple query fallback"}
}

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
