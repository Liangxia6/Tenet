# Go Strategy Router

> 复杂度分析 · Workflow 路由 · Registry · ComplexityAnalyzed 事件
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 1. 路由 Pipeline

Task 提交后 → Strategy Router → 调 Python LLM 做复杂度分析 → 选 Workflow → Scheduler 入队。

分析结果持久化为 `ComplexityAnalyzed` 事件，保证策略选择可追溯。

---

## 2. 复杂度分析

调 `GenerateThought` 做轻量评估——不占 Task 的 token 预算。

**入参**：system prompt 描述评分标准（0.0-0.3 简单问答 / 0.3-1.0 需多步骤），messages 只含用户的原始 query。

**出参**：`complexity_score`（0.0-1.0）+ `reason`（简短理由）。期望 LLM 返回 JSON 格式。

**降级**：分析失败（LLM 不可用、超时、返回非法格式）→ 降级为 `SimpleWorkflow`。

---

## 3. 路由规则

| 条件 | Workflow | 说明 |
|---|---|---|
| 用户显式指定 `--workflow` | 直接使用用户指定的类型 | 覆盖自动路由 |
| 任务涉及代码编写/修改 | `CodingWorkflow` | 优先级最高：关键词命中直接路由，忽略复杂度得分 |
| 任务需要人工审批 | `InteractiveWorkflow` | 次高优先级：策略判定在编码场景之外触发 |
| `complexity_score < 0.3` | `SimpleWorkflow` | 单 Agent 直接回答 |
| `0.3 <= complexity_score < 0.7` | `DAGWorkflow` | 分解为子任务并行执行。DAG 的每个子任务内部使用 ReactWorkflow（Go 侧 for 循环） |
| `complexity_score >= 0.7` | `ScientificWorkflow` | 高复杂度需要 CoT→Debate→ToT→Reflection 推理链 |

**内部委托模型**：
- `DAGWorkflow` 的子任务内部使用 `ReactWorkflow`——React 不是顶层路由目标，而是 DAG 的执行单元。
- `ScientificWorkflow` 内部调用四种 Reasoning Pattern（CoT/ Debate/ ToT/ Reflection），这些 Pattern 分别通过 `GenerateThought` 驱动——Pattern 不是 Workflow，而是 Workflow 内部的推理策略。
- `InteractiveWorkflow` 是 `ReactWorkflow` 的变体——差异仅在 HITL 挂起点（`ctx.Sleep` 等待人工审批），其余循环逻辑相同。
- `CodingWorkflow` 是独立的 Phase 串行流程（Design→Code→Test→Review），不经过 DAG 分解。

---

## 4. Workflow Registry

全局注册表——`map[workflow_name]WorkflowFunc`。启动时注册 6 个内置 Workflow：simple、react、dag、interactive、scientific、coding。

支持用户通过 `tenet.yaml` 的 `workflows.custom` 段注册插件 Workflow（`.so` 或 `.wasm`）——Strategy Router 启动时加载。

---

## 5. 降级链

保证系统在任何情况下不会「不知道该跑什么」：

1. 用户显式指定 → 直接使用
2. LLM 分析 → Simple 或 DAG
3. 分析失败 → 降级 `SimpleWorkflow`
4. 选定的 Workflow 不存在 → 降级 `SimpleWorkflow`
5. `SimpleWorkflow` 也失败 → `TaskFailed`
