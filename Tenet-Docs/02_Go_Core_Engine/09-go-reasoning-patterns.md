# Go Reasoning Patterns

> CoT · Debate · ToT · Reflection — Go 侧确定性推理模式
>
> 文档状态：DRAFT · 版本：v1.1.0
>
> 所有推理模式 100% 在 Go 进程中通过 `for` 循环 + 多次 `Decide("GenerateThought")` 执行。Python 层不感知 Pattern——它只看到不同 SystemPrompt 和 Messages 的独立 GenerateThought 调用。Pattern 可组合——ScientificWorkflow = CoT → Debate → ToT → Reflection。

---

## 1. Pattern 统一接口与注册

```go
type PatternFunc func(ctx *WorkflowContext, task *TaskHandle, params any) (any, error)

var PatternRegistry = map[string]PatternFunc{
    "chain_of_thought": ChainOfThought,
    "debate":           Debate,
    "tree_of_thoughts": TreeOfThoughts,
    "reflection":       Reflection,
}
```

每个 Pattern 内部通过多次 `GenerateThought` + `ExecuteTool` 实现推理算法。每次 `Decide` 独立落盘——崩溃后精确恢复到最后一轮的最后一步。

---

## 2. Chain-of-Thought（CoT）

**目的**：引导 LLM 分步推理，每步的中间结论显式输出。

**特化参数**：`MaxSteps int`（默认 5），`StepPrefix string`（默认 "Step {n}:"）

**Go 侧行为**：

1. 构造 CoT 专用的 system prompt：「请按步骤推理。每步以 Step N: 开头。如果已得出最终结论，输出 FINAL: <答案>。」
2. 维护 `allSteps []string` 收集每步输出
3. for 循环（最多 MaxSteps 轮）：
   a. 在 messages 末尾追加 user 消息 `StepPrefix`（如 "Step 3:"）
   b. `ctx.Decide("GenerateThought")` → Go 调 Python GenerateThought(messages, tools=nil) → LLM 返回推理文本 → **立即落盘**
   c. 将 LLM 输出追加到 `allSteps` 和 messages（role=assistant）
   d. 检查输出是否含 "FINAL:" → 是则 break
   e. 如果 `is_final=true` → break
4. 返回 `allSteps`

**Python 层感知**：每次 GenerateThought 看到不同的 messages（Go 逐轮追加了新消息）。不知道这是 CoT 的第几步。

---

## 3. Debate Pattern

**目的**：通过多方对抗（支持/质疑/裁判）在共享上下文沙箱中达成共识。

**特化参数**：`ProRole` / `ConRole` / `JudgeRole`（角色名），`MaxRounds int`（默认 5），`Topic string`

**Go 侧行为**：

1. 初始化 `sharedContext string = "命题: {Topic}\n"`
2. for 循环（最多 MaxRounds 轮）：
   a. **Pro 方**：`ctx.Decide("GenerateThought")` → system_prompt = "你是{ProRole}。为以下命题辩护。\n{sharedContext}" → LLM 输出辩护论点 → **立即落盘** → 追加到 sharedContext
   b. **Con 方**：`ctx.Decide("GenerateThought")` → system_prompt = "你是{ConRole}。找出论证漏洞。\n{sharedContext}" → LLM 输出质疑论点 → **立即落盘** → 追加到 sharedContext
   c. **每 2 轮或最后一轮**：`ctx.Decide("GenerateThought")` → system_prompt = "你是{JudgeRole}。审阅辩论记录并裁决。\n{sharedContext}" → LLM 输出裁决 → **立即落盘** → 如果 `is_final=true`，返回裁决
3. 返回 sharedContext（未收敛时的完整辩论记录）

**Pro/Con/Judge 是同一个 GenerateThought RPC 的三次不同调用**——Python 不知道这三者是同一个辩论的不同角色。区别只在 SystemPrompt 参数。

---

## 4. Tree-of-Thoughts（ToT）

**目的**：多路径探索 + 评估剪枝，以 token 代价换推理质量。

**特化参数**：`MaxDepth int`（默认 3），`BranchFactor int`（默认 3），`ScoreThreshold float64`（默认 0.5），`RootPrompt string`

**Go 侧行为**（BFS 搜索树，Go 内存中维护）：

1. 创建根节点：`root = {Thought: RootPrompt}`
2. BFS 队列：`queue = [root]`
3. for 循环（每层 depth=1..MaxDepth）：
   a. 对 queue 中每个 node：
      - **生成候选**：`ctx.Decide("GenerateThought")` → system_prompt = "基于以下思路生成{BranchFactor}个候选推演方向。输出 JSON 数组。\n当前思路: {node.Thought}" → **立即落盘** → 解析候选列表
      - **评估每个候选**：`ctx.Decide("GenerateThought")` → system_prompt = "评估此推演方向的质量和风险，打分 0.0-1.0。\n推演方向: {candidate}\n评估标准: {EvaluationPrompt}" → **立即落盘** → 解析分数
      - 创建子节点：score >= ScoreThreshold → 加入下一层队列。score < ScoreThreshold → 丢弃（剪枝）
   b. `queue = 下一层队列`
   c. queue 为空 → 提前退出（所有分支被剪）
4. 返回完整搜索树根节点

**搜索树状态完全由 Go 持有**——每步生成和评估是独立的 `Decide` → Immediate Commit 落盘，崩溃后精确恢复。

---

## 5. Reflection Pattern

**目的**：产出 → 自评 → 循环改进。从错误中学习。

**特化参数**：`GeneratePrompt string`，`SafetyRules []string`，`MaxIterations int`（默认 3）

**Go 侧行为**：

1. for 循环（最多 MaxIterations 轮）：
   a. **产出**：`ctx.Decide("GenerateThought")` → system_prompt = GeneratePrompt → LLM 产出当前版本 → **立即落盘**
   b. **自评**：`ctx.Decide("GenerateThought")` → system_prompt = "严格审查以下产出。如果有缺陷，指出改进建议。如果无缺陷，回复 PASS。\n产出: {output}\n审查标准:\n- {safety_rules[0]}\n- {safety_rules[1]}..." → **立即落盘**
   c. 自评返回 "PASS" 或 `is_final=true` → 返回产出
   d. 自评返回缺陷 → 将改进建议注入 messages：`{role: "user", content: "改进建议: {critique}。请修正。"}` → 回到步骤 a
2. 未收敛 → 返回最后一次产出 + fmt.Errorf("reflection did not converge")

**失败经验存储**：Reflection 循环结束后，Go 将「初始产出 + 失败原因 + 最终修正方案」写入 Qdrant Episodic Memory（如 Qdrant 可用）。后续相似任务在 GenerateThought 前检索相关经验，注入 system prompt。

---

## 6. Pattern 组合

```go
func ScientificWorkflow(ctx *WorkflowContext, task *TaskHandle) (any, error) {
    hypothesis, _ := ChainOfThought(ctx, task, CoTParams{...})
    consensus, _ := Debate(ctx, task, DebateParams{Topic: hypothesis})
    riskAnalysis, _ := TreeOfThoughts(ctx, task, ToTParams{RootPrompt: consensus})
    passed, _ := Reflection(ctx, task, ReflectionParams{Proposal: riskAnalysis})
    // ...
}
```

每个 Pattern 是独立的 Go 函数——内部每步 `Decide` 独立落盘。组合后的 ScientificWorkflow 具备像素级灾备——崩溃后从最后落盘的 GenerateThought 事件精确恢复。
