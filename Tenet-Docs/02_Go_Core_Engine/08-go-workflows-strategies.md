# Go Workflow Strategies

> 六大 Workflow：Simple · React · DAG · Interactive · Scientific · Coding
>
> 文档状态：DRAFT · 版本：v1.1.0
>
> Go 层编排引擎的策略库。每个 Workflow 是一段确定性 Go 函数——定义任务如何分解、Agent 如何调度、结果如何收敛。所有微观循环在 Go 进程中通过 `for` 循环 + `Decide` 执行，Python 层只做单步 `GenerateThought` / `ExecuteTool`。

---

## 1. 统一 Workflow 契约

```go
type WorkflowFunc func(ctx *WorkflowContext, task *TaskHandle) (result any, err error)
```

所有 Workflow 通过 `WorkflowRegistry.Register(name, fn)` 注册，按名查找。生命周期状态转换：

```
TaskCreated → TaskStarted → [Decide/Record 循环] → TaskCompleted / TaskFailed / TaskSuspended
```

Python 层通过三个 Unary gRPC 调用被 Go 访问：
- `GenerateThought(session_id, model, system_prompt, messages, tools)` → `{thought, tool_calls, is_final, usage}`
- `ExecuteTool(session_id, fencing_token, tool_name, arguments)` → `{stdout, stderr, exit_code}`
- `HealthCheck()` → `{status, worker_count, uptime_seconds}`

---

## 2. Registry

```go
var Registry = map[string]WorkflowFunc{
    "simple":      SimpleWorkflow,
    "react":       ReactWorkflow,
    "dag":         DAGWorkflow,
    "interactive": InteractiveWorkflow,
    "scientific":  ScientificWorkflow,
    "coding":      CodingWorkflow,
}
```

Strategy Router（16）选定 Workflow 类型后，通过 `Registry.Get(name)` 获取函数指针，传给 Scheduler。

---

## 3. SimpleWorkflow

**场景**：复杂度 < 0.3，单 Agent 直接回答。

**Go 侧行为**：
1. `ctx.Record("TaskStarted")` → flushRecords 落盘
2. `ctx.Decide("GenerateThought")` → Go 调 Python GenerateThought → LLM 返回 Thought + is_final=true → **立即落盘** → 返回 result
3. `ctx.Record("TaskCompleted")` → Commit

**特点**：1 次 Decide，无循环，无工具调用，无并发。1 个 Decide 事件 + 2 个 Record 事件。

---

## 4. ReactWorkflow — Go 侧确定性 ReAct 循环

**场景**：复杂度 0.3-0.5，需要多轮工具调用的探索性任务。

### 4.1 完整控制流

ReactWorkflow 是一个 `for` 循环，每轮经历 Thought → Tool → Thought，每步 `Decide` 立即落盘。

**初始化阶段**：
1. `ctx.Record("TaskStarted")` → flushRecords
2. 构建初始 `messages`：`[{system: system_prompt}, {user: user_query}]`
3. 设置 `maxSteps = config.Agent.DefaultMaxSteps`（默认 50）
4. 设置 `noToolCallCount = 0`

**循环体**（每轮）：

**Step A — Generate Thought**：
1. 构造 `GenerateThoughtRequest{SessionID, TaskID, Model, Temperature, SystemPrompt, Messages, Tools}`
2. `ctx.Decide("GenerateThought")` → 闭包内部通过 gRPC 调 Python GenerateThought → 等待返回
3. **Decide 内部 Step 4 立即落盘**：INSERT event_log (type="GenerateThought", payload={thought, tool_calls, usage})
4. 从返回的 `GenerateThoughtResponse` 提取 `thought`, `tool_calls`, `is_final`, `usage`
5. `ctx.Record("TokenUsed", {model, prompt_tokens, completion_tokens, cost_usd})`
6. 构造 assistant Message：`{role: "assistant", content: thought, tool_calls: tool_calls}` → 追加到 messages

**收敛判断**：
1. 如果 `is_final == true` 或 `len(tool_calls) == 0` → `noToolCallCount++`
2. 如果 `noToolCallCount >= config.Agent.ConvergenceNoToolCalls`（默认 3）→ **强制收敛**：`ctx.Record("TaskCompleted", {final_answer: thought, total_steps: step})` → 返回 thought
3. 如果 `len(tool_calls) > 0` → `noToolCallCount = 0`，继续

**Step B — Execute Tools**：
对 `tool_calls` 中的每个 `tc`：

1. **HITL 安全拦截**：检查 `tc.tool_name` 是否在 `config.Safety.RequireApproval` 列表中 → 如果是 → `ctx.Sleep(waiting for human approval)` → 挂起等待人工审批
2. `ctx.Decide("ToolExecuted")` → 闭包内部：
   - 获取当前 Fencing Token：`LockManager.GetCurrentToken(sessionID)`
   - gRPC 调 Python `ExecuteTool(session_id, fencing_token, tc.tool_name, tc.arguments)`
   - Python 校验 Fencing Token → 执行工具 → 返回 stdout/stderr/exit_code
3. **Decide 内部 Step 4 立即落盘**：INSERT event_log (type="ToolExecuted", payload={tool_name, arguments, stdout, stderr, exit_code})
4. 构造 tool Message：`{role: "tool", tool_call_id: tc.call_id, content: stdout_or_error}` → 追加到 messages

**超限处理**：`step > maxSteps` → `ctx.Record("TaskFailed", {error: "exceeded max steps"})`

### 4.2 崩溃恢复保证

ReactWorkflow 的每一步 `Decide` 独立落盘。event_log 中可能有：
```
seq=1: TaskStarted
seq=2: GenerateThought{step=1}
seq=3: ToolExecuted{step=1, tool=read_file}
seq=4: GenerateThought{step=2}
seq=5: ToolExecuted{step=2, tool=shell}    ← 崩溃在这一步执行中
```

重启回放：跳过 seq 1-4（0ms）→ 从 seq=5 的 ToolExecuted 开始首次执行。不是整个 ReactWorkflow 重跑——只重做未完成的那一个工具调用。

### 4.3 MCP 工具发现与注入流程

Python 层初始化 MCP Server 子进程需要冷启动时间（通常 1-3s）。工具 Schema 的完整传播路径：

```
Session 启动
  → Go 加载内置工具定义（read_file, shell, write_file, web_search）
  → Go 加载 Skill 声明的工具（从 Skill YAML frontmatter 提取）
  → Go 发起首次 GenerateThought(tools=[内置+Skill工具])
  → Python 首次调用时启动 MCP 子进程 → 协议握手 → tools/list 发现 MCP 工具
  → GenerateThoughtResponse.discovered_tools = [MCP 工具 schema 列表]
  → Go 收到后写入 event_log: "ToolsDiscovered" 事件
  → 本轮 LLM 响应中没有 MCP 工具调用（因为发送请求时 MCP 工具尚未注入 tools 列表）
  → 下一轮 GenerateThought(tools=[内置+Skill+MCP工具])  → LLM 可以调用 MCP 工具
```

**关键设计点**：
- **首轮不带 MCP 工具是预期行为**：首次 GenerateThought 的参数 tools 不包含 MCP 工具。LLM 的第一次回复是纯文本推理或被限制在已知工具范围内。这避免了「LLM 想调 MCP 工具但 schema 还没就绪」的竞态。
- **ToolsDiscovered 事件持久化**：即使没有产生新的 MCP 工具（Python 启动时已缓存），Response 中 `discovered_tools` 为空即可。事件写入 event_log 后，回放时 Go 从事件中恢复工具列表，确保确定性。
- **MCP 进程崩溃后**：Python 重启子进程 → 下一轮 GenerateThought 重新发现 → `discovered_tools` 再次非空 → Go 写入新的 ToolsDiscovered 事件（替换旧工具列表）。

---

## 5. DAGWorkflow — 有向无环图拓扑调度

**场景**：复杂度 0.3-0.7，可分解为独立子任务的复杂分析。

### 5.1 控制流

1. `TaskDecomposed`：调 Python GenerateThought 分解为子任务列表 + 依赖关系。system prompt 含「请将任务分解为子任务，输出 JSON 数组，每项含 id/agent/task/depends_on」
2. 拓扑排序：纯 Go 逻辑，不涉及外部调用。输出分层列表——每层内子任务无相互依赖
3. 逐层并发执行：每层内通过 `ctx.Async(subtask_id, fn)` 并发启动子任务
4. 子任务内部是 ReactWorkflow（Go 侧 for 循环）
5. 依赖收敛后执行汇总子任务

### 5.2 Data Relay（数据接力棒）

子任务完成后，其产出被注入为下游子任务的 Prompt 上下文：

1. 子任务 A 完成 → ReactWorkflow 返回最终结果（含 `key_findings` 和 `artifacts`）→ 父 DAGWorkflow 调用 `ctx.Record("SubTaskCompleted", {subtask_id, key_findings: ["发现 3 个 CPU 热点", ...], artifacts: ["findings/profiler-cpu.md"]})`
2. 子任务 B 依赖 A → B 启动前，Go 层读取 A 的 `key_findings`，构造注入文本：`"[前置任务 profiler 的结论]\n发现 3 个 CPU 热点..."` → 拼接到 B 的 user_query 之前
3. B 的 messages = `[system_prompt, 注入文本, user_query]`

### 5.3 并发模型

DAG 分解为 N 个子任务，每层 M 个子任务通过 `ctx.Async` 并发。
- 子任务数量上限：50（防 DAG 爆炸）
- 每层内子任务通过 Out-of-Order Buffer 保证事件按注册顺序写入
- 子任务内部是独立的 WorkflowContext（各自有 streamID，共用父 Task 的 sessionID）
- **Workspace 隔离**：每个子任务使用独立的子目录 `workspaces/{session_id}/subtasks/{subtask_id}/`，避免并发读写同一文件导致非确定性。父 Task 的 Data Relay 通过读取子任务完成事件中的 `artifacts` 字段（路径相对子任务 workspace）实现，不直接读取子任务文件系统。
- 子任务完成后，其产出（event payload 中的 `key_findings` 和 `artifacts`）由父 Task 注入下游子任务的 Prompt，数据流走事件溯源而非文件共享

---

## 6. InteractiveWorkflow — 人机协同

**Go 侧行为**（多轮循环）：

1. 每轮开始：`WorkspaceManager.GitCommit(workspace, "tenet: interactive round {n}")` 存盘
2. `ctx.Decide("GenerateThought")` → Agent 产出当前版本
3. 检查 `is_final` 和 `needs_human_review` → 如果都不需要 → TaskCompleted
4. 否则：`ctx.Record("WaitingForHumanInput", {round, commit_hash})` → `ctx.Sleep(config.Interactive.HumanTimeout)` → 抛哨兵错误挂起
5. 人类在 workspace 修改文件 → CLI `tenet task resume {streamID}`
6. Go 的 WorkspaceManager 调 `GitDiff(workspace, commit_hash)` 提取人类修改
7. 将 diff 注入 messages：`"以下是人类的修改：\n```diff\n{diff}\n```\n请基于这些修正继续。"`
8. 回到步骤 1

**超时**：人类未在 `HumanTimeout` 内响应 → Workflow 以当前状态完成（取最后一次 Agent 产出作为结果）。

---

## 7. ScientificWorkflow — 四步 Pattern 链

**Go 侧行为**：
1. `ChainOfThought` → 生成科学假设。system prompt 含「生成本领域 3 个科学假设」。内部多次 `Decide("GenerateThought")`
2. `Debate`（Pro/Con/Judge 三方对抗）→ 在同一 workspace 的共享沙箱中达成共识。Pro 方 prompt：「为以下假设辩护」；Con 方 prompt：「找出论证漏洞」；Judge 方 prompt：「裁决并给出最终方案」。最多 5 轮
3. `TreeOfThoughts`（3 层树状发散）→ 对共识方案预演连锁影响。每层 3 个分支，每分支评分 0-1，低于 0.5 裁剪
4. `Reflection`（安全质检）→ system prompt 含 `safety_rules`（来自 config），LLM 逐条审查。不通过 → 退回步骤 2 重新 Debate

任何一步失败 → TaskFailed。每一步内部的 `Decide` 独立落盘。

---

## 8. CodingWorkflow — 七阶段代码工程

| 阶段 | 操作 | 使用的 RPC | 失败处理 |
|---|---|---|---|
| 1. Design | Architect Agent 分析代码库 → 产出 `PLAN.md` | GenerateThought | 重试 1 次 |
| 2. Snapshot | `WorkspaceManager.GitCommit()` 存盘安全点 | — | — |
| 3. Coding | Coder Agent 并发编写/修改文件 | GenerateThought + ExecuteTool(write_file) | — |
| 4. Static Check | 运行 `config.Coding.StaticCheckCmd`（如 `go vet ./...`） | Go 本地执行（不调 Python） | 失败 → Coder 纠错 → 重试最多 3 次 → `GitResetHard` 回滚到 Phase 2 |
| 5. Unit Test | 运行 `config.Coding.TestCmd`（如 `go test ./...`） | Go 本地执行 | 同上自动纠错闭环 |
| 6. Review | Reviewer Agent 通过 Reflection Pattern 审计代码 Diff | GenerateThought | 不通过 → 退回 Phase 3 |
| 7. Finalize | `WorkspaceManager.GitCommit("tenet: coding completed")` | — | — |

**autoFix 纠错闭环**（Phase 4/5 共用）：
1. 运行检查命令 → 捕获 stderr
2. 检查失败 → 将 stderr 作为新 message 注入：「[Auto-Fix attempt {n}/{max}] 检查失败：{stderr}。请修正代码。」
3. 调 Coder Agent（GenerateThought）→ 修正代码 → 重新运行检查命令
4. 连续 `maxRetries`（默认 3）次失败 → `WorkspaceManager.GitResetHard(safeCommit)` 物理回滚到 Phase 2 快照 → 返回 error

---

## 9. 策略矩阵

| Workflow | 复杂度 | 循环模式 | 每步落盘 | Decide 次数 | Python 调用 | 路由定位 |
|---|---|---|---|---|---|---|
| Simple | < 0.3 | 无 | 1 次 | 1 | GenerateThought ×1 | 顶层路由 |
| React | 内部执行单元 | Go for 循环 | 每步 | N 轮 × (1 + M 工具) | GenerateThought ×N, ExecuteTool ×M | **DAG 的子任务内部**（非顶层路由） |
| DAG | 0.3-0.7 | ctx.Async 并行 | 每子任务 | Σ 子任务 Decide 数 | 同上 × 子任务数 | 顶层路由 |
| Interactive | 任意 | 轮次 for 循环 | 每轮 | N 轮 × 1 | GenerateThought ×N | 顶层路由（工具命中 RequireApproval 时触发） |
| Scientific | > 0.7 | Pattern 链 | 每步 | 4 Pattern × 内部步数 | GenerateThought ×N（不同角色 prompt） | 顶层路由 |
| Coding | 代码任务 | Phase 串行 | 每 Phase | 7 + autoFix 重试 | GenerateThought + ExecuteTool + 本地命令 | 顶层路由（关键词检测触发） |

**关键关系**：ReactWorkflow 在 `WorkflowRegistry` 中注册但**不作为顶层路由目标**——Strategy Router 不会直接选择 ReactWorkflow。React 仅在 DAGWorkflow 内部作为子任务的执行单元被调用。InteractiveWorkflow 是 ReactWorkflow 的变体，差异仅在于工具调用前增加 HITL 挂起点。SimpleWorkflow 是降级链的最终兜底。
