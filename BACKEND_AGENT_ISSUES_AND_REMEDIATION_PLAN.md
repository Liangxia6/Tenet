# Tenet 后端与 Agent Runtime 问题清单及修复计划

> Scope: 本文档暂不讨论前端体验，专注 Tenet 后端、Agent runtime、工具系统、Trace、回放、工作区、API、测试与运维。
>
> 当前判断：Tenet 已经具备可运行 MVP 骨架，但距离“稳定可用的 Agent 项目”还有明显差距。主要问题不是单点 bug，而是多个核心系统还停留在“能跑通 demo”阶段：会话语义、真实回放、上下文管理、工具可靠性、工作区快照、任务调度、可观测性、错误恢复都需要系统性补齐。

## 1. 总体结论

当前 Tenet 的强项：

- 有事件日志和 workflow context 基础。
- 有 Go CLI/API、Python worker、gRPC 通信、OpenAI-compatible/DeepSeek adapter。
- 有 ReAct loop、DAG/scientific/coding/interactive workflow 的初步实现。
- 有本地工具执行器、workspace path containment、部分 Git/archive snapshot、fork/lineage。
- 有 Projection Engine、SSE events、基础 task status。

当前 Tenet 的主要短板：

- Task/session 语义混乱，继续对话是后补的，不是 workflow 原生能力。
- Replay 还不是真正的 workflow replay，只是初步事件读取/兼容机制。
- Trace 记录不够结构化，无法完整回答“上下文如何拼接、每次 LLM 输入是什么、工具为什么调用”。
- 工具生态刚起步，工具 schema、错误语义、权限、审计和自动 checkpoint 不够。
- Workflow 很多是“顺序流程 + LLM 调用”，还不是成熟 planner/scheduler/runtime。
- Workspace 回溯只能在有 snapshot 的点工作，不支持任意事件点恢复。
- Timer/resume 只是事件级延迟恢复，还没有真正释放 worker 后从断点继续 workflow。
- API 是 CLI 逻辑外包了一层 HTTP，没有形成稳定产品级 contract。
- 测试覆盖多为 happy path，缺少真实模型、真实工具链、故障、并发、恢复测试。

## 2. 优先级定义

- P0: 不修会阻碍“可用 Agent”的核心问题。
- P1: 不修会影响可靠性、回溯、复杂任务能力。
- P2: 产品化、性能、扩展性、体验增强。

## 3. P0 问题清单

### P0-01. Task 与 Session 语义不统一

问题：

- 当前 `task run` 更像一次性 job。
- HTTP `POST /tasks/{id}/messages` 是后补 continuation API，不是 workflow runtime 的一等概念。
- 同一个 stream 中多次 `TaskCompleted` 会让 Projection、Replay、状态语义变得模糊。

影响：

- 用户期望一个 task 像 ChatGPT session，可以持续对话。
- 但后端模型仍偏向“一次执行一次完成”。
- 多轮对话、上下文恢复、状态展示、fork/replay 都会被多次 completion 搞复杂。

根因：

- 数据模型只有 event stream，没有明确 Session / Turn / Run 三层结构。
- Workflow execution 没有 turn id / run id。

修复方案：

1. 引入三层概念：
   - Session: 用户长期对话或任务容器。
   - Turn: 用户一次输入。
   - Run: Agent 对该 turn 的一次执行。
2. 事件 payload 必须包含：
   - `session_id`
   - `turn_id`
   - `run_id`
3. 将现有 `stream_id` 保留为 session stream。
4. 将 `TaskCompleted` 改为：
   - `RunCompleted`
   - `SessionUpdated`
5. Projection 同时输出：
   - session status
   - current run status
   - turns list

验收标准：

- 同一个 session 连续发送 3 条消息，Projection 能显示 3 个 turn。
- 每个 turn 有独立 LLM/tool/tokens/final answer。
- session 不因为某个 turn completed 就失去继续执行能力。

### P0-02. Replay 不是真正回放

问题：

- `task replay` 当前主要是读取并打印事件，不能证明 workflow determinism。
- `WorkflowContext.Decide` 有 replay 能力，但 CLI/API 没有真正用它执行 workflow replay。
- replay 没有校验所有历史事件都被消费。

影响：

- “可回溯、可重放”目前不可信。
- 代码改动后旧任务是否还能 replay 无法验证。
- Agent 运行失败后无法稳定恢复。

根因：

- replay 缺少独立 harness。
- 没有外部调用隔离断言。
- 没有 event fixture regression。

修复方案：

1. 实现 `workflow.Replay(ctx, store, registry, task)`。
2. Replay 模式必须断言：
   - zero new events
   - zero external LLM calls
   - zero external tool calls
   - zero unconsumed historical decisions
3. `task replay` 调用真实 workflow replay。
4. 增加 replay fixture 测试。

验收标准：

- 对 simple/react/coding 的历史事件流 replay 成功。
- 如果 workflow 代码多记录一个事件，replay 必须失败。
- 如果 replay 尝试调用真实 worker，测试必须失败。

### P0-03. Trace 缺少结构化 LLM Call 事件

问题：

- 现在主要记录 `GenerateThought` decision payload 和 `TokenUsed`。
- 没有 `LLMCallStarted / LLMCallCompleted / LLMCallFailed`。
- 没有记录完整 request metadata：
  - provider
  - model
  - system prompt hash
  - message count
  - input chars/tokens estimate
  - tools count
  - latency
  - retry count

影响：

- 用户无法准确知道“调用了几次模型、每次输入了什么、上下文多大、为什么调用工具”。
- 成本和性能分析不可靠。
- Debug 模型错误困难。

根因：

- `Decide("GenerateThought")` 同时承担“决策缓存”和“LLM 调用记录”两个职责。
- Token 事件和 LLM 事件是分散的。

修复方案：

1. 新增结构化事件：
   - `LLMCallStarted`
   - `LLMCallCompleted`
   - `LLMCallFailed`
2. 每次 LLM 调用生成 `call_id`。
3. `GenerateThought` 保留为 deterministic decision 事件，但不要替代 Trace。
4. Token usage 关联 `call_id`。

验收标准：

- 任意任务能查询 `llm_calls` 列表。
- 每个 call 显示 model/provider/messages/tools/token/latency。
- 前端/CLI 能准确展示 LLM 调用次数。

### P0-04. Context 拼接没有一等实现

问题：

- 当前 messages 拼接散落在 workflow 中。
- 没有统一 ContextAssembler。
- 没有 token budget 下的裁剪、摘要、检索策略。
- 多轮 session continuation 只粗略从事件里恢复 user/assistant messages。

影响：

- 长任务会超上下文。
- Agent 容易丢历史。
- 无法回答“上下文拼接了多少、哪些事件被注入、哪些被省略”。

根因：

- Memory 与 prompt assembly 尚未模块化。

修复方案：

1. 新建 `internal/context/assembler`。
2. 输入：
   - system prompt
   - current user turn
   - previous turns
   - recent tool results
   - task summary
   - workspace summary
3. 输出：
   - messages
   - included event refs
   - omitted event refs
   - estimated tokens
4. 记录事件：
   - `ContextAssembled`
   - `ContextCompacted`

验收标准：

- 每次 LLM call 前都有 `ContextAssembled`。
- Trace 能显示上下文来源和 token estimate。
- 超预算时自动 compact，而不是失败或无限增长。

### P0-05. 工具调用协议不够严格

问题：

- 工具执行结果只有 stdout/stderr/exit_code。
- 没有统一错误码。
- 没有 tool call id 的完整关联。
- 没有参数 schema validation。
- 工具权限和审批没有真正执行。

影响：

- 模型调用工具失败时难以自我修复。
- 工具错误无法分类统计。
- 对危险工具缺乏产品级安全边界。

根因：

- 工具只是本地 executor function map，没有完整 ToolRuntime。

修复方案：

1. 新建 ToolRuntime：
   - schema validate
   - permission check
   - rate limit
   - timeout
   - audit
   - checkpoint
2. 统一工具错误：
   - `INVALID_ARGS`
   - `PERMISSION_DENIED`
   - `PATH_ESCAPE`
   - `TIMEOUT`
   - `EXEC_FAILED`
   - `NETWORK_FAILED`
3. 事件：
   - `ToolCallStarted`
   - `ToolCallCompleted`
   - `ToolCallFailed`

验收标准：

- 工具失败时模型能看到结构化错误。
- CLI/API 能按工具名、错误码统计。
- 危险工具可配置禁用或审批。

### P0-06. 工具生态仍不足

问题：

- 虽然已经补了 `list_dir/search_files/grep/replace/git_diff/http_fetch` 等基础工具，但仍缺少重要能力：
  - `apply_patch`
  - `git_log`
  - `git_show`
  - `git_branch`
  - `workspace_snapshot`
  - `workspace_restore`
  - `symbol_search`
  - `code_outline`
  - `test_runner`
  - `package_manager`
  - `browser_fetch`
  - `sqlite_query`

影响：

- Agent 做复杂代码任务会频繁退化到 shell。
- Shell 过多会降低可控性和 Trace 质量。

修复方案：

1. 第二批工具：
   - `apply_patch`
   - `git_log`
   - `git_show`
   - `git_branch`
   - `workspace_snapshot`
   - `run_tests`
2. 第三批工具：
   - `symbol_search`
   - `code_outline`
   - `sqlite_query`
   - `browser_fetch`
3. 每个工具必须 Go/Python 双实现或明确只支持一端。

验收标准：

- coding workflow 默认不直接依赖 shell 做所有事情。
- 关键文件改动通过 `apply_patch` 或结构化 edit 工具完成。

### P0-07. Workflow 状态机不严格

问题：

- Workflow 事件是自由字符串，缺少状态机约束。
- 同一 stream 可以出现多个 `TaskStarted/TaskCompleted`。
- Failure/cancel/resume 和 completed 的关系不清晰。

影响：

- Projection 很容易显示错误状态。
- Replay/fork/continue 的行为不稳定。

根因：

- 没有显式状态 transition table。

修复方案：

1. 定义 state machine：
   - Session: `OPEN / ARCHIVED`
   - Run: `QUEUED / RUNNING / PAUSED / COMPLETED / FAILED / CANCELLED`
2. 事件 append 前验证 transition。
3. Projection 按 run_id 聚合。

验收标准：

- Completed run 后可以开始新 run，但不是把 completed 改回 running。
- Cancel 只影响当前 run。
- 状态非法转换写入失败。

### P0-08. Token budget 只投影不强制

问题：

- Projection 能显示 budget exceeded。
- 但 workflow 没有在超预算时真正停止。

影响：

- 用户设置 token budget 没有实际保护。
- 成本控制不可靠。

修复方案：

1. 在每次 `recordUsage` 后检查 budget。
2. 超预算写入 `BudgetExceeded` 和 `TaskFailed/RunFailed`。
3. Worker call 前也做剩余额度检查。

验收标准：

- 设置极小 budget 时任务必须停止。
- Trace 显示预算耗尽原因。

## 4. P1 问题清单

### P1-01. Timer/Suspend/Resume 不是真正断点续跑

问题：

- `task resume --after` 主要是写事件。
- Workflow 没有真正 suspend 后释放 worker，再从断点恢复执行。

影响：

- 长任务、人工审批、等待外部事件场景不可用。

修复方案：

1. 引入 durable command model：
   - `SleepCommand`
   - `WaitForHumanCommand`
   - `ResumeCommand`
2. Scheduler 识别 paused run，不占用 worker。
3. Timer fired 后重新调度 replay + continue。

验收标准：

- interactive workflow 等待期间 worker pool active 数为 0。
- timer fired 后自动恢复并完成。

### P1-02. Workspace 回溯不是任意事件点恢复

问题：

- 只有手动 snapshot 或少数 snapshot 点能恢复文件状态。
- `write_file/replace/shell` 不会自动 checkpoint。
- Shell 可能修改大量文件，系统不知道改了什么。

影响：

- 用户想回到“三个提示词前”的代码状态，通常做不到。

修复方案：

1. 对所有写操作前后自动 checkpoint，可配置。
2. 工具事件记录 touched files。
3. Shell 执行前后运行 git diff 或 filesystem scan。
4. 引入 `WorkspaceCheckpointCreated`。

验收标准：

- 每个 turn 完成后都有 workspace checkpoint。
- fork 到任意 turn 可以恢复对应文件状态。

### P1-03. Strategy Router 太弱

问题：

- 当前 route 主要靠简单规则/分数。
- 难任务不能稳定自动匹配 DAG/coding/scientific。

影响：

- 用户不手动指定 workflow 时，复杂任务可能走 simple/react。

修复方案：

1. 加 LLM-assisted classify。
2. 输出结构：
   - task_type
   - complexity
   - recommended_workflow
   - required_tools
   - risk_level
3. 保留手动 override。

验收标准：

- 给 20 个 fixture prompt，workflow 选择符合预期。

### P1-04. Coding workflow 还不是可靠代码 Agent

问题：

- coding workflow 是 design/coding/check/review 的顺序调用。
- 没有真实 patch planning。
- 没有失败后 auto-fix loop。
- 没有基于 test failure 的 targeted repair。

影响：

- 做真实项目修改时质量不稳定。

修复方案：

1. Coding workflow 拆成：
   - inspect
   - plan
   - edit
   - test
   - fix
   - review
   - summarize
2. 每步都有事件和工具约束。
3. test 失败最多自动修复 N 轮。

验收标准：

- fixture repo 中修复一个 failing test。
- Trace 中能看到 inspect/edit/test/fix/review。

### P1-05. DAG workflow 没有真正并发和依赖调度

问题：

- DAG workflow 目前更像顺序执行子任务。
- 没有 worker pool 并行执行 subtask。
- dependency resolution 基础。

影响：

- 复杂任务无法有效拆解并行。

修复方案：

1. 实现 DAG executor。
2. ready queue + dependency graph。
3. subtask 有独立 run_id 和 agent role。
4. 支持失败策略：
   - fail-fast
   - continue-on-error
   - retry

验收标准：

- 两个无依赖子任务可并行。
- 有依赖子任务严格等待上游完成。

### P1-06. Python/Go 工具定义可能漂移

问题：

- Go `BuiltinToolDefinitions` 和 Python `ToolRegistry.definitions` 分别维护。
- 容易出现同名工具 schema 不一致。

影响：

- Go 传给模型的 schema 与 Python worker 实际支持不一致。

修复方案：

1. 建立单一工具 manifest：
   - `tools/builtin_tools.json`
2. Go/Python 均从 manifest 加载 definition。
3. 实现端只注册 handler。

验收标准：

- 测试校验 Go/Python 工具名集合一致。
- schema snapshot 测试。

### P1-07. HTTP API 不是稳定产品 API

问题：

- 现在 HTTP API 写在 `cmd/tenet/http_api.go`，更像 CLI 适配层。
- 错误响应是 plain text。
- 没有 API version。
- 没有 OpenAPI spec。

影响：

- 前端和第三方集成会频繁破。

修复方案：

1. 移入 `internal/api`。
2. 路由版本化：`/api/v1/...`
3. 统一响应：
   - `{data, error}`
4. 统一错误码。
5. 生成 OpenAPI 文档。

验收标准：

- API contract 测试覆盖所有路由。
- 错误响应可被前端稳定解析。

### P1-08. Event schema 没有版本管理

问题：

- 事件 payload 是自由 map。
- 没有 event schema version。
- 改 payload 会破坏 Projection/Replay。

修复方案：

1. 每个事件 payload 加 `schema_version`。
2. 为核心事件定义 Go struct。
3. 增加 migration/compat adapter。

验收标准：

- 旧事件 fixture 在新版本 projection 下仍可读。

### P1-09. Worker registry 与 worker lifecycle 不完整

问题：

- Python worker 可 RegisterAgent，但调度没有真正利用动态 registry。
- Go task run 使用显式 worker address。

影响：

- 多 worker、高并发、worker 下线场景不可用。

修复方案：

1. Worker registry 持久化。
2. Worker heartbeat。
3. Scheduler 根据 capability 选择 worker。
4. Worker failure 自动 retry/mark unhealthy。

验收标准：

- 启动两个 worker 后，任务能被分配。
- worker down 后不再分配新任务。

## 5. P2 问题清单

### P2-01. 缺少长期记忆

问题：

- 目前记忆是 event history。
- 没有 project memory、summary memory、embedding/search memory。

修复方案：

1. `SessionSummaryCreated`
2. `WorkspaceSummaryCreated`
3. SQLite FTS 先行，后续再加 vector store。

### P2-02. Web search 仍是 TODO stub

问题：

- `web_search` 当前返回 TODO。

修复方案：

1. 明确 provider：
   - Brave
   - Tavily
   - Bing
   - local disabled
2. 无 key 时不暴露 web_search 工具，避免模型误用。

### P2-03. MCP/Skill 集成缺失

问题：

- 工具生态目前是内置工具。

修复方案：

1. MCP server registry。
2. Tool discovery。
3. Skill manifest。

### P2-04. 数据库迁移机制太弱

问题：

- schema 初始化直接 `CREATE TABLE IF NOT EXISTS`。
- 没有版本化 migration 执行器。

修复方案：

1. `schema_migrations` 真正记录版本。
2. 每次 schema change 新 migration。
3. migration test。

### P2-05. 测试缺少真实端到端矩阵

问题：

- 有 unit/integration，但缺少完整矩阵：
  - Go local echo
  - Go OpenAI-compatible mock
  - Go -> Python gRPC
  - DeepSeek mock
  - tool failure
  - replay
  - fork restore
  - concurrent scheduler

修复方案：

1. 新增 `scripts/e2e/`。
2. 建 mock OpenAI server。
3. CI 跑无外部 key 的全部 e2e。

### P2-06. 安全边界仍不足

问题：

- shell 工具仍很强。
- HTTP fetch 可访问内网 URL。
- 没有 secret redaction。

修复方案：

1. SSRF 防护。
2. secret redaction。
3. tool allowlist。
4. shell 默认 approval。

## 6. 推荐修复路线图

### Phase 1: 稳住 Agent 核心语义

目标：让 Task 成为真正 Session，Trace/LLM/tool/run 结构清楚。

任务：

1. 引入 Session/Turn/Run 数据模型。
2. 重命名或兼容事件：
   - `RunStarted`
   - `RunCompleted`
   - `TurnCreated`
3. Projection 按 run/turn 聚合。
4. `POST /tasks/{id}/messages` 改为创建新 turn + run。

验收：

- 一个 session 三轮对话，状态和输出都清楚。

### Phase 2: 结构化 Trace

目标：完整记录 LLM/tool/context。

任务：

1. `LLMCallStarted/Completed/Failed`
2. `ToolCallStarted/Completed/Failed`
3. `ContextAssembled`
4. Token usage 关联 call_id。

验收：

- CLI/API 能输出完整 Trace report。

### Phase 3: 真 Replay

目标：回放可信。

任务：

1. 实现 workflow replay harness。
2. zero external calls。
3. zero new events。
4. unconsumed history detection。

验收：

- replay fixture 测试覆盖 simple/react/coding。

### Phase 4: 工具运行时升级

目标：工具强且可控。

任务：

1. Tool manifest 单一来源。
2. schema validation。
3. permission/rate/timeout/audit。
4. 自动 workspace checkpoint。
5. 第二批工具。

验收：

- 工具失败有结构化错误。
- 每轮修改后可恢复文件状态。

### Phase 5: Workspace 任意 turn 回溯

目标：能回到“三个提示词之前”的状态。

任务：

1. turn-level checkpoint。
2. touched files tracking。
3. fork from turn。
4. restore from checkpoint。

验收：

- 对第 5 轮 session，可 fork 到第 2 轮，并恢复当时文件。

### Phase 6: Workflow 深化

目标：难任务自动走复杂流程。

任务：

1. LLM strategy router。
2. Coding auto-fix loop。
3. DAG executor 并行调度。
4. Scientific workflow 结构化证据/反思。

验收：

- 复杂 coding fixture 能自动 inspect/edit/test/fix。

### Phase 7: 产品级 API 与测试

目标：前后端/第三方可稳定集成。

任务：

1. `/api/v1`
2. OpenAPI
3. error envelope
4. e2e matrix
5. migration system

验收：

- CI 无外部 key 完整通过。

## 7. 建议立即开始的 10 个任务

1. 定义 Session/Turn/Run event schema。
2. 修改 continuation API，写入 `TurnCreated/RunStarted/RunCompleted`。
3. 新增 `LLMCallStarted/Completed`。
4. 新增 `ToolCallStarted/Completed/Failed`。
5. 实现 `ContextAssembler`。
6. 实现真实 `task replay`。
7. 实现 token budget 强制中断。
8. 将工具定义迁移到 manifest。
9. 增加每个 turn 后 workspace checkpoint。
10. 建立 e2e smoke matrix。

## 8. 风险提示

最大风险不是某个工具没实现，而是核心抽象如果继续混用：

- task = session?
- task = run?
- stream = replay source?
- completed 后是否还能继续？

这些不明确会让后续所有能力越来越难补。因此下一轮最应该优先修的是数据模型和事件语义，而不是继续堆工具或前端。

## 9. 完成标准

当以下能力都满足时，Tenet 才能从 MVP 进入“可用 Agent 项目”：

- 一个 session 可多轮对话，每轮有独立 run。
- 每次 LLM/tool/context 都结构化可追踪。
- replay 能真实验证 workflow determinism。
- 每轮代码修改都有 checkpoint，可 fork/restore。
- 工具有 schema validation、权限、错误码、审计。
- workflow 能按复杂度自动选择并执行。
- budget、timeout、cancel、resume 都有强语义。
- API 有稳定 contract 和 e2e 测试。
