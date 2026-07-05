# Tenet 项目完整学习指南

本文面向第一次系统阅读 Tenet 的开发者。目标不是只告诉你“怎么跑”，而是帮你理解这个项目为什么这样设计、一次 Agent 任务如何从用户输入走到 LLM、工具、事件日志、Trace、回放和前端状态。

## 1. 一句话理解 Tenet

Tenet 是一个事件驱动的 Agent Runtime。

它把 Agent 拆成三层：

- Agent 能力层：规划、记忆、工具调用、工作流。
- 工程可靠层：事件日志、可追踪 Trace、Replay、锁、预算、快照、恢复。
- 产品接口层：CLI、HTTP API、OpenAPI、前端、Python Worker/gRPC。

如果用公式表示：

```text
Tenet = Session/Run 语义 + Workflow Runtime + Event Sourcing + Tool Runtime + Projection/API
Agent = 规划 + 记忆 + 工具调用
可用工程 = 可回溯 + 可观察 + 可恢复 + 可测试
```

## 2. 推荐先建立的核心概念

### 2.1 Session / Turn / Run

Tenet 不再把 task 当成一次性 job，而是拆成：

- Session：用户长期会话，类似 ChatGPT 左侧的一个会话。
- Turn：用户的一次输入。
- Run：Agent 针对某个 Turn 的一次执行。

同一个 Session 可以有多个 Turn，每个 Turn 可以有自己的 Run。事件 payload 中通常会看到：

```json
{
  "session_id": "...",
  "turn_id": "...",
  "run_id": "..."
}
```

阅读代码时要记住：

- `stream_id` 基本等于 session event stream。
- `RunStarted/RunCompleted/RunFailed/RunPaused` 描述一次执行。
- `TurnCreated` 描述用户输入。
- Projection 会把这些事件聚合成前端能看的状态。

### 2.2 Event Sourcing

Tenet 的事实来源不是某张 `tasks` 表，而是事件日志。

典型事件包括：

- `SessionCreated`
- `TurnCreated`
- `RunStarted`
- `ContextAssembled`
- `LLMCallStarted`
- `GenerateThought`
- `LLMCallCompleted`
- `ToolCallStarted`
- `ToolExecuted`
- `ToolCallCompleted`
- `TaskCompleted`
- `RunCompleted`

事件是 append-only 的。也就是说，系统倾向于“追加事实”，而不是原地覆盖状态。

### 2.3 Projection

事件日志适合回放和审计，但不适合前端直接展示。所以 Tenet 有 Projection Engine，把事件变成状态视图：

- 当前任务状态。
- turns / runs 列表。
- LLM 调用列表。
- 工具调用列表。
- context 拼接记录。
- token 使用。
- timeline。

你可以把 Projection 理解成：

```text
Event Log -> Projection -> API/Frontend View
```

### 2.4 Replay

Replay 是 Tenet 的可靠性核心。

正常执行时：

```text
workflow -> Decide("GenerateThought") -> 真调用 LLM -> 写入事件
workflow -> Decide("ToolExecuted") -> 真调用工具 -> 写入事件
```

Replay 时：

```text
workflow -> Decide("GenerateThought") -> 从历史事件取结果，不调用 LLM
workflow -> Decide("ToolExecuted") -> 从历史事件取结果，不调用工具
```

Replay 的目标是验证：

- workflow 是否确定性。
- 历史事件是否能完整消费。
- 是否偷偷发生了新的外部调用。
- 是否追加了新事件。

## 3. 仓库结构

```text
Tenet/
  go/
    cmd/tenet/                 CLI 与 HTTP API 入口
    internal/workflow/          Agent workflow runtime
    internal/storage/           SQLite event store
    internal/projection/        事件投影状态视图
    internal/worker/            Go 侧 LLM client 与本地工具
    internal/gateway/           gRPC client/server 与 worker registry
    internal/context/assembler/ 上下文拼接
    internal/workspace/         workspace 快照、恢复、fork
    internal/guard/             lock、fencing token、rate limit、budget
    internal/scheduler/         任务调度
    internal/timer/             timer suspend/resume
    internal/skills/            Skill/MCP manifest discovery
  python/
    tenet/worker.py             Python Worker 核心
    tenet/grpc_worker.py        Python gRPC Worker 服务
    tenet/adapters.py           Echo/OpenAI/DeepSeek adapter
    tenet/tools.py              Python 工具执行器
  tools/
    builtin_tools.json          Go/Python 共享工具 manifest
  scripts/e2e/
    no_key_smoke.sh             无外部 key 的端到端 smoke
    mock_openai_server.py       OpenAI-compatible mock server
  frontend/
    src/                        前端源码
  Tenet-Docs/
    ...                         原始设计文档
```

## 4. 一次任务的完整调用链

以 CLI 为例：

```bash
cd go
go run ./cmd/tenet task run \
  --config ../config/tenet.local.yaml \
  --worker echo \
  --workflow react \
  --workspace /tmp/tenet-workspace \
  "请阅读 README 并总结"
```

调用链如下：

```text
cmd/tenet/main.go
  -> taskRun()
  -> buildTaskClient()
  -> workflow.Execute()
      -> acquireSessionLease()
      -> NewContext()
      -> Record("RunStarted")
      -> ReactWorkflow()
          -> generateThought()
              -> ContextAssembler
              -> Record("ContextAssembled")
              -> Record("LLMCallStarted")
              -> Decide("GenerateThought")
              -> Record("LLMCallCompleted")
          -> executeTool()
              -> Record("ToolCallStarted")
              -> allowlist / approval / rate / fencing
              -> Decide("ToolExecuted")
              -> Record("ToolCallCompleted" or ToolCallFailed)
          -> Record("TaskCompleted")
      -> Record("RunCompleted")
      -> Commit()
      -> saveRunMemory()
      -> captureRunCheckpoint()
```

## 5. Workflow Runtime

核心文件：

- `go/internal/workflow/task.go`
- `go/internal/workflow/context.go`
- `go/internal/workflow/strategies.go`
- `go/internal/workflow/replay.go`
- `go/internal/workflow/router.go`

### 5.1 `TaskHandle`

`TaskHandle` 是一次 Agent Run 的输入结构，包含：

- stream/session/turn/run id
- query
- workspace
- system prompt
- tools
- model
- config
- worker client
- lock manager
- rate limiter

### 5.2 `workflow.Execute`

这是主入口。它做这些事：

1. 补默认配置。
2. 选择 workflow。
3. 标准化 workspace 路径。
4. 获取 session lock 和 fencing token。
5. 创建 `WorkflowContext`。
6. 写入 `RunStarted`。
7. 执行 workflow 函数。
8. 根据结果写入 `RunCompleted/RunFailed/RunPaused`。
9. 写入 summary memory。
10. 提交事件。
11. 保存 memory。
12. 创建 workspace checkpoint。

### 5.3 `WorkflowContext`

这个对象封装事件记录和 Replay 行为：

- `Record`：记录普通事件。
- `Decide`：记录或回放外部副作用。
- `Commit`：提交 pending events。
- `Sleep`：调度 timer 并挂起 workflow。
- `GetVersion`：兼容历史 workflow 事件演进。

### 5.4 `ReactWorkflow`

ReAct 的循环大致是：

```text
用户消息
  -> LLM 思考
  -> 如果 final，结束
  -> 如果有 tool calls，执行工具
  -> 把工具结果作为 tool message 放回上下文
  -> 再问 LLM
```

Tenet 的实现额外做了：

- context trace
- LLM trace
- tool trace
- token budget
- tool approval
- touched files
- rate limit
- fencing token

## 6. Tool Runtime

核心文件：

- `tools/builtin_tools.json`
- `go/internal/worker/tools.go`
- `python/tenet/tools.py`
- `go/internal/workflow/strategies.go` 的 `executeTool`

### 6.1 工具定义与实现分离

`tools/builtin_tools.json` 是模型看到的工具 schema。

Go/Python 各自实现 handler：

- Go：`LocalToolExecutor.execute`
- Python：`ToolRegistry.execute`

这样做的原因是：模型工具 schema 必须一致，但不同 worker 可以有不同执行环境。

### 6.2 当前主要工具

文件类：

- `read_file`
- `write_file`
- `append_file`
- `replace_in_file`
- `list_dir`
- `search_files`
- `grep`
- `file_info`

代码类：

- `apply_patch`
- `symbol_search`
- `code_outline`
- `run_tests`

Git 类：

- `git_status`
- `git_diff`
- `git_log`
- `git_show`
- `git_branch`

Workspace 类：

- `workspace_snapshot`
- `workspace_restore`

网络/数据：

- `http_fetch`
- `sqlite_query`
- `web_search`，无 provider key 时默认不暴露。

### 6.3 工具安全

工具调用有多层防护：

- workspace path containment
- JSON schema validation
- tool allowlist
- require approval
- shell dangerous pattern
- SSRF 防护
- fencing token
- rate limit
- secret redaction

## 7. Trace 能回答什么问题

Tenet 的 Trace 可以回答：

- Agent 当前执行到哪个 run？
- 一共调用了几次 LLM？
- 每次 LLM 用的 model 是什么？
- 上下文拼了多少条 message？
- token 估算是多少？
- 调用了哪些工具？
- 工具参数是什么？
- 工具失败错误码是什么？
- 工具修改了哪些文件？
- 工作区有没有 checkpoint？

主要事件：

- `ContextAssembled`
- `LLMCallStarted`
- `LLMCallCompleted`
- `LLMCallFailed`
- `ToolCallStarted`
- `ToolCallCompleted`
- `ToolCallFailed`
- `ToolApprovalRequired`
- `WorkspaceSnapshot`
- `WorkspaceCheckpointCreated`

## 8. Projection/API

HTTP API 入口：

- `go/cmd/tenet/http_api.go`

常用路由：

- `GET /api/v1/healthz`
- `GET /api/v1/config`
- `GET /api/v1/workers`
- `GET /api/v1/skills`
- `POST /api/v1/tasks`
- `GET /api/v1/tasks/{id}`
- `POST /api/v1/tasks/{id}/messages`
- `GET /api/v1/tasks/{id}/events`
- `POST /api/v1/tasks/{id}/fork`
- `POST /api/v1/workspace/snapshot`
- `POST /api/v1/workspace/restore`
- `GET /api/v1/openapi.json`

## 9. Python Worker

Python Worker 主要负责：

- 作为 gRPC worker 被 Go 调用。
- 调 LLM provider。
- 执行 Python 侧工具。

核心文件：

- `python/tenet/grpc_worker.py`
- `python/tenet/worker.py`
- `python/tenet/adapters.py`
- `python/tenet/tools.py`

Go -> Python 的 gRPC 调用链：

```text
Go workflow
  -> gateway.WorkerClient
  -> gRPC TenetWorker.GenerateThought / ExecuteTool
  -> python/tenet/grpc_worker.py
  -> StatelessWorker
  -> ProviderRouter / ToolRegistry
```

当前本机如果没安装 `grpcio`，相关测试会 skip。安装方式：

```bash
cd python
python3 -m pip install -e .
```

## 10. 本地测试命令

Go 测试：

```bash
cd /Users/hcy/Desktop/Tenet/go
go test ./...
```

Python 测试：

```bash
cd /Users/hcy/Desktop/Tenet
PYTHONPATH=python python3 -m unittest discover -s python/tests
```

无 key 端到端 smoke：

```bash
cd /Users/hcy/Desktop/Tenet
scripts/e2e/no_key_smoke.sh
```

## 11. 推荐学习顺序

如果你想真正读懂 Tenet，建议按这个顺序：

1. 先读本文，建立全局地图。
2. 读 `go/internal/storage/store.go`，理解事件日志。
3. 读 `go/internal/workflow/context.go`，理解 Record/Decide/Replay。
4. 读 `go/internal/workflow/task.go`，理解一次 Run 的生命周期。
5. 读 `go/internal/workflow/strategies.go`，理解 simple/react/coding/dag。
6. 读 `go/internal/worker/tools.go`，理解工具执行与安全。
7. 读 `go/internal/projection/projection.go`，理解状态如何生成。
8. 读 `go/cmd/tenet/main.go` 和 `http_api.go`，理解 CLI/API。
9. 读 Python worker，理解远程 worker 如何接入。
10. 跑测试和 smoke，然后自己加一个小工具。

## 12. 当前仍可继续深化的方向

项目已经是可运行 Agent MVP，但还可以继续增强：

- MCP tool 真执行。
- 任意 turn 的文件级恢复。
- 审批后的继续执行。
- 更强的 DAG 并发调度。
- 更强的 coding workflow test-fix loop。
- 前端 Trace 交互体验。
- CI 中安装 `grpcio` 后强制跑 Go->Python gRPC smoke。

