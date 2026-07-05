# Tenet 代码阅读教学文档

这份文档是“带你读代码”的路线图。建议你不要从 `main.go` 一头扎到底，而是按系统层次读。

## 第一课：先跑起来

先确认项目能跑：

```bash
cd /Users/hcy/Desktop/Tenet/go
go test ./...
```

```bash
cd /Users/hcy/Desktop/Tenet
PYTHONPATH=python python3 -m unittest discover -s python/tests
scripts/e2e/no_key_smoke.sh
```

如果这些通过，说明你阅读时看到的行为可以用测试复现。

## 第二课：读事件存储

文件：

```text
go/internal/storage/schema.go
go/internal/storage/store.go
```

重点看：

- `Event`
- `AppendEvent`
- `AppendEvents`
- `encodeEventPayload`
- `SaveSnapshot`
- `ForkStream`
- `Read`

阅读问题：

1. 事件是如何获得 `stream_seq` 的？
2. `schema_version` 是在哪里补上的？
3. secret redaction 是在哪里做的？
4. fork 事件流时，新 stream 如何指向 parent？

你要形成的理解：

```text
所有状态变化先变成事件。
事件不可变。
当前状态由 Projection 重新计算。
```

## 第三课：读 WorkflowContext

文件：

```text
go/internal/workflow/context.go
```

重点看：

- `Record`
- `Decide`
- `Commit`
- `Sleep`
- `GetVersion`

最重要的是 `Decide`。

正常执行时，`Decide` 会调用传入的函数，并把结果写成事件：

```text
Decide("GenerateThought") -> 调 LLM -> 写 GenerateThought
Decide("ToolExecuted") -> 调工具 -> 写 ToolExecuted
```

Replay 时，`Decide` 不调用函数，而是消费历史事件。

阅读问题：

1. 为什么普通事件用 `Record`？
2. 为什么 LLM/工具要用 `Decide`？
3. Replay 如何发现 workflow 代码变了？

## 第四课：读 Run 生命周期

文件：

```text
go/internal/workflow/task.go
```

重点看：

- `TaskHandle`
- `Execute`
- `captureRunCheckpoint`
- `saveRunMemory`
- `acquireSessionLease`

推荐你手动画一条流程：

```text
Execute
  -> 补默认值
  -> 选 workflow
  -> 获取锁
  -> 创建 WorkflowContext
  -> RunStarted
  -> 执行 workflow
  -> RunCompleted/RunFailed/RunPaused
  -> summary memory
  -> Commit
  -> workspace checkpoint
```

阅读问题：

1. `SessionID`、`TurnID`、`RunID` 分别什么时候补默认？
2. workflow 出错和 workflow suspend 的处理有什么不同？
3. checkpoint 为什么在 run 成功后保存？

## 第五课：读 ReAct Workflow

文件：

```text
go/internal/workflow/strategies.go
```

先看：

- `SimpleWorkflow`
- `ReactWorkflow`
- `generateThought`
- `executeTool`

`ReactWorkflow` 的核心循环：

```text
LLM -> tool calls? -> execute tools -> tool results -> LLM -> final
```

`generateThought` 负责：

- 拼上下文。
- 记录 `ContextAssembled`。
- 记录 `LLMCallStarted`。
- 用 `Decide("GenerateThought")` 调 LLM。
- 记录 `LLMCallCompleted/Failed`。
- 记录 token usage。

`executeTool` 负责：

- `ToolCallStarted`
- allowlist
- approval
- rate limit
- fencing token
- `Decide("ToolExecuted")`
- `ToolCallCompleted/Failed`
- touched files

阅读问题：

1. 为什么工具执行前要检查 fencing token？
2. `require_approval` 为什么会导致 workflow suspended？
3. `touched_files` 是如何推断出来的？

## 第六课：读工具系统

文件：

```text
tools/builtin_tools.json
go/internal/worker/tools.go
python/tenet/tools.py
```

先读 manifest：

```json
{
  "name": "read_file",
  "parameters_schema": "..."
}
```

然后读 Go 的：

- `BuiltinToolDefinitions`
- `ValidateToolArguments`
- `LocalToolExecutor.Execute`
- `LocalToolExecutor.execute`
- 每个具体工具 handler

再读 Python 的：

- `ToolRegistry.definitions`
- `ToolRegistry.execute`
- 每个 `_xxx` handler

阅读问题：

1. manifest 和 handler 为什么分开？
2. Go/Python 如何避免 schema 漂移？
3. 哪些工具会修改 workspace？
4. `workspace_snapshot` 和 `workspace_restore` 如何保证路径安全？

小练习：

给 manifest 加一个只读工具，然后分别补 Go/Python handler 和测试。

## 第七课：读 Projection

文件：

```text
go/internal/projection/projection.go
```

重点看：

- `TaskView`
- `Apply`
- `TaskProjection`
- `upsertLLMCall`
- `completeLLMCall`
- `upsertToolCall`
- `completeToolCall`

你要理解：

```text
事件日志是事实。
Projection 是视图。
前端/API 大多数时候应该读 Projection，而不是自己解析事件。
```

阅读问题：

1. `ToolCallStarted` 和 `ToolCallCompleted` 如何合并成一个 `ToolCallState`？
2. `RunStarted/RunCompleted` 如何更新 run 状态？
3. token 是如何聚合的？

## 第八课：读 Replay

文件：

```text
go/internal/workflow/replay.go
```

重点看：

- `Replay`
- `buildReplaySpec`
- `replayGuardClient`

Replay 的核心约束：

- 不允许真实 LLM 调用。
- 不允许真实工具调用。
- 不允许追加事件。
- 必须消费完历史事件。

阅读问题：

1. `replayGuardClient` 为什么存在？
2. Replay 如何选择一个 run 的事件片段？
3. 如果 workflow 新增了事件，Replay 会怎么失败？

## 第九课：读 API/CLI

文件：

```text
go/cmd/tenet/main.go
go/cmd/tenet/http_api.go
```

CLI 重点：

- `run`
- `taskCmd`
- `taskRun`
- `buildTaskClient`
- `skillsCmd`

HTTP 重点：

- `newAPIHandler`
- `handleHTTPTaskCreate`
- `handleHTTPTaskMessage`
- `openAPISpec`

阅读问题：

1. CLI 和 HTTP 如何共用 workflow runtime？
2. `--worker echo/openai/deepseek/grpc` 分别构造什么 client？
3. `/api/v1/tasks/{id}/messages` 如何创建新 turn/run？

## 第十课：读 Python Worker

文件：

```text
python/tenet/grpc_worker.py
python/tenet/worker.py
python/tenet/adapters.py
python/tenet/tools.py
```

调用链：

```text
Go gateway.WorkerClient
  -> gRPC
  -> TenetWorkerProtoServicer
  -> StatelessWorker
  -> ProviderRouter 或 ToolRegistry
```

阅读问题：

1. Python Worker 为什么是 stateless？
2. provider 是如何通过环境变量选择的？
3. Python 工具结果如何转成 gRPC response？

## 第十一课：读测试

优先读这些测试：

```text
go/internal/workflow/strategies_test.go
go/internal/workflow/replay_test.go
go/internal/worker/tools_test.go
go/internal/projection/projection_test.go
go/cmd/tenet/main_test.go
python/tests/test_tools.py
scripts/e2e/no_key_smoke.sh
```

测试是最好的代码说明。

建议你每读一个模块，就做这件事：

1. 找对应 test。
2. 跑单个 package 测试。
3. 改一个小行为。
4. 看哪个测试失败。
5. 再回到源码理解原因。

## 第十二课：从一个具体用例贯穿代码

用例：

```text
用户让 Agent 修改代码并跑测试。
```

你应该能在代码里对应到：

1. CLI/API 创建 task。
2. Router 判断是 coding/react。
3. `workflow.Execute` 创建 run。
4. `generateThought` 调 LLM。
5. LLM 选择 `read_file` / `code_outline` / `apply_patch` / `run_tests`。
6. `executeTool` 记录工具 trace。
7. `ToolCallCompleted` 记录 touched files。
8. run 成功后创建 workspace checkpoint。
9. Projection 展示最终状态。
10. Replay 可以验证这次执行。

## 第十三课：如何继续贡献代码

推荐从小任务开始：

1. 新增一个只读工具。
2. 给 Projection 加一个字段。
3. 给 OpenAPI 加一个 schema。
4. 给 no-key smoke 加一个场景。
5. 给 workflow 增加一个事件，并同步 Replay 测试。

每次改动都跑：

```bash
cd go && go test ./...
cd .. && PYTHONPATH=python python3 -m unittest discover -s python/tests
scripts/e2e/no_key_smoke.sh
```

## 第十四课：常见阅读误区

误区一：只看 HTTP API。

HTTP API 只是入口，真正的核心在 workflow/event/projection。

误区二：把事件当日志。

Tenet 的事件不是普通 log，而是状态事实来源。

误区三：认为工具只在 worker 里安全。

工具安全分两层：workflow 调用前拦截 + worker 执行时拦截。

误区四：Replay 等于打印历史。

Tenet 的 Replay 是重新执行 workflow，只是不真实调用外部系统。

