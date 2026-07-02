# Go Workflow Context

> WorkflowContext · Decide 立即落盘 · Record 延迟缓冲 · ctx.Async 乱序缓冲 · ctx.Sleep 哨兵退出 · 回放指针
>
> 文档状态：DRAFT · 版本：v1.1.0
>
> Tenet 确定性状态机的心脏。同一段 Workflow 代码通过 `historyPos` 指针获得双重人格——首次执行是事件生产者，回放是事件消费者。本文件是所有 Workflow 和 Pattern 实现的基础规范。

---

## 1. WorkflowContext 结构定义

Workflow 函数的唯一参数。所有外部交互——调 LLM、读写时间、生成随机数、操作文件——必须通过 Context 的方法完成。Workflow 函数内部禁止直接调用 `time.Now()`、`time.Sleep()`、`http.Get()`、`rand.Int()`。

### 1.1 完整字段清单

| 字段 | 类型 | 类别 | 初始化值 | 职责 |
|---|---|---|---|---|
| `store` | `EventStore` 接口 | 持久化 | 构造函数注入 | 读写 SQLite 的唯一通道。所有事件落盘通过此接口 |
| `streamID` | `string` | 标识 | 构造函数注入 | 物理流 ID。编码谱系：`task:<uuid>` / `task:<uuid>/fork:<N>` |
| `parentID` | `string` | 标识 | 构造函数注入 | Fork 来源主流 ID。首次执行为空字符串 |
| `history` | `[]Event` | 状态机 | 见 1.2 节 | 从 SQLite 加载的该流已有事件切片。首次执行=空切片，回放=完整事件流 |
| `historyPos` | `int` | 状态机 | 见 1.2 节 | **引擎最关键的单个变量**。`< len(history)` = 回放模式，`>= len(history)` = 首次执行模式 |
| `pendingRecords` | `[]Event` | 缓冲 | 空切片 | Record 事件的内存暂存区。Decide 前强制 flush |
| `recordBatchSize` | `int` | 配置 | `config.Workflow.RecordBatchSize`（默认 20） | 触发自动 flush 的阈值 |
| `version` | `int` | 版本 | `config.Workflow.Version` | 当前 Workflow 代码版本号 |
| `versionMarkers` | `map[string]bool` | 版本 | 空 map | 已写入的 VersionMarker 变更 ID 集合。防重复写入 |
| `asyncOrderCounter` | `int64` | 并发 | 0（atomic） | ctx.Async 的全局注册序号计数器 |
| `asyncBuffer` | `map[int64][]Event` | 并发 | 空 map | Out-of-Order Buffer——key=注册序号，value=该序号的待落盘事件 |
| `asyncNextToFlush` | `int64` | 并发 | 0 | 下一个允许落盘的注册序号 |
| `asyncBufferMu` | `sync.Mutex` | 并发 | 零值 | 保护 asyncBuffer 的互斥锁 |
| `asyncFlushCond` | `*sync.Cond` | 并发 | `sync.NewCond(&asyncBufferMu)` | 条件变量——唤醒等待落盘的 goroutine |
| `config` | `*RuntimeConfig` | 配置 | 构造函数注入 | Session 启动时冻结的只读配置快照。整个生命周期不变 |

### 1.2 三种初始化模式

`NewWorkflowContext(store, streamID, mode, forkFromSeq, parentID, config)` 根据 mode 参数初始化 history 和 historyPos：

| mode | history 初始化 | historyPos | parentID | 使用场景 |
|---|---|---|---|---|
| `ContextModeExecution` | `[]Event{}`（空） | `0` | `""` | CLI `tenet task run` |
| `ContextModeReplay` | `store.ReadAll(streamID)` | `0` | `history[0].ParentID`（如存在） | 崩溃恢复、CLI `tenet task replay` |
| `ContextModeFork` | `store.Read(parentID, 0, forkFromSeq)` | `forkFromSeq` | `parentID` | CLI `tenet task fork` |

---

## 2. Decide — 决策点

### 2.1 函数签名与语义

```go
func (ctx *WorkflowContext) Decide(decisionType string, fn func() (any, error)) (any, error)
```

- `decisionType`：决策点类型常量。必须是预定义的：`"GenerateThought"`、`"Sleep"`、`"Now"`、`"Random"`、`"SideEffect"`
- `fn`：实际执行外部调用的闭包。回放模式下不调用。闭包内部负责 gRPC 调用、文件 I/O 等
- 返回值：`fn()` 的结果（首次执行）或历史事件的缓存结果（回放）

### 2.2 内部控制流（5 步）

**Step 0 — 防御性 Flush**：调用 `ctx.flushRecords()`——将所有 `pendingRecords` 中的 Record 事件通过 WriteDaemon 写入 SQLite 并提交事务。保证本次 Decide 之前的所有 Record 事件已物理持久化。原因：Record 事件是回放时确定性检查的物理指纹——如果 Decide 已落盘但其前面的 Record 因崩溃丢失，重放指针偏移会导致 panic。

**Step 1 — 模式判断**：比较 `historyPos` 与 `len(history)`。`<` = 回放模式（Step 2），`>=` = 首次执行模式（Step 3-4）。

**Step 2 — 回放路径**：
- 从 `history[historyPos]` 读取事件 `evt`
- **类型比对**：`evt.Type` 必须 == `decisionType`。不一致 → panic，消息格式：`"non-determinism detected: at stream={streamID}, pos={historyPos}, expected={decisionType}, got={evt.Type} — did you forget GetVersion()?"`
- `historyPos++`
- 返回 `evt.Payload.Result`, `nil`（注意：即使原事件记录了 error，也返回 nil error——error 信息在 Payload.Result 中）
- **不发起任何外部调用**

**Step 3 — 首次执行路径（执行 fn）**：
- 调用 `fn()`，获取 `result, err`
- 无论 `err` 是否为 nil——失败也是合法的状态转换（`AgentFailed` 事件）

**Step 4 — 立即落盘（Immediate Commit）**：
- 计算 `streamSeq`：`len(history) + len(pendingRecords) + 1`（pendingRecords 已在 Step 0 flush，理论上为 0）
- 构造 Event：`StreamID=streamID`, `StreamSeq=streamSeq`, `Type=decisionType`, `Payload={Result: result, Error: err}`
- 通过 `store.Append(event)` 写入 SQLite。内部通过 WriteDaemon 的 `Submit` 方法——构造 WriteRequest，包含 INSERT SQL + 连续性断言（`streamSeq == MAX(streamSeq) + 1`），写入 writeCh channel，阻塞等待 resultCh 返回
- WriteDaemon 返回成功（事务已提交）→ `history = append(history, event)` → `historyPos++`
- 返回 `result, err`

### 2.3 决策点类型定义

| 类型常量 | 对应的外部操作 | 非确定性来源 |
|---|---|---|
| `"GenerateThought"` | gRPC 调 Python 的 GenerateThought RPC | LLM 输出天然不确定 |
| `"ToolExecuted"` | gRPC 调 Python 的 ExecuteTool RPC | 工具输出随环境变化 |
| `"Sleep"` | 定时等待（最终调 time.After） | 时间流逝不可重复 |
| `"Now"` | 获取当前时间 | 两次调用时间不同 |
| `"Random"` | 生成随机数 | 随机数不同 |
| `"SideEffect"` | Go 内部确定性的辅助操作（如写本地缓存） | 仅用于 Record，不经过 Decide |

### 2.4 Panic 恢复语义

Decide 内部的 panic（确定性检查失败）**不应被 recover**——它是设计意图，强制开发者意识到代码变更破坏了事件流兼容性。Go 的 panic 会导致当前 goroutine 终止 + deferred 函数执行 → Scheduler 通过 recover 捕获 → 记录 TaskFailed 事件 → Worker 放回池。

---

## 3. Record — 确定性状态标记

### 3.1 函数签名

```go
func (ctx *WorkflowContext) Record(eventType string, payload any)
```

- 不调用外部服务。只记录「状态从 A 变成了 B」这一事实
- 回放时用于细粒度的控制流对齐检查（类型比对）

### 3.2 延迟批量提交

1. 构造 Event：`StreamID=streamID`, `StreamSeq=nextRecordSeq()`（= len(history) + len(pendingRecords) + 1）, `Type=eventType`, `Payload=payload`
2. 追加到 `pendingRecords`
3. 如果 `len(pendingRecords) >= recordBatchSize` → 调 `flushRecords()`

### 3.3 flushRecords 行为

1. 如果 `len(pendingRecords) == 0` → 直接返回
2. 构造 `[]WriteStatement`——每个 pending Event 一条 INSERT SQL
3. 通过 `store.Submit(statements...)` 提交到 WriteDaemon——单事务批量写入
4. WriteDaemon 返回成功 → `history = append(history, pendingRecords...)` → `historyPos += len(pendingRecords)` → `pendingRecords = nil`

### 3.4 Decide vs Record

| | Decide | Record |
|---|---|---|
| 调用外部服务 | 是（gRPC、文件 I/O） | 否 |
| 落盘时机 | **立即**——fn() 返回后立即写入 | **延迟**——批量或在 Decide 前 flush |
| 回放行为 | 跳过 fn()，从事件取缓存结果 | 类型比对，跳过 |
| 典型事件 | GenerateThought, Sleep, SideEffect | TaskStarted, TaskCompleted, SubTaskDispatched |
| 崩溃后丢失影响 | **致命**——重复执行 + 脏工作区 | **可恢复**——回放时重新生成 |

---

## 4. Immediate Commit 崩溃安全

### 4.1 懒提交的灾难

假设 Workflow 执行完所有步骤后统一 Flush：

1. `Record("TaskStarted")` → 内存缓冲
2. `Decide("GenerateThought")` → 实际调 Python，30s，$0.50 token → 事件暂存内存
3. `Record("TaskCompleted")` → 内存缓冲
4. Go 进程 OOM 崩溃
5. SQLite 中**零记录**。重启后系统判定「首次执行」→ 重新 Decide → 重复消费 $0.50 → 面对已被上次执行污染的工作区

### 4.2 逐步落盘保证

Decide 的 Step 4 保证：`fn()` 返回后，事件在函数返回前已通过 WriteDaemon 同步写入 SQLite + 事务提交。崩溃在任何位置：

- 崩溃在 `fn()` 执行中 → event_log 无记录 → 重启回放从该 Decide 重新开始（token 重复不可避免——函数运行中无法恢复中间状态）
- 崩溃在 Step 4 落盘后、return 前 → event_log 已有事件 → 重启回放通过 Step 2 跳过 → **不重复执行**
- 崩溃在 Record flushRecords 前 → Decide 事件已落盘，Record 未落盘 → 回放时重新生成 Record

---

## 5. ctx.Async — 确定性并发

### 5.1 为什么禁止 `go`

```go
// 危险——在 Workflow 内禁止！
go ctx.Decide("Step_A", fnA)
go ctx.Decide("Step_B", fnB)
```

OS 调度不确定 → Step_B 可能先完成 → event_log 顺序：`[Step_B, Step_A]`。回放时 OS 调度变化 → Step_A 先执行 → 确定性检查崩在 Step_A 的位置期望 `Step_A` 但读到 `Step_B`。

### 5.2 函数签名

```go
func (ctx *WorkflowContext) Async(name string, fn func() error) *Future
```

- 返回 `Future` 对象。调用方通过 `Future.Wait()` 阻塞等待子任务完成
- 子任务内部可以调用 `Decide` / `Record`——这些调用产生的所有事件通过 Out-of-Order Buffer 强制按注册顺序写入

### 5.3 Out-of-Order Buffer 机制

1. `Async("Step_A", fnA)` → 分配 `order=0`（atomic 递增）
2. `Async("Step_B", fnB)` → 分配 `order=1`
3. `go fnA()` 和 `go fnB()` 物理并发执行
4. 假设 fnB 先完成（第 2 秒），fnA 后完成（第 15 秒）：
   - fnB 完成 → 尝试落盘 → `asyncBufferMu.Lock()` → `order(1) != asyncNextToFlush(0)` → 事件存入 `asyncBuffer[1]` → `asyncFlushCond.Wait()` 阻塞
   - fnA 完成 → 尝试落盘 → `order(0) == asyncNextToFlush(0)` → 事件通过 WriteDaemon 写入 → `asyncNextToFlush = 1` → 检查 `asyncBuffer[1]` 中是否有等待事件 → 有 → 继续写入 → `delete(asyncBuffer, 1)` → `asyncNextToFlush = 2` → `asyncFlushCond.Broadcast()` 唤醒
   - 最终 event_log 顺序：`[Step_A.events..., Step_B.events...]`——与注册顺序一致，与物理完成顺序无关

### 5.4 Future 结构

| 字段 | 类型 | 说明 |
|---|---|---|
| `Name` | `string` | 子任务名（日志用） |
| `Order` | `int64` | 注册序号 |
| `Err` | `error` | 子任务返回的错误（Wait 后填充） |
| `done` | `chan struct{}` | 完成信号 |

### 5.5 约束

- 子任务内部可以创建子-子任务（嵌套 Async），每个子-子任务共享父任务的 order 前缀
- 所有子任务必须通过 `Wait()` 收集完成——泄漏的 goroutine 在 Workflow Commit 时检测并 Warn

---

## 6. ctx.Sleep — 哨兵错误退出

### 6.1 为什么不能 `time.Sleep`

Go 的 goroutine 不支持序列化调用栈后恢复。`time.Sleep` 会永久占用 Worker——长 sleep（如 30 分钟）阻塞 Worker Pool。

### 6.2 函数签名

```go
func (ctx *WorkflowContext) Sleep(duration time.Duration) error
```

### 6.3 哨兵错误

```go
var ErrWorkflowSuspended = errors.New("workflow suspended")
```

这是系统级哨兵——不是真正的错误。Worker 捕获后识别为「正常挂起」而非「执行失败」。

### 6.4 首次执行模式行为

1. 调用 `ctx.Record("TimerStarted", TimerStartedPayload{TimerID, Duration, StartTime})`
2. 调用 `ctx.flushRecords()`——强制落盘 TimerStarted 事件
3. 向 `TimerService.Add(timerID, duration, streamID)` 注册定时器（插入最小堆）
4. **返回 `ErrWorkflowSuspended`**

### 6.5 Worker 对哨兵错误的处理

1. `errors.Is(err, ErrWorkflowSuspended)` → true
2. 不做 `ctx.Commit()`——TimerStarted 已在步骤 6.4.2 中落盘
3. 将 Worker 放回空闲池（`pool.workers <- worker`）
4. 当前 goroutine 正常返回（不持有任何资源）

### 6.6 唤醒

1. TimerService 后台检测到定时器到期
2. 向 event_log 写入 `TimerFired` 事件（stream_id=streamID, seq=上次 seq+1）
3. 将 TaskHandle 重新放入 `workflowQueue`
4. Scheduler 分配新 Worker（可能是不同的 goroutine）
5. 新 Worker 以 `ContextModeReplay` 创建 WorkflowContext
6. 回放 Workflow 函数 → `ctx.Sleep()` 内部检测到 `history[historyPos]` 是 `TimerStarted` → 跳过 → `history[historyPos+1]` 是 `TimerFired` → 跳过 → **0ms 返回 nil**（不抛哨兵错误）

---

## 7. GetVersion — 版本门控

### 7.1 函数签名

```go
func (ctx *WorkflowContext) GetVersion(changeID string, minVersion int) int
```

### 7.2 行为

**首次执行**：当前 `historyPos` 之后没有该 changeID 的 VersionMarker → 写入 `VersionMarker` 事件到 event_log → `ctx.versionMarkers[changeID] = true` → 返回 `ctx.version`

**回放**：在 history 中从**索引 0** 开始向后扫描——查找 `event_type == "VersionMarker"` 且 `payload.change_id == changeID` 的事件（不能从 historyPos 开始扫描，因为 VersionMarker 可能已被之前的回放步骤消耗，其索引 < historyPos）。找到 → 将该事件从 history 切片中移除（`history = append(history[:i], history[i+1:]...)`）。**必须补偿 historyPos：如果被移除的索引 `i < historyPos`，则 `historyPos--`（因为后续已回放事件在切片中的索引全部偏移了 -1）。** → 返回 `payload.version`。未找到 → 返回 `defaultVersion=1`（此事件流是该变更引入之前创建的）

### 7.3 VersionMarker Payload 结构

```json
{
  "change_id": "add-retry-logic",
  "version": 2
}
```

---

## 8. Commit

```go
func (ctx *WorkflowContext) Commit() error
```

Workflow 函数执行完毕后调用。行为：`ctx.flushRecords()`——将所有 pending 的 Record 事件写入 SQLite。Decide 事件在调用时已落盘，Commit 不处理它们。

Worker 的 Run 方法在 Workflow 返回后调用 Commit（除非 Workflow 返回 `ErrWorkflowSuspended`——此时 Commit 已在 Sleep 内部完成）。

---

## 9. nextSeq 计算方法

```go
func (ctx *WorkflowContext) nextSeq() int64 {
    return int64(len(ctx.history) + len(ctx.pendingRecords) + 1)
}
```

首次执行时 history 随 Decide 增长，pendingRecords 在 Step 0 flush 后为 0。回放时 historyPos 递增但不产生新 seq——seq 仅在首次执行路径的 Step 4 中计算和使用。

---

## 10. 并发安全

- `WorkflowContext` **不是 goroutine-safe**——每个 Context 绑定一个 goroutine（Worker）
- `asyncBufferMu` 和 `asyncFlushCond` 仅在 `Async` 子任务之间共享——这是 Context 唯一的跨 goroutine 共享状态
- `history`、`historyPos`、`pendingRecords` 只在拥有 Context 的 goroutine 中修改
- `store`（EventStore）是 goroutine-safe 的——WriteDaemon 内部序列化所有写入

---

## 附录 A：事件类型目录

所有事件写入同一条物理流（`task:<uuid>`），通过 `event_type` 字段区分语义。以下为完整的事件类型清单，按产生阶段分组。

### A.1 编排事件（驱动 Workflow 控制流）

| event_type | 产生方式 | Payload 关键字段 | 产生位置 |
|---|---|---|---|
| `TaskCreated` | `ctx.Record` | `{query, workspace, workflow_type}` | Workflow 入口 |
| `TaskStarted` | `ctx.Record` | `{session_id, workspace}` | 各 Workflow 初始化 |
| `ComplexityAnalyzed` | `ctx.Record` | `{complexity_score, reason, selected_workflow}` | Strategy Router |
| `TaskDecomposed` | `ctx.Record` | `{subtasks: [{id, agent_role, task, depends_on}]}` | DAGWorkflow |
| `SubTaskDispatched` | `ctx.Record` | `{subtask_id, agent_role}` | DAGWorkflow |
| `SubTaskCompleted` | `ctx.Record` | `{subtask_id, key_findings, artifacts}` | DAGWorkflow |
| `TaskCompleted` | `ctx.Record` | `{result, total_steps, key_findings, artifacts}` | 各 Workflow 收敛 |
| `TaskFailed` | `ctx.Record` | `{error: string}` | 各 Workflow 异常退出 |
| `TaskSuspended` | `ctx.Record` | `{reason, commit_hash}` | InteractiveWorkflow 挂起 |

### A.2 Agent 执行事件（LLM 推理与工具调用）

| event_type | 产生方式 | Payload 关键字段 | 产生位置 |
|---|---|---|---|
| `GenerateThought` | `ctx.Decide` | `{thought, tool_calls, is_final, finish_reason, usage: {prompt_tokens, completion_tokens, total_tokens, cost_usd}}` | ReactWorkflow / SimpleWorkflow / Reasoning Patterns |
| `ToolExecuted` | `ctx.Decide` | `{tool_name, arguments: JSON string, stdout, stderr, exit_code, is_error, duration_ms}` | ReactWorkflow |
| `TokenUsed` | `ctx.Record` | `{task_id, agent_name, model, prompt_tokens, completion_tokens, cost_usd}` | Token Budget |
| `ToolsDiscovered` | `ctx.Record` | `{tools: [{name, description, parameters_schema}]}` | ReactWorkflow（MCP 工具首次发现后） |

### A.3 版本与谱系事件

| event_type | 产生方式 | Payload 关键字段 | 产生位置 |
|---|---|---|---|
| `VersionMarker` | `ctx.Record` | `{change_id, version}` | GetVersion 首次执行 |
| `ForkCreated` | `ctx.Record` | `{forked_from: streamID, fork_at_seq: int}` | ForkWorkflow |

### A.4 快照与系统事件

| event_type | 产生方式 | Payload 关键字段 | 产生位置 |
|---|---|---|---|
| `SnapshotCreated` | `ctx.Record` | `{snapshot_type, snapshot_ref, stream_seq}` | Projection Engine 快照触发器 |
| `TimerStarted` | `ctx.Record` | `{timer_id, duration, start_time}` | ctx.Sleep 首次执行 |
| `TimerFired` | 直接写入（非 Record） | `{timer_id, fired_at}` | TimerService 后台 goroutine |
| `ConfigChanged` | 直接写入 system:config 流 | `{param_path, old_value, new_value, operator_id, reason}` | 动态配置变更 |
| `WaitingForHumanInput` | `ctx.Record` | `{round: int, commit_hash}` | InteractiveWorkflow |

### A.5 Payload 字段命名约定

- 所有 Payload 字段使用 `snake_case`
- `usage` 子对象统一为 `{prompt_tokens, completion_tokens, total_tokens, cost_usd}`
- `tool_calls` 子对象统一为 `[{call_id, tool_name, arguments}]`
- `arguments` 在 `ToolCall` 中是 JSON string（LLM 原始输出），在 `ToolExecuted` payload 中同样是 JSON string（传入 ExecuteTool 的参数）
