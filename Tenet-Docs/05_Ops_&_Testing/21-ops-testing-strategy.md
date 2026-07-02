# Testing Strategy

> 测试分层 · Go 核心引擎单元测试 · Python 安全对抗测试 · 确定性重放回归 · CI 集成
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 1. 测试分层金字塔

```
         ┌──────────┐
         │  Replay   │  确定性重放回归（10%）
         │ Regression│  历史账本对齐 · Three-Zero 断言
         ├──────────┤
         │Integration│  集成测试（20%）
         │          │  gRPC 跨进程 · SQLite 事务 · Redis 锁
         ├──────────┤
         │   Unit   │  单元测试（70%）
         │          │  Go 组件 mock · Python 对抗注入
         └──────────┘
```

---

## 2. Go 核心引擎：高精细度单元测试

### 2.1 event 包 — 事件存储与 WriteDaemon

**测试用例 1：单写串行化校验**

| 维度 | 说明 |
|---|---|
| 测试目标 | 50 个并发 goroutine 向同一 stream 写入事件时，SQLite 不发生锁竞争错误 |
| 前置条件 | in-memory SQLite，WriteDaemon 已启动，queueSize=100 |
| 测试步骤 | 启动 50 个 goroutine，每个向 `stream_id="test:concurrent"` 调用 `store.Append` 写入 10 个事件，共 500 个事件 |
| 断言 1 | 所有 500 次 `Append` 调用无一次返回 `SQLITE_BUSY` 错误 |
| 断言 2 | 查询 `SELECT stream_seq FROM event_log WHERE stream_id='test:concurrent' ORDER BY stream_seq`——返回序列严格为 `1,2,3,...,500`，无空洞，无重复 |
| 断言 3 | WriteDaemon 的 `writeCh` channel 在高峰期未满（无阻塞丢事件） |

**测试用例 2：原子事务回滚**

| 维度 | 说明 |
|---|---|
| 测试目标 | 写入中模拟磁盘满 IO 异常，验证整笔事务回滚 |
| 前置条件 | 使用 mock WriteDaemon——在 `tx.Exec` 的第一条 SQL 成功后、第二条 SQL 前注入错误 |
| 测试步骤 | 调用 `store.Append`，内部执行：INSERT event_log → INSERT token_telemetry → UPDATE sessions。在第二步 INSERT token_telemetry 时注入 `io.ErrShortWrite` |
| 断言 1 | event_log 中该 stream 的 `MAX(stream_seq)` 未增加——第一条 INSERT 已回滚 |
| 断言 2 | token_telemetry 中无新增记录 |
| 断言 3 | sessions 的 `updated_at` 未变化 |
| 断言 4 | `Append` 返回 error，类型为 `io.ErrShortWrite` |

---

### 2.2 workflow 包 — WorkflowContext 状态机

**测试用例 3：双人格模式切换**

| 维度 | 说明 |
|---|---|
| 测试目标 | Context 在 history 耗尽后自动从回放模式切到首次执行模式 |
| 前置条件 | 手动构造 history 数组：3 个事件（type=TaskStarted, GenerateThought, TaskCompleted），每事件 payload.Result 包含预定义值 |
| 测试步骤 | `NewWorkflowContext(mode=Replay, history=上述3事件)` → 连续调用 4 次 `Decide` |
| 断言 1 | 第 1 次 Decide：`historyPos=0 < len(history)=3` → 回放路径。`fn` 闭包**未被调用**（通过闭包内的计数器验证）。返回值 == `history[0].Payload.Result`。耗时 < 1ms |
| 断言 2 | 第 2 次 Decide：同上，`historyPos=1`，`fn` 未被调用 |
| 断言 3 | 第 3 次 Decide：同上，`historyPos=2`，`fn` 未被调用 |
| 断言 4 | 第 4 次 Decide：`historyPos=3 >= len(history)=3` → 首次执行路径。`fn` 闭包**被调用**。返回值 == `fn()` 的实际结果。event_log 中新增一条事件（seq=4） |
| 断言 5 | `ctx.historyPos == 4`，`len(ctx.history) == 4` |

**测试用例 3b：确定性检查 panic**

| 维度 | 说明 |
|---|---|
| 测试目标 | 回放时决策类型不匹配 → panic |
| 前置条件 | history 中第 2 个事件的 type="GenerateThought"，但 Workflow 代码在第 2 步调 `Decide("ToolExecuted")` |
| 测试步骤 | 用上述 history 初始化 Context → 第 1 次 Decide("GenerateThought") 成功 → 第 2 次 Decide("ToolExecuted") |
| 断言 | `Decide` 内部 panic。panic 消息包含：`"non-determinism detected"`、`streamID`、`historyPos=2`、`expected=ToolExecuted`、`got=GenerateThought`、`"GetVersion()"` |

**测试用例 3c：Record 批量提交与 Decide 前强制 flush**

| 维度 | 说明 |
|---|---|
| 测试目标 | Decide 前自动 flush 所有 pending Records |
| 前置条件 | in-memory SQLite，recordBatchSize=20 |
| 测试步骤 | 连续 `Record` 5 次 → 验证 pendingRecords 长度=5，event_log 中无新增 → `Decide("GenerateThought")` |
| 断言 1 | Decide 返回后，event_log 中已有 5 条 Record 事件 + 1 条 GenerateThought 事件 |
| 断言 2 | Record 事件的 seq 严格在 GenerateThought 之前（1-5 → 6） |

---

### 2.3 timer 包 — 确定性定时器

**测试用例 4：虚拟时钟时间加速**

| 维度 | 说明 |
|---|---|
| 测试目标 | `ctx.Sleep(1*time.Hour)` 在虚拟时钟下 5ms 内完成 |
| 前置条件 | TimerService 使用可注入的 `Clock` 接口。测试注入 `VirtualClock`——初始时间 `t0`，可通过 `Advance(d)` 直接跳转 |
| 测试步骤 | 1. `ctx.Sleep(1*time.Hour)` → 内部 Record("TimerStarted") + flushRecords → 向 TimerService 注册定时器：`FireAt = t0 + 1h` → 返回 `ErrWorkflowSuspended`。2. 调用 `VirtualClock.Advance(1*time.Hour)` 直接跳转时间。3. TimerService 后台检测到到期 → `FireCh` 收到 `TimerFired` 事件 |
| 断言 1 | 从 Sleep 调用到 TimerFired 事件产生的实际耗时 < 5ms（不是 1 小时） |
| 断言 2 | `TimerFired.StreamID` 正确 |
| 断言 3 | event_log 中 TimerStarted 的 seq 和 TimerFired 的 seq 连续 |
| 断言 4 | 重新以 Replay 模式执行 Workflow → `ctx.Sleep(1*time.Hour)` 0ms 返回（不是哨兵错误），跳过 TimerStarted 和 TimerFired 两个事件 |

---

### 2.4 lock 包 — 会话排它锁

**测试用例 5：脑裂双锁拦截**

| 维度 | 说明 |
|---|---|
| 测试目标 | 两个 Agent 同时申请同一 session 的锁，只有一个成功 |
| 前置条件 | mock Redis——`SETNX` 语义正确模拟（首次返回 1，后续返回 0），sessionID="test:lock" |
| 测试步骤 | goroutine A 调 `AcquireSessionLock("test:lock", "agent-a")` → goroutine B 同时调 `AcquireSessionLock("test:lock", "agent-b")` |
| 断言 1 | A 的调用返回 `FencingLease{Token=1}` |
| 断言 2 | B 的调用返回 error，error 消息含 `"locked by another agent"` |
| 断言 3 | `session_fencing:test:lock` 的值为 1（只被 A 递增了一次） |

**测试用例 5b：Fencing Token 过期后拒绝写入**

| 维度 | 说明 |
|---|---|
| 测试目标 | GC 卡顿导致锁过期后，原 Agent 的写入被拒绝 |
| 前置条件 | A 持有锁（Token=1），模拟 40s 后 TTL 过期，B 抢占锁（Token=2） |
| 测试步骤 | A 醒来后调 `ValidateFencingToken(lease{Token=1})` |
| 断言 | 返回 `(false, nil)`——Token 不匹配。A 的后续写入被拒绝 |

---

## 3. Python 执行层：物理工具与安全对抗测试

### 3.1 executor 包 — 绝对路径防越权

**测试用例 6：路径穿越拦截矩阵**

| 测试输入（恶意路径） | 攻击向量 | 期望结果 |
|---|---|---|
| `../../etc/passwd` | 向上穿越出 workspace | `PermissionError` |
| `./../workspaces/other_session/script.py` | 水平穿越到其他 session | `PermissionError` |
| `/tmp/symlink_to_root`（符号链接指向 `/`） | 符号链接绕过前缀校验 | `PermissionError`（realpath 解析后前缀校验失败） |
| `workspaces/malicious/../../etc/shadow` | 混合相对路径 + 向上穿越 | `PermissionError` |
| `正常路径：findings/report.md` | 合法路径（对照） | 通过，正常执行 |

**断言**：所有恶意路径被 `validatePath` 在第 1 层（前缀校验）或第 2 层（realpath 符号链接追踪）拦截。Go 层收到 `AgentFailed` 事件（`PERMISSION_DENIED`），不重试。

---

### 3.2 executor 包 — 进程超时控制

**测试用例 7：死循环子进程强制 Kill**

| 维度 | 说明 |
|---|---|
| 测试目标 | 恶意无限循环脚本在超时后被 SIGKILL |
| 前置条件 | 创建临时 Python 脚本：`import time; time.sleep(999999)` |
| 测试步骤 | 调 `ShellTool.execute(command="python deadloop.py", timeout=2)` |
| 断言 1 | 2 秒后 `asyncio.wait_for` 抛出 `TimeoutError` |
| 断言 2 | 子进程被发送 `SIGKILL`（通过 `process.returncode == -9` 或 OS 进程列表检查） |
| 断言 3 | 执行器返回 `ToolResult{is_error=true, stderr含"timeout"}`。Go 层正常收到，不崩溃 |
| 断言 4 | 宿主机的 CPU 使用率在子进程被 Kill 后恢复正常（无僵尸进程） |

---

### 3.3 adapters 包 — Pydantic 异常数据注入

**测试用例 8：损坏的 LLM 响应拦截**

| 测试输入 | 缺陷描述 | 期望结果 |
|---|---|---|
| `{"tool_calls": [{"tool_name": "shell"}]}` | 缺少 `call_id` 和 `arguments` | `ValidationError`——`call_id` 字段必填缺失 |
| `{"tool_calls": [{"call_id": "x", "tool_name": "shell", "arguments": "not_json"}]}` | arguments 非合法 JSON | `ValidationError`——`arguments` 无法解析为 dict |
| `{"tool_calls": [{"call_id": "x", "tool_name": "shell", "arguments": {"cmd": 123}}]}` | arguments 类型错误（cmd 应为 string） | `ValidationError`——根据工具 JSON Schema 动态校验，`cmd` 期望 string 实际 int |
| `{"tool_calls": [], "content": "正常文本"}` | 合法响应（对照） | 通过，正常返回 |

**断言**：所有非法响应被 Pydantic v2 拦截在 Python 本地。gRPC 返回 `INVALID_ARGUMENT`。Go 层不重试。脏数据不进入 SQLite。

---

## 4. 确定性重放回归测试（Replay Regression Tests）

这是 Tenet 的杀手级测试——防止 Workflow 代码变更无意中破坏历史任务的重放确定性。

### 4.1 测试种子管理

预生成 event_log 快照文件存放在 `testdata/replay/`：

| 种子文件 | 内容 | 事件数 |
|---|---|---|
| `simple_v1.jsonl` | SimpleWorkflow v1 代码生成 | 3 |
| `react_v1.jsonl` | ReactWorkflow v1，含 5 轮 Thought→Tool 循环 | 15 |
| `dag_v1.jsonl` | DAGWorkflow v1，3 个子任务 + 汇总 | 8 |
| `forked_v1.jsonl` | Fork 流，含 parent_id 谱系 | 6 |
| `versioned_v1.jsonl` | 含 2 个 VersionMarker 的流 | 7 |

每份种子是 `{stream_id, stream_seq, event_type, payload, parent_id, timestamp}` 的 JSONL 文件。

### 4.2 Three-Zero 断言

对每份种子执行回放测试，必须满足：

| 断言 | 条件 | 失败含义 |
|---|---|---|
| **Zero-1** | `historyPos == len(history)` | 历史事件未被完全消耗——Workflow 代码的控制流与历史事件流不匹配（某些历史事件未被访问） |
| **Zero-2** | `len(newEvents) == 0` | 回放过程中产生了新事件——Workflow 代码新增了 `Decide` 或 `Record` 调用但没有用 `GetVersion` 标记 |
| **Zero-3** | mock gRPC 客户端调用次数 == 0 | 回放过程中发起了真实的外部 I/O——某个 `Decide` 未从历史中读取缓存结果，而是实际执行了 `fn()` |

**如果任何断言失败**：测试框架 `FAIL`，阻断 CI。错误信息明确指出哪个 stream、哪个 seq 位置、期望的类型与实际类型的差异。开发者必须用 `GetVersion(changeID, newVersion)` 标记变更，或修复代码使控制流与历史事件流一致。

### 4.3 回归测试流程

1. CI 加载所有 `testdata/replay/*.jsonl` 种子文件
2. 对每个种子：写入 in-memory SQLite → `NewWorkflowContext(mode=Replay)` → 执行对应的 Workflow 函数
3. 运行 Three-Zero 断言
4. 全部种子通过 → CI 继续。任一失败 → CI 阻断

---

## 5. 集成测试

### 5.1 gRPC 通信

| 场景 | 验证点 |
|---|---|
| Go GenerateThought → Python mock LLM | 正确的 AdapterResponse 返回 |
| Go ExecuteTool → Python Shell 执行 | stdout/stderr/exit_code 正确 |
| Python 启动后 RegisterAgent | WorkerRegistry 正确注册 |

### 5.2 SQLite 事务

| 场景 | 验证点 |
|---|---|
| 并发 10 Task 写入 | WriteDaemon 串行执行，无 BUSY |
| Append 中 panic | 事务回滚，seq 不递增 |

### 5.3 Redis 锁

| 场景 | 验证点 |
|---|---|
| 两 Agent 竞争锁 | SETNX 只允许一个 |
| Redis 宕机 | 降级到本地锁，State 通道正常写入 |

---

## 6. 混沌测试

| 场景 | 模拟 | 验证点 |
|---|---|---|
| Decide 执行中 OOM | `fn()` 内 `os.Exit(1)` | 重启回放跳过已落盘事件，从断点继续 |
| Decide 落盘后崩溃 | `Append` 后 `os.Exit(1)` | 事件已持久化，回放 0ms 返回 |
| Workspace 半成品 | Agent 子任务执行中 `os.Exit(1)` | `Backup` 存在 → `Restore` 还原 → 重新执行 |

---

## 7. CI 集成

```yaml
unit:
  go test ./go/internal/... -v -count=1
  cd python && pytest -v

integration:
  go test ./go/... -tags=integration -v
  # 需要 Redis + Python gRPC

replay-regression:
  go test ./go/internal/workflow/... -run TestReplay -count=1 -v
  # 阻断条件：任何 Three-Zero 断言失败

chaos:
  go test ./go/... -tags=chaos -v -timeout=5m
```
