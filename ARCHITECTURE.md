# Tenet Architecture

> 轻量级、事件溯源、多智能体 Agent 框架
> 
> Go（编排引擎）+ Python（执行引擎）两层架构
>
> 核心原则：**Event Log is the single source of truth**

---

## 一、系统定位

Tenet 是一个两层 Agent 框架。Go 层负责编排——自建确定性工作流引擎，用事件溯源替代 Temporal。Python 层负责执行——LLM 调用、Agent Loop、Swarm 多智能体协作、工具执行。

与已学项目的关系：

| | Shannon | Crush | Tenet |
|---|---|---|---|
| 语言 | Go + Rust + Python | Go | Go + Python |
| 工作流引擎 | Temporal（重依赖） | 无 | 自建 Event Sourcing |
| 可回放 | Temporal 原生 | 无 | 事件溯源原生 |
| 可分支 | 不支持 | 不支持 | Fork 原生 |
| LLM 调用 | Python llm-service | Go fantasy 库 | Python 层统一 |
| 多 Agent | Swarm（Lead Agent） | Coordinator（静态） | Python 层 Swarm |
| 工具执行 | Rust WASI 沙箱 | Go 原生 | Python 层本地执行 |

核心取舍：**不用 Temporal，Go 层自己实现事件溯源工作流引擎**。Temporal 给持久化/可视化/回放，Tenet 用 Event Store + Projection + Fork 实现同等能力，零外部依赖、完全控制。

---

## 二、系统全景

```
┌──────────────────────────────────────────────┐
│                 Go 层（编排引擎）               │
│                                              │
│  CLI (Cobra) ──→ Config ──→ Storage Init     │
│                     │                        │
│    ┌────────────────┼────────────────┐       │
│    ▼                ▼                ▼       │
│  gRPC Server    Workflow Engine   Redis Pool │
│  (gateway)      (workflow)       (storage)   │
│    │                │                │       │
│    │  事件入站       │  调 Python     │       │
│    ▼                │                │       │
│  Event Router ──────┤                │       │
│  (channel)    State │ Stream         │       │
│    │          通道   │ 通道           │       │
│    ▼                ▼                ▼       │
│  Event Store    Projection      SSE Publisher│
│  (SQLite)       Engine          (Redis)      │
│    │                │                        │
│    └────────┬───────┘                        │
│             │ 读状态                          │
│             ▼                                │
│    WorkflowContext                           │
│    Decide / Record / Fork / Replay           │
│             │                                │
│    ┌────────┼────────┐                       │
│    ▼        ▼        ▼                       │
│  Simple  DAG     Swarm                       │
│  Workflow                                        │
│                                              │
│  Token Budget · Lock Manager · Health Check  │
└──────────────────────┬───────────────────────┘
                       │ gRPC
┌──────────────────────▼───────────────────────┐
│              Python 层（执行引擎）             │
│                                              │
│  gRPC Client ──→ Agent Loop (ReAct)          │
│                      │                       │
│         ┌────────────┼────────────┐          │
│         ▼            ▼            ▼          │
│    LLM Provider  Swarm Engine  Tool Registry │
│    (OpenAI etc)  (Lead Agent)  (file/shell..)│
│                                              │
│  Qdrant Client（保留接口，暂不实现）            │
└──────────────────────────────────────────────┘
```

---

## 三、两层职责划分

### Go 层 —— 编排引擎

Go 层不调 LLM API。它的工作是管理 Task 的生命周期、决定「什么时候该做什么」、记录每一步发生了什么。

| 组件 | 职责 | 核心约束 |
|---|---|---|
| Workflow Engine | 确定性执行、回放、Fork 分支、版本控制 | 所有非确定性操作必须通过 Decide |
| Event Store | append-only 事件持久化（SQLite），谱系查询 | 事务保证 seq 连续，不缓存 |
| Event Router | 接收 Python 事件，分叉 State/Stream 双通道 | State 同步阻塞，Stream 异步 |
| Projection Engine | 从事件流 fold 当前状态，快照管理 | 双触发器（事件数 + 时间） |
| Strategy Router | 复杂度分析 → 选 Workflow 类型 | 分析结果持久化到事件流 |
| gRPC Gateway | Python 层唯一入口，middleware（超时/重试/断路器） | 断路器状态机 |
| Token Budget | Token 记账聚合 | Guard Pattern 防重复 |
| Lock Manager | Session-Workspace 排它锁 + Tool Rate-Limit | Redis 不可用时降级 |
| Config | 静态配置（yaml）+ 动态配置（事件流） | 启动时加载，运行时通过事件变更 |

### Python 层 —— 执行引擎

Python 层不管「该不该做」。它的工作是接到 Go 层的 ExecuteAgent 请求后，跑 Agent Loop、调 LLM、执行工具、回传事件。

| 组件 | 职责 |
|---|---|
| Agent Loop | ReAct 循环（Thought → Action → Observation），收敛控制 |
| LLM Provider | 统一 OpenAI/Anthropic/DeepSeek 接口 |
| Swarm Engine | Lead Agent 事件驱动：任务分配、质量检查、进度跟踪 |
| Tool Registry | 工具注册 + JSON Schema，供 LLM tool_use |
| Qdrant Client | 向量记忆检索接口（保留，暂不实现） |

### 两层通信：gRPC

```
Go → Python：ExecuteAgent(task_id, agent_config, messages, tools)
Python → Go：AgentEvent stream（实时流式回传）
  - AgentStarted, ThoughtGenerated（含 LLM 原始输出）
  - ToolCalled, ToolResulted
  - AgentCompleted / AgentFailed
```

proto 定义共享于 `proto/tenet/v1/tenet.proto`。

---

## 四、工作流引擎设计

这是 Tenet 最核心的部分。Go 层自建确定性执行引擎，对标 Temporal 的工作流能力子集。

### 4.1 核心概念

同一段 Workflow 代码，两种运行模式：

**执行模式**：Workflow 是「生产者」。调用外部服务，产生事件。事件流从空到有。

**回放模式**：Workflow 是「消费者」。消费已有事件流，从事件中取结果。不产生新事件。

引擎由五个概念组成：

| 概念 | 干什么 | 为什么存在 |
|---|---|---|
| WorkflowContext | Workflow 函数的唯一参数，封装所有外部交互 | 隔离非确定性，同一代码两种运行模式 |
| Decide | 所有外部调用的入口 | 执行模式真实调用，回放模式读缓存 |
| Record | 记录纯状态变更 | 回放时做确定性检查（类型比对） |
| GetVersion | 标记代码变更点 | 让新代码能回放旧事件 |
| Fork | 从任意事件点分叉 | 事件溯源核心价值：What-If 和错误恢复 |

### 4.2 WorkflowContext

```go
type WorkflowContext struct {
    store      EventStore
    streamID   string        // 物理流标识，如 "task:<uuid>"
    parentID   string        // fork 来源（空 = 首次执行）

    history    []Event       // 从 SQLite 加载的历史（回放时非空）
    historyPos int           // 当前读取位置

    newEvents  []Event       // 本轮新事件（回放时为空）
    version    int           // 当前 Workflow 代码版本
}
```

三个核心方法：

```go
// 决策点：所有非确定性操作必须通过此方法
// 执行模式 → 实际调用 fn() → 记录结果到 newEvents
// 回放模式 → 从 history 读取缓存结果 → 跳过 fn()
func (ctx *WorkflowContext) Decide(decisionType string, fn func() (any, error)) (any, error)

// 确定性记录：不调外部，只记录状态变更
// 回放时通过确定性检查确保控制流一致
func (ctx *WorkflowContext) Record(eventType string, payload any)

// 版本控制：标记代码变更点
// 回放时从事件中读版本号，决定走新逻辑还是旧逻辑
func (ctx *WorkflowContext) GetVersion(changeID string, minVersion int) int
```

决策点类型——所有破坏确定性的外部操作：

```
ExecuteAgent   → gRPC 调 Python 层（LLM 结果不保证相同）
Sleep          → time.Sleep（两次执行时长不同）
Now            → time.Now()
Random         → 随机数
SideEffect     → 写文件、发网络请求
```

**硬约束**：Workflow 函数内部不能直接调 `time.Now()`、`time.Sleep()`、`http.Get()`。必须通过 WorkflowContext。Go 编译器无法在编译期检查——这靠代码审查保证。

### 4.3 立即提交（Immediate Commit）—— 替代批量 Flush

**设计原则：每一个 Decide 在获得外部返回结果后，必须在当前步骤立即写入 SQLite 并提交事务，然后才能执行下一行代码。**

禁止批量 Flush。原因：如果 Decide 执行成功（Python 跑了 30 秒、花了 token、改了文件），但事件还在内存的 newEvents 里没落盘，此时进程崩溃 → 事件全部丢失 → 重启后系统以为没执行过 → 重复执行 → 重复消耗 token + 面对已被修改的脏工作区。

Decide 的内部逻辑：

```go
func (ctx *WorkflowContext) Decide(decisionType string, fn func() (any, error)) (any, error) {
    // 回放路径：历史事件流中还有事件 → 读缓存，不执行
    if ctx.historyPos < len(ctx.history) {
        event := ctx.history[ctx.historyPos]
        if event.Type != decisionType {
            panic(fmt.Sprintf("non-determinism at pos %d in stream %s: expected %s, got %s",
                ctx.historyPos, ctx.streamID, decisionType, event.Type))
        }
        ctx.historyPos++
        return event.Payload.Result, nil
    }

    // 首次执行路径：实际调用 → 立即落盘 → 再返回
    result, err := fn()
    
    // ★ 立即写入 SQLite（事务内完成），不缓冲
    event := Event{
        StreamID:  ctx.streamID,
        StreamSeq: ctx.nextSeq(),
        Type:      decisionType,
        Payload:   DecisionPayload{Result: result, Error: err},
    }
    ctx.store.Append(event)  // 同步写入 + 事务提交
    ctx.history = append(ctx.history, event)  // 更新内存历史
    
    return result, err
}
```

Record 采用不同的策略——**延迟批量提交**。Record 记录的是纯确定性事件，即使 crash 丢失也可以从回放中重建。Record 事件在内存中缓冲，达到阈值（默认 20 条）或 Workflow 结束时一次性写入：

```go
func (ctx *WorkflowContext) Record(eventType string, payload any) {
    // Record 可以缓冲，因为 crash 后回放会重新生成
    ctx.pendingRecords = append(ctx.pendingRecords, Event{...})
    if len(ctx.pendingRecords) >= ctx.recordBatchSize {
        ctx.flushRecords()
    }
}
```

提交模型对比：

```
═══ 首次执行 ═══
SimpleWorkflow(ctx, task)
  → Record("TaskStarted")      → 缓冲在 pendingRecords（未落盘）
  → Decide("AgentExecuted")    → gRPC 调 Python → 拿到结果
                               → ★ 立即 INSERT event_log + COMMIT
                               → 从这一行起，事件已持久化
  → Decide("AgentRetry")       → gRPC 调 Python → 拿到结果
                               → ★ 立即 INSERT event_log + COMMIT
  → Record("TaskCompleted")    → 缓冲在 pendingRecords
  → ctx.Commit()               → flushRecords: INSERT 两条 Record 事件

崩溃在任何位置的恢复：
  崩溃在 AgentExecuted 之后 → event_log 已有 AgentExecuted
                            → 重启后回放，跳过该 Decide，从下一行继续
  崩溃在 AgentRetry 之前    → AgentExecuted 已落盘但后续未执行
                            → 重启回放跳过 AgentExecuted，继续执行 AgentRetry


═══ 回放（crash 后恢复） ═══
Context{history: [TaskStarted, AgentExecuted{result}, AgentRetry{result}, TaskCompleted]}

SimpleWorkflow(ctx, taskFromHistory)
  → Record("TaskStarted")      → 命中 history[0]，跳过
  → Decide("AgentExecuted")    → 命中 history[1]，不调 Python，直接返回 result
  → Decide("AgentRetry")       → 命中 history[2]，不调 Python
  → Record("TaskCompleted")    → 命中 history[3]，跳过

Commit: 无新事件落盘
```

### 4.4 确定性检查

回放时不做「值比对」，只做「类型比对」。引擎不检查 AgentExecuted 的 result 是否和上次一样——LLM 调用结果本来就不可能相同。引擎只检查：在事件流的位置 N，代码期望的决策类型和历史事件类型是否一致。

这也意味着：改 Workflow 的纯逻辑（加减 if 判断，只要不改 Decide/Record 的调用顺序），回放不受影响。改了 Decide/Record 的调用顺序，就必须用 GetVersion 标记。

### 4.5 物理标识化与分支路由

事件溯源的核心价值不是「存历史」，而是从任意事件点分叉。

**物理流 ID 的树形结构**：

```
stream_id = "task:<uuid>"              // 主分支
stream_id = "task:<uuid>/fork:<N>"     // 从第 N 个事件 fork
stream_id = "task:<uuid>/fork:<N>/replay:<R>"  // 回放产生的子分支
```

流 ID 编码了谱系（lineage），从 ID 就能看出执行是从哪来的。

**Fork 操作**：读取原流的前 N 个事件 → 物理复制到新流 → 以 historyPos=N 初始化 Context → 从 N 之后开始执行新 Workflow。新流完全独立，主分支删除不影响 fork 分支。

两个核心场景：

1. **What-If 探索**：Agent 在某个决策点走了路径 A，Fork 看路径 B 的结果。两条分支独立。
2. **错误恢复**：Agent 第 3 步读错文件，后续全错。Fork 到第 3 步修正，原分支保留用于分析。

**谱系查询**：

```
GetLineage("task:abc/fork:2/replay:1")
  → ["task:abc", "task:abc/fork:2", "task:abc/fork:2/replay:1"]

GetChildStreams("task:abc")
  → ["task:abc/fork:2", "task:abc/fork:5"]
```

### 4.6 版本控制

Workflow 代码演进时，旧事件不能丢。GetVersion 机制：

```go
func SimpleWorkflow(ctx *WorkflowContext, task Task) error {
    ctx.Record("TaskStarted", ...)

    v := ctx.GetVersion("add-retry-logic", 2)
    if v >= 2 {
        // 新逻辑：失败后重试
        result, err := ctx.Decide("AgentExecuted", ...)
        if err != nil {
            result, err = ctx.Decide("AgentRetry", ...)
        }
    } else {
        // 旧逻辑：不重试
        result, err := ctx.Decide("AgentExecuted", ...)
    }

    ctx.Record("TaskCompleted", ...)
    return nil
}
```

回放旧事件时，GetVersion 在 history 中找不到 VersionMarker → 返回 0 → 走旧逻辑。确定性检查通过。

版本号不是全局的——每个变更独立命名。SimpleWorkflow 可以有多个 GetVersion 调用，每个对应一个具体的代码变更。回放一条旧事件流时，有些变更标记存在（走新逻辑），有些不存在（走旧逻辑），各自独立。

### 4.7 两层事件流分离

Task 级和 Agent 级的事件流是两条物理分离的流。

**Task Stream**（`task:<uuid>`）：Go 层编排逻辑产生。TaskStarted → AgentExecuted → TaskCompleted。Workflow 引擎只回放 Task Stream。

**Agent Stream**（`agent:<name>`）：Python 层执行逻辑产生。AgentStarted → ThoughtGenerated → ToolCalled → ToolResulted → AgentCompleted。不参与 Workflow 回放——用于 Web UI 可视化和 Agent 级调试。

分离原因：Task 流和 Agent 流混在一条流中，Task 级回放会极慢（一个 Task 可能含几十个 Agent 事件）。分离后 Task 级回放只需读几个事件。

### 4.8 Workflow 注册与执行

```go
// 注册
Registry.Register("simple", SimpleWorkflow)
Registry.Register("dag", DAGWorkflow)
Registry.Register("swarm", SwarmWorkflow)

// 三种 Context 初始化模式
首次执行：history=[], historyPos=0, parentID=""
回放：    history=store.Read(streamID), historyPos=0
Fork：    history=store.Read(parentID, 0, forkSeq), historyPos=len(history)

// Commit：Workflow 执行完毕后 flush 剩余的 Record 事件
Commit 流程：
  ctx.flushRecords()  // 写入所有未落盘的 Record 事件
  // Decide 事件已经在调用时立即落盘，不需要额外 flush
```

### 4.9 工作区备份与恢复 —— 防止 Side-Effect 污染

**问题**：Agent 执行过程中修改了 workspace 文件（写文件、改文件、跑脚本），执行失败后重试，workspace 处于「脏状态」——第二次执行面对的是上次跑了一半的文件，推理基础全错。

**方案**：Go 层在执行任何涉及文件修改的 Decide（ExecuteAgent、SideEffect）之前，自动备份 workspace 目录。如果 Decide 失败并触发重试，先恢复 workspace 到备份状态，再拉起 Python Agent。

```
ExecuteAgent 重试流程：

1. Decide("AgentExecuted") 开始
2. WorkspaceManager.Backup(sessionID) → 复制 workspace 到 .backup/
3. gRPC → Python: ExecuteAgent(...)
4. Python 执行 Agent Loop（修改 workspace 文件）
5. 如果成功 → WorkspaceManager.CleanBackup(sessionID) → 删除备份
6. 如果失败 →
     WorkspaceManager.Restore(sessionID) → 清空 workspace，从 .backup/ 还原
     Decide 重新执行（从步骤 2 开始）
```

备份粒度：整个 session workspace 目录。不是单个文件——因为 Agent 的操作通常是多文件的，必须整体还原到干净状态。

备份存储：在 workspace 同目录下创建 `.backup/<session_id>/`。不使用 SQLite 存备份——文件系统操作更快且语义清晰。

---

## 五、存储架构：三层数据库协同

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│    SQLite     │  │    Redis      │  │   Qdrant      │
│  权威状态存储  │  │ 实时流 + 锁   │  │ 向量知识库    │
│              │  │              │  │ (暂不实现)    │
│ · Sessions   │  │ · SSE Stream │  │ · Embeddings │
│ · Event Log  │  │ · Session    │  │ · Episodic   │
│ · Snapshots  │  │   Lock       │  │   Memory     │
│ · Token      │  │ · Tool RL    │  │ · Long-Term  │
│   Telemetry  │  │   Lock       │  │   Memory     │
└──────────────┘  └──────────────┘  └──────────────┘
     ↑ 状态通道       ↑ 流+锁通道        ↑ 知识通道
```

### 5.1 SQLite —— 权威状态存储

| 表 | 用途 | 写入时机 |
|---|---|---|
| `sessions` | 会话元数据（id, workspace_path, status, created_at） | 会话创建/状态变更 |
| `event_log` | 事件溯源账本（append-only，不可变） | 每一步状态变更 |
| `snapshots` | Projection 快照（stream_id, seq, state_blob） | 每 50 事件或每 5 分钟 |
| `token_telemetry` | Token 用量 + 成本 | 每次 LLM 调用后 |

`event_log` 表结构：

```
id            INTEGER PRIMARY KEY AUTOINCREMENT
stream_id     TEXT    -- "task:<uuid>" | "task:<uuid>/fork:3"
stream_seq    INTEGER -- 流内序号
event_type    TEXT    -- TaskStarted, AgentExecuted, ToolCalled...
payload       JSON    -- 完整事件数据（含 LLM 原始输出）
parent_id     TEXT    -- fork 来源流 ID（空 = 主分支）
timestamp     TEXT    -- ISO 8601
```

**Append 必须在事务中完成**，保证同一流内 seq 连续无空洞。Read 不做缓存——Event Store 是数据的唯一权威，缓存放在 Projection 层。

**SQLite 并发写入瓶颈与解决方案**：

SQLite 的 WAL 模式支持多读单写，但事件溯源架构下所有 Task 都在高频 Append 事件。同时跑 50 个 Task，每个都在写 event_log → SQLite 频繁返回 `SQLITE_BUSY` → 大量排队延迟。

解决方案：**单写守护协程（Write-Daemon Goroutine）**。所有需要写 SQLite 的操作不直接执行 INSERT，而是把写请求丢进一个内存 channel。由一个专用的 goroutine 消费 channel，单线程串行写入。

```
所有 goroutine ──→ writeCh chan WriteRequest ──→ 单写守护协程
                                                      │
                                                SQLite INSERT + COMMIT
                                                      │
                                                resultCh ← 返回结果
```

实现约束：

```go
// 写请求结构
type WriteRequest struct {
    SQL     string
    Args    []any
    ResultCh chan WriteResult  // 同步等待结果
}

// 守护协程
func (w *WriteDaemon) Run() {
    for req := range w.writeCh {
        tx := db.Begin()
        tx.Exec(req.SQL, req.Args...)
        tx.Commit()
        req.ResultCh <- WriteResult{...}
    }
}

// 初始化
storage.Init() {
    MaxOpenConns = 1   // SQLite 驱动层只开一个连接
    启动 WriteDaemon goroutine
}
```

- 所有写操作排队到单 goroutine → 物理上不可能发生 `SQLITE_BUSY`
- 读操作不受影响——WAL 模式下多个 goroutine 可以并发读
- decide 的立即写入 → 把 WriteRequest 发到 channel → 阻塞等待 resultCh → 拿到确认后继续

### 5.2 Redis —— 实时流通道 + 分布式锁

| 用途 | 机制 | 生命周期 |
|---|---|---|
| SSE Stream Channel | Pub/Sub → Web UI 实时推送 | 事件产生即推送，消费后丢弃 |
| Session-Workspace Lock | SETNX + TTL 30s + 心跳续约 | 会话期间持有 |
| Tool Rate-Limit Lock | Lua 原子令牌桶 | 滑动窗口内计数 |

Redis 中的数据是瞬时的。重启后 Redis 为空不影响系统恢复——所有状态从 SQLite Event Log 重建。

### 5.3 Qdrant —— 向量知识库（保留结构，暂不实现）

| 用途 | 存储内容 |
|---|---|
| Document Embeddings | 外部知识库文档向量 |
| Episodic Memory | Agent 执行经验向量 |
| Long-Term Memory | 跨会话偏好/事实向量 |

---

## 六、State/Stream 物理双通道分流

核心问题：**实时回传事件到 Go 层后，既要写入 SQLite（持久化），又要推送到 Web UI（实时可视化）。如果走同一条路径，数据库写延迟会阻塞 UI 推送。**

解决方案：Event Router 物理分叉。

```
Python 层 ──gRPC stream──→ Go 层 Event Router
                               │
                ┌──────────────┴──────────────┐
                │                             │
        State 通道（同步阻塞）          Stream 通道（异步非阻塞）
                │                             │
        INSERT INTO event_log          PUBLISH redis-channel
        更新 Projection 缓存            Web UI SSE 消费
        触发快照判断                    fire-and-forget
        返回 ACK 给 Python
```

**关键约束**：
- State 通道同步阻塞——事件必须落盘才 ACK。Python 层不收到 ACK 不会继续执行，保证事件不丢。
- Stream 通道异步非阻塞——独立 goroutine，Redis 不可用时降级，不影响 State 通道。
- Web UI 历史回看走 State 通道（SQLite），实时订阅走 Stream 通道（Redis）。

---

## 七、分布式锁设计

### 7.1 会话级物理目录排它锁（Session-Workspace Lock）+ Fencing Token

每个 Tenet session 分配独立 workspace 目录。Redis 锁保证同一 session 同一时间只有一个 Agent 写文件。

**基础锁机制**：

```
Key:    session_lock:<session_id>
Value:  agent_id
TTL:    30s

获取：SETNX session_lock:<session_id> <agent_id> EX 30
释放：Lua script: if GET == agent_id then DEL
续约：EXPIRE session_lock:<session_id> 30（Agent 每 10s 续约）
```

**Fencing Token —— 防止 GC 卡顿/网络延迟导致的脑裂写入**：

经典陷阱：Agent 因为 GC 卡顿 40 秒，锁自动过期，另一个 Agent 抢占锁并开始写 workspace。原 Agent 醒来后也继续写——两个进程同时操作同一物理目录，数据静默损毁。

Fencing Token 方案：

```
锁结构扩展：
  Key:    session_lock:<session_id>
  Value:  {"agent_id": "coder-1", "fencing_token": 17}

每次锁获取或续约成功：
  fencing_token = INCR(session_fencing:<session_id>)
  更新锁 Value 中的 fencing_token

Python 层执行任何文件写入前：
  校验本地持有的 fencing_token == Redis 中的当前值
  如果不相等 → 说明锁已被抢占 → 拒绝写入 → 自我终止

Go 层 WorkspaceManager 同样校验：
  Backup/Restore/CleanBackup 操作前先校验 fencing_token
```

为什么是目录级而非文件级？Agent 的工作通常是多文件操作（读 A、改 B、写 C），文件级锁会导致死锁。目录级锁语义清晰：「这个 workspace 现在是我的」。

并发语义：排它但不阻塞读取。其他 Agent 可读 workspace 文件，如需读一致性从快照读。

### 7.2 工具级频次限制锁（Tool Rate-Limit Lock）

部分工具有天然频次限制（Shell 命令、LLM API rate limit）。用 Redis Lua 原子令牌桶：

```
Key:   tool_rl:<tool_name>:<window_ts>
Value: count
TTL:   窗口时长 + 1s

Lua 原子检查+递增：
  count = INCR(key)
  if count == 1 then EXPIRE(key, window_ttl)
  if count > limit then return 0  // 超限拒绝
  return 1  // 允许
```

配置示例（`tenet.yaml`）：

```yaml
tool_rate_limits:
  shell:
    max_per_minute: 30
    max_per_second: 5
  web_search:
    max_per_minute: 10
  write_file:
    max_per_second: 20
```

### 7.3 降级策略

Redis 不可用时：
- Session Lock 不生效（多 Agent 可并发写 workspace——依赖 SQLite 串行写做最后防线）
- Tool Rate Limit 回退到 Python 层本地内存限流（不跨进程）

---

## 八、Go 层组件详述

### 8.1 Event Store（`internal/event/`）

接口：

```
Append(streamID, parentID, eventType, payload) → (streamSeq, error)
Read(streamID, fromSeq, toSeq) → []Event
GetLineage(streamID) → []string
GetChildStreams(streamID) → []string
```

Append 在事务中完成，保证 seq 连续。Read 不做缓存。谱系查询通过 parent_id 字段递归。

### 8.2 Event Router（`internal/channel/`）

gRPC Gateway 收到 Python 事件后，Router 立即分叉到 State Channel 和 Stream Channel。两条通道的处理在独立 goroutine 中运行。

- State Channel：接收事件 → EventStore.Append() → ACK 回 Router
- Stream Channel：接收事件 → Redis PUBLISH → fire-and-forget

### 8.3 Projection Engine（`internal/projection/`）

接口：

```
Project(streamID) → ProjectionState
Snapshot(streamID, state) → error
```

快照触发策略：每 50 个事件 或 每 5 分钟（双触发器并存，避免低流量时长时间无快照）。

恢复逻辑：查 snapshots 表最新快照 → 反序列化 → 读快照 stream_seq 之后的事件 → fold。无快照则从头 fold 全部事件。

三种 Projection 类型：

| Projection | 消费事件 | 产出 |
|---|---|---|
| TaskProjection | Task 流事件 | Task 当前状态、子任务、结果 |
| TimelineProjection | Agent 流事件 | 时间线（Web UI 渲染用） |
| TokenProjection | TokenUsed 事件 | 累计 token 用量 |

### 8.4 Strategy Router（`internal/workflow/router.go`）

调 Python 层 LLM 做复杂度分析（不占 Task token 预算）。LLM 返回评分 (0-1) + 推荐策略 + 理由。分析结果持久化为 ComplexityAnalyzed 事件。

路由规则：

```
复杂度 < 0.3   → SimpleWorkflow
复杂度 0.3~0.7 → DAGWorkflow
复杂度 > 0.7   → SwarmWorkflow
用户可覆盖（Task 参数中指定 workflow_type）
```

### 8.5 gRPC Gateway（`internal/gateway/`）

proto 定义：

```
service TenetGateway {
    rpc RegisterAgent(RegisterRequest) returns (RegisterResponse);
    rpc ExecuteAgent(AgentRequest) returns (stream AgentEvent);
    rpc PublishAgentEvent(AgentEvent) returns (AckResponse);
}
```

Middleware 三层：

```
请求进入 → 超时检查（context deadline）
        → 重试判定（可重试的错误 + 最大次数）
        → 断路器（连续失败 N 次 → 熔断 M 秒 → 半开尝试）
        → 实际处理
```

断路器状态机：CLOSED → (连续失败 N 次) → OPEN → (等待 M 秒) → HALF_OPEN → (成功 → CLOSED | 失败 → OPEN)

### 8.6 Token Budget（`internal/budget/`）

Token 记账，不决定预算额度。Guard Pattern 防重复——每条 LLM 调用只记一次。零 token 执行（纯工具调用）不记录。TokenUsed 事件写入 event_log，TokenProjection 聚合。

### 8.7 Lock Manager（`internal/lock/`）

**职责**：管理两种 Redis 锁，提供统一接口，包含 Fencing Token 机制。

接口：

```
AcquireSessionLock(sessionID, agentID) → (lease FencingLease, error)
    获取会话锁，返回租约（含 fencing_token）
    内部：SETNX + INCR fencing_token

RenewSessionLock(lease) → (newLease FencingLease, error)
    心跳续约，返回新的 fencing_token

ReleaseSessionLock(lease) → error
    Lua 原子释放（检查 fencing_token 持有者）

ValidateFencingToken(lease) → (valid bool)
    文件操作前校验：本地 token == Redis 当前值？
    不相等 → 锁已被抢占 → 拒绝操作

CheckToolRateLimit(toolName) → (allowed bool, retryAfter Duration)
    Lua 原子令牌桶
```

### 8.8 Config（`internal/config/`）

两层配置：
- **静态**（tenet.yaml）：SQLite 路径、Redis 地址、gRPC 端口、Workflow 参数、限频配置、LLM Provider 列表
- **动态**（event_log 中的配置变更事件）：版本号、策略路由规则。通过事件溯源保证配置变更可追溯。

---

## 九、请求生命周期

一个 Task 从创建到完成的完整路径：

```
Phase 1: 创建
  CLI → "帮我分析这个代码库"
      → 生成 task_id，创建 Task Stream
      → Record("TaskCreated", {query, config})
      ↓

Phase 2: 策略路由
  Strategy Router → gRPC → Python LLM 复杂度分析
      → Record("ComplexityAnalyzed", {score, strategy, reason})
      → 选择 SimpleWorkflow
      ↓

Phase 3: Workflow 执行
  WorkflowContext 初始化（streamID="task:<uuid>", history=[]）
  SimpleWorkflow(ctx, task):
    → Decide("AgentExecuted")
         │
         │  gRPC → Python: ExecuteAgent(task_id, messages, tools)
         │  Python 执行 Agent Loop:
         │    AgentStarted → ThoughtGenerated → ToolCalled → ToolResulted
         │    每个事件通过 gRPC stream 回传 Go 层
         │
         │  Event Router 收到:
         │    State 通道 → INSERT event_log (agent stream)
         │    Stream 通道 → Redis PUBLISH → Web UI
         │
         │  Python 返回 AgentCompleted → Decide 拿到 result
         │
    → Record("TaskCompleted", {result})
    → Flush: Task Stream 事件写入 event_log
      ↓

Phase 4: 完成
  结果返回 CLI
  Token Budget 聚合
  快照触发（满足条件时）
```

---

## 十、错误处理分层

| 层 | 错误类型 | 处理方式 |
|---|---|---|
| gRPC Gateway | 网络超时、Python 不可达 | 重试 N 次 → 断路器熔断 → TaskFailed |
| Workflow Engine | Decide 执行失败 | 回写 AgentFailed 事件 → Workflow 决定重试/失败 |
| Event Store | SQLite 写入失败（磁盘满/锁） | 立即失败（不能丢事件）→ TaskFailed |
| Stream Channel | Redis 不可达 | 降级（Warn 日志），不影响 State 通道 |
| Lock Manager | Redis 不可达 | 降级（跳过锁检查），SQLite 串行写做最后防线 |
| Projection | 快照恢复失败 | 从上个快照 + 中间事件重建；无快照则从头 fold |

**兜底原则**：event_log 完整就一切可重建。

---

## 十一、并发模型与安全约束

### 11.1 goroutine 分配

- **主 goroutine**：CLI 命令执行、gRPC Server accept loop、Workflow Engine 主调度
- **每个 Task 一个 goroutine**：Workflow 函数在独立 goroutine 运行，Task 间完全独立（stream_id 隔离）
- **Event Router**：当前 goroutine 内分叉；State 通道通过 WriteDaemon channel 写 SQLite；Stream 通道新 goroutine 异步执行
- **WriteDaemon goroutine**：SQLite 单写守护协程（全局唯一）
- **并发上限**：同时执行的 Workflow 数量可配（默认 10），超过排队

共享资源：SQLite（通过 WriteDaemon 串行写）、Redis 连接池（goroutine-safe）。

### 11.2 Workflow 内部并发安全 —— 禁止原生 go 关键字

**严禁在 Workflow 函数内部使用原生 `go` 关键字启动 goroutine。**

经典陷阱：假设开发者在 Workflow 中写：

```go
// 危险 —— 禁止！
go ctx.Decide("Step_A", ...)
go ctx.Decide("Step_B", ...)
```

首次执行时，由于 OS 线程调度，Step_B 先完成 → 事件序列是 [Step_B, Step_A]。系统崩溃回放时，CPU 调度变化 → Step_A 跑到前面 → 确定性检查失败（expected Step_A, got Step_B） → panic。

**解决方案**：如果需要并发执行多个子任务，使用 WorkflowContext 提供的 `ctx.Async()` 方法：

```go
// 安全 —— ctx.Async() 保证确定性排序
futures := make([]Future, 2)
futures[0] = ctx.Async("Step_A", func() error { ... })
futures[1] = ctx.Async("Step_B", func() error { ... })

// 等待所有完成，Async 内部按注册顺序写入事件
for _, f := range futures {
    f.Wait()
}
```

`ctx.Async()` 的内部保证：
- 子任务可以并发执行，但事件写入强制按注册顺序
- 回放时按注册顺序依次从 history 中读取，不受实际执行顺序影响
- 首次执行时，无论实际哪个先完成，event_log 中的顺序与注册顺序一致

---

## 十二、多 Agent —— Python 层 Swarm

Python 层实现 Lead Agent 事件驱动模式：

```
Lead Agent 循环：
  initial_plan → 分解任务，生成子 Agent
  agent_idle → 分配任务给空闲 Agent
  agent_completed → 质量检查，接受/重试
  checkpoint（每 N 分钟）→ 审查整体进度
  closing_checkpoint → 最终综合
```

Agent 间通信：共享 workspace 文件 + Key Findings 摘要（3-5 条带数据的结论）。

收敛控制：连续 N 次无工具调用 → 强制收敛。

---

## 十三、启动流程

```
1. CLI 解析命令（Cobra）
     tenet serve     → 启动 gRPC Server + Workflow Engine
     tenet task run  → 创建并执行单个 Task
     tenet task replay → 回放指定 Task 流

2. Config 加载
     tenet.yaml → 结构体

3. Storage 初始化
     SQLite: 建库 + 跑迁移（event_log/sessions/snapshots/token_telemetry）
     Redis:  建连接池，PING 验证；不可用 → Warn + 降级
     Qdrant: 不初始化

4. 组件组装（依赖注入，参考 Crush app.go 模式）
     Config → Storage → EventStore → ProjectionEngine → WorkflowEngine
           → EventRouter → gRPC Gateway
           → LockManager → WorkspaceManager → TokenBudget

5. gRPC Server 启动
     等待 Python 层连接并注册

6. 就绪
     Health check 通过，日志：tenet ready on gRPC :50051
```

---

## 十四、目录结构

```
Tenet/
├── go/
│   ├── cmd/tenet/main.go
│   ├── internal/
│   │   ├── event/           # Event Store（SQLite 事件持久化）
│   │   ├── workflow/        # Workflow Engine（Context/Decide/Fork/Replay）
│   │   ├── channel/         # Event Router（State/Stream 双通道）
│   │   ├── projection/      # Projection Engine（状态重建 + 快照）
│   │   ├── gateway/         # gRPC Gateway（middleware 超时/重试/断路器）
│   │   ├── budget/          # Token Budget
│   │   ├── lock/            # Lock Manager（会话锁 + 工具限频）
│   │   ├── config/          # 配置管理
│   │   └── storage/         # SQLite + Redis 连接管理
│   ├── go.mod / go.sum
│
├── python/
│   ├── tenet/
│   │   ├── agent/           # Agent Loop + Swarm + Lead Agent
│   │   ├── llm/             # LLM Provider（OpenAI/Anthropic）
│   │   ├── tools/           # 工具系统（file/shell/search）
│   │   ├── memory/          # Qdrant 客户端（保留）
│   │   ├── gateway/         # gRPC 客户端
│   │   └── proto/           # proto 生成的 Python stub
│   ├── pyproject.toml
│
├── proto/tenet/v1/tenet.proto   # 共享 proto
├── web/                         # Web UI（CLI 之后实现）
├── config/tenet.yaml            # 默认配置
└── ARCHITECTURE.md
```

---

## 十五、已定决策

| 决策 | 结论 |
|---|---|
| 语言 | Go + Python 两层 |
| 工作流引擎 | Go 层自建（确定性执行 + 事件溯源 + 回放 + Fork） |
| 物理标识化 | stream_id 树形结构（`task:<uuid>/fork:<N>`） |
| 分支路由 | Fork：物理复制基础事件，新流独立执行 |
| 事件存储模式 | Event Sourcing（存结果，不存意图） |
| 事件粒度 | 三层（Task/Agent/System），LLM 原始输出全存 |
| 存储 | SQLite（权威）+ Redis（实时流+锁）+ Qdrant（保留，暂不实现） |
| 实时推送 | State/Stream 物理双通道分流 |
| 分布式锁 | 会话级目录排它锁 + 工具级频次限制锁 |
| 两层通信 | gRPC |
| 事件回传 | 实时回传（Python → Go gRPC stream） |
| 多 Agent | Python 层 Swarm（Lead Agent 事件驱动） |
| 依赖注入 | 所有组件通过接口注入，不依赖全局变量 |
| Web UI | CLI 之后实现 |
| CLI | 稍后讨论 |
