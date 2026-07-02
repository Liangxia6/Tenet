# Tenet: A Durable Multi-Agent Framework

> *"The Event Log is the single source of truth."*
>
> 轻量级 · 事件溯源 · 确定性回放 · 多智能体协作
>
> Go（Durable Orchestration）+ Python（Dynamic Agentic Emergence）双层物理架构

---

## 1. Core Philosophy & Positioning

### 1.1 The Name

**Tenet** — 信条，原则。

在分布式系统中，状态是最脆弱的资产。进程会崩溃，网络会分区，大模型的输出天生具有非确定性。面对这些不确定性，Tenet 的信条只有一条：

> **Event Log is the single source of truth.**

只要 append-only 事件日志完整，一切状态——Agent 的每一次思考、工具的每一次调用、任务的每一次推进——都可以精确重建。不是近似恢复，是**逐字节确定性回放**。

这个名字同时暗示了系统的**双向对称时间流**：
- **正向**：事件从 Go 层流向 Python 层，执行推进，日志增长。
- **逆向**：事件从日志回流到 Workflow 函数，逻辑剪枝跳过已执行步骤，状态精确回到任意历史断点。

### 1.2 The Gap Tenet Fills

现有 Agent 框架在两个极端之间摇摆：

| | Shannon | Crush | Tenet |
|---|---|---|---|
| 工作流引擎 | Temporal（重外部依赖） | 无（Agent Loop 即工作流） | 自建 Event Sourcing |
| 可回放 | Temporal 原生 | 不支持 | 事件溯源原生 |
| 可分支 | 不支持 | 不支持 | Fork 物理分支原生 |
| 部署依赖 | Go+Rust+Python + Temporal + PostgreSQL | Go 单二进制 | Go+Python + SQLite 单文件 |
| 历史可审计 | Temporal UI | 日志散落 | SQLite 不可变账本 |

**Shannon** 用 Temporal 获得了持久化与回放，代价是引入了重量级外部引擎。

**Crush** 极其轻量，但 Agent 执行完状态即消散——崩溃后无法恢复，无法回看「Agent 当时在想什么」。

**Tenet** 在两者之间找到平衡点：**用 Go 层自建轻量确定性状态机，实现 Temporal 的核心能力（持久化、回放、分支），但不引入外部引擎依赖。** Python 层专注 LLM 调用与工具执行，Go 层专注编排与工程化保障。

### 1.3 The Physical Contract

Go 层与 Python 层之间只有一条通信路径：**gRPC**。

Go 层不调 LLM API。Python 层不管「这个任务该不该做」。两层之间的 proto 定义是唯一的接口契约。任何一层的内部重构，只要 proto 不变，另一层完全不受影响。

---

## 2. High-Level Architecture & Boundaries

### 2.1 System Topology

```
┌──────────────────────────────────────────────────┐
│                  Go 层：编排引擎                     │
│                 Durable Orchestration              │
│                                                  │
│   CLI (Cobra) ──→ Config ──→ Storage Init         │
│                      │                           │
│     ┌────────────────┼────────────────┐          │
│     ▼                ▼                ▼          │
│   gRPC Server    Workflow Engine   Redis Pool    │
│   (gateway)      (workflow)       (pubsub/lock)  │
│     │                │                │          │
│     │  事件入站       │  调 Python     │          │
│     ▼                │                │          │
│   Event Router ──────┤                │          │
│   (channel)    State │ Stream         │          │
│     │          通道   │ 通道           │          │
│     ▼                ▼                ▼          │
│   Event Store    Projection      SSE Publisher   │
│   (SQLite)       Engine          (Redis)         │
│     │                │                           │
│     └────────┬───────┘                           │
│              ▼                                   │
│     WorkflowContext                              │
│     Decide · Record · Fork · Replay              │
│              │                                   │
│     ┌────────┼────────┐                          │
│     ▼        ▼        ▼                          │
│   Simple   DAG                                  │
│   Workflow  Workflow  Workflow                    │
│                                                  │
│   Lock Manager · Token Budget · Health Check     │
└──────────────────────────┬───────────────────────┘
                           │ gRPC
┌──────────────────────────▼───────────────────────┐
│                Python 层：执行引擎                  │
│              Dynamic Agentic Emergence            │
│                                                  │
│   gRPC Client ──→ Agent Loop (ReAct)             │
│                       │                          │
│          ┌────────────┼────────────┐             │
│          ▼            ▼            ▼             │
│     LLM Provider                Tool Registry    │
│     (OpenAI etc)  (Lead Agent)  (file/shell…)    │
│                                                  │
│   Qdrant Client（向量记忆，保留接口）               │
└──────────────────────────────────────────────────┘
```

### 2.2 Layer Responsibilities

**Go 层 — Durable Orchestration（铁笼管控）**

Go 层的职责是提供确定性保障。它对 Python 层施加纪律约束：每一步外部调用必须记录为不可变事件才能继续。Go 层不执行任何 AI 推理——它只负责「什么时候该做什么」以及「每一步都被记录」。

| 子系统 | 职责 |
|---|---|
| Workflow Engine | 确定性执行、历史回放、Fork 分支、版本控制 |
| Event Store | append-only 事件持久化（SQLite），谱系查询 |
| Event Router | 事件入站后物理分叉：State 通道同步落盘，Stream 通道异步推送 |
| Projection Engine | 从事件流折叠（fold）当前状态，快照管理 |
| Strategy Router | 任务复杂度分析，Workflow 类型选择 |
| gRPC Gateway | Python 层唯一入口，超时/重试/断路器 |
| Lock Manager | Session-Workspace 排它锁（含 Fencing Token）+ 工具频次限制 |
| Token Budget | LLM 调用成本记账与聚合 |

**Python 层 — Dynamic Agentic Emergence（现场智能演进）**

Python 层的职责是在 Go 层划定的安全边界内自由发挥。它不关心任务从哪来、该不该做——只负责接到指令后，驱动 LLM 推理、执行工具、回传结果。

| 子系统 | 职责 |
|---|---|
| Agent Loop | Go 侧 for 循环（Thought → GenerateThought → Tool → ExecuteTool），收敛控制 |
| LLM Adapter | 统一 OpenAI / Anthropic / DeepSeek 等 Provider 接口 |
| Tool Executor | 本地工具执行（文件/Shell/搜索），目录安全防越权 |

### 2.3 Communication Contract

Go 层通过三个 Unary gRPC RPC 调用 Python 层。Python 层完全无状态——每次调用是一次原子操作。

```
Go → Python  GenerateThought(session_id, model, system_prompt, messages, tools)
            → Python 调一次 LLM → 返回 Thought + ToolCalls + TokenUsage

Go → Python  ExecuteTool(session_id, fencing_token, tool_name, arguments)
            → Python 校验 Token → 执行工具 → 返回 stdout/stderr/exit_code

Go → Python  HealthCheck()
            → Python 返回服务状态 + Worker 数 + 运行时长（驱动断路器状态机）
```

proto 定义是两层的唯一接口契约，位于 `proto/tenet/v1/tenet.proto`。详见 `02-tenet-proto-spec.md`。

## 3. Core Architectural Pillars

### 3.1 Durable Execution & Deterministic Replay

Tenet 的 Workflow 函数具有**双重人格**——同一段代码，两种运行模式：

- **执行模式（Execution）**：Workflow 作为生产者，调用外部服务，产生事件。每步外部调用的结果**立即写入 SQLite 并提交事务**，然后才能执行下一行。不存在「批量 Flush 后统一落盘」——那会在崩溃时丢失已执行步骤的全部记录。
- **回放模式（Replay）**：Workflow 作为消费者，从 Event Store 加载历史事件流，逐事件「喂」给函数。遇到已执行的外部调用时，直接从事件中读取缓存结果，0ms 返回，不重复调用。

两种模式的切换是自动的——Workflow 函数不需要知道自己处于哪种模式。引擎通过 `historyPos` 指针与事件流的长度比对来判断。

**确定性检查**：回放时不做值比对（LLM 的输出本质上不可能相同），只做**类型比对**——在事件流的位置 N，代码期望的决策类型是否与历史事件类型一致。不一致说明 Workflow 代码的控制流变了但事件序列没变，引擎 panic，强制开发者用版本门控（GetVersion）标记变更。

**崩溃恢复保证**：事件在 Decide 执行完成后、函数返回前立即落盘。任何时刻崩溃，重启后事件流中已有的步骤被回放跳过，未完成的步骤从断点继续。不丢事件，不重复消费 token。

### 3.2 Symmetric Time Flow & Branching

事件溯源的价值不只是「记录历史」——那是数据库 audit log 的层次。Tenet 的核心价值是**从任意历史时间点对称分叉**。

**物理流 ID 树形谱系**：

```
task:<uuid>                         // 主分支（正向时间线）
task:<uuid>/fork:<N>                // 从第 N 个事件分叉的平行时间线
task:<uuid>/fork:<N>/replay:<R>     // 回放产生的子分支
```

流 ID 不是随机 UUID——它**物理编码了执行谱系**。从 ID 即可读出这次执行从哪里来、在哪个点分叉。

**Fork 操作**：读取原流的前 N 个事件 → **物理复制**到新流 → 从第 N+1 步起，用新的 Workflow 参数执行。新流是完全独立的一等公民——主分支删除不影响 fork 分支。

两个核心场景：
- **What-If 探索**：Agent 在决策点走了路径 A。Fork 到该点，修改 prompt 或参数，观察路径 B 的结果。两条时间线独立演进。
- **错误恢复与修正**：Agent 在第 K 步读错了文件，后续推理全错。Fork 到第 K 步，修正输入，从该点重新执行。原分支保留用于事后分析。

### 3.3 State/Stream Dual-Channel Separation

Python 层通过 gRPC stream 实时回传 Agent 事件到 Go 层后，Event Router 进行**物理双通道分叉**：

- **State 通道（同步阻塞）**：事件写入 SQLite Event Store → 落盘确认 → ACK 返回 Python 层。Python 层不收到 ACK 不会执行下一步——保证事件永不丢失。这是系统的**权威路径**。
- **Stream 通道（异步非阻塞）**：事件推送到 Redis Pub/Sub → Web UI 通过 SSE 实时消费。fire-and-forget 语义——Redis 不可用时降级，不影响核心流程。

两条通道在**物理上独立**（各自的 goroutine、各自的连接池），保证 State 通道的落盘延迟不会阻塞 Stream 通道的实时推送，反之 Stream 通道的 Redis 故障不会拖垮 State 通道。

### 3.4 Event Type-Based Replay Filtering

所有事件——编排事件和 Agent 执行事件——写入**同一条物理流**（`task:<uuid>`）。不需要物理分离为 Task/Agent 两条流，原因有三：

1. **SQLite 顺序读性能**：即使一个 DAG Workflow 产生 500+ 个 Agent 执行事件（每次 GenerateThought + ToolExecuted），WAL 模式下顺序读取 500 条记录 < 30ms。物理分离避免的 I/O 开销不构成瓶颈——真正的瓶颈在 LLM API 调用（每次 2-30s），不在事件读取。
2. **回放时类型匹配即跳过**：所有 Agent 执行事件（GenerateThought、ToolExecuted）在回放时同样经过 Decide 的类型比对——比对通过后 0ms 返回缓存结果，fn() 闭包不被调用。回放耗时 = O(事件数) 的纯内存操作，无外部 I/O。
3. **单一流保证顺序**：如果物理分离为两条流，编排事件和 Agent 事件之间的因果顺序必须通过跨流协调来保证——引入分布式事务复杂度，得不偿失。单一流天然保证 `Record → Decide → Record` 的严格时序。

逻辑上，事件通过 `event_type` 分为两类：

- **编排事件**（TaskStarted、ComplexityAnalyzed、TaskDecomposed、SubTaskDispatched、SubTaskCompleted、TaskCompleted、TaskFailed 等）：驱动 Workflow 回放的控制流骨架。回放时这些事件通过 Decide 和 Record 的类型比对校验控制流一致性。
- **Agent 执行事件**（GenerateThought、ToolExecuted、TokenUsed 等）：Agent 推理和工具调用的完整记录。回放时类型比对通过后直接取缓存结果。**TimelineProjection 从同一流中过滤这些事件构建 Web UI 时间线**——不需要独立的 Agent Stream。

这种设计在「回放精度」和「工程简洁性」之间取得平衡：每次 Agent 思考都被记录为不可变事件（精确回放），但回放不因事件数量而退化（纯内存跳过）。

### 3.5 Hybrid Snapshot Strategy（物理双策略快照架构）

Tenet 的快照系统根据工作空间内容的物理性质**自适应分流**为两种驱动：

- **Git 增量驱动（Git-Backed Driver）**：纯文本工作空间（代码、Markdown、配置）自动切换为 Git 管理。快照 = 一次 `git commit`（仅存储 delta，几百 KB）。Fork 还原 = `git checkout <commit_hash>`（毫秒级 HEAD 指针跳转，**零额外磁盘开销**）。
- **物理打包驱动（Archive-Backed Driver）**：非文本工作空间（二进制、Parquet 数据、编译产物）自动降级为 `tar.gz` 物理归档。配合 `exclude_patterns` 过滤 `.venv`、`node_modules` 等大体积临时目录。

两种驱动通过 `snapshots` 表中的多态字段统一记录：`snapshot_type` (`git`|`archive`) + `snapshot_ref`（Git commit hash | tar.gz 相对路径）。恢复和 Fork 时，WorkspaceManager 根据 `snapshot_type` 选择对应的物理还原路径。

**为什么需要双策略？** 纯文本代码库如果用 tar.gz 全量打包——每次快照几百 MB、Fork 耗时数秒、磁盘迅速爆炸。Git 的增量 delta 压缩使文本快照几乎免费。但二进制文件（模型权重、数据集）无法被 Git 有效 diff——必须物理归档。双策略在两种场景下各取最优。

---

## 4. Safety Mechanisms

在设计阶段已识别并解决五个分布式系统陷阱：

| 陷阱 | 根因 | 解决方案 |
|---|---|---|
| 崩溃丢事件 | 延迟 Flush 导致已执行步骤的记录未落盘 | Decide 立即落盘 + 事务提交 |
| 脏工作区 | Agent 执行失败，workspace 残留半成品文件 | Decide 前自动备份，失败后物理还原 |
| 回放乱序 | Workflow 内并发 goroutine 导致事件顺序不确定 | 禁止原生 `go`，提供 `ctx.Async()` 确定性并发原语 |
| 脑裂双写 | Redis 锁过期后原持有者继续写入 | Fencing Token：写前校验，不匹配即自终止 |
| SQLite 写锁竞争 | 高并发下多 goroutine 竞争 SQLITE_BUSY | 单写守护协程（WriteDaemon）：所有写操作排队串行 |

---

## 5. End-to-End Lifecycle

以下以「分析 Go 项目的性能瓶颈」为例，展示一次 DAG Workflow 执行中数据与控制流的完整生命周期——不展示逐行代码，只展示状态转换和组件交互。

### Phase 1: Ingestion & Routing

```
用户请求 → CLI 解析 → Workflow Engine 创建 Task Stream
         → Strategy Router 调 Python 做复杂度分析
         → 复杂度 0.65 → 选定 DAGWorkflow
         → 分析结果持久化为 ComplexityAnalyzed 事件
```

### Phase 2: Decomposition

```
DAGWorkflow 分解：
  sub-1: CPU 分析    ─┐
  sub-2: 内存分析     ├── 并行执行
  sub-3: goroutine 检查 │
  sub-4: IO/锁竞争    ─┘
  sub-5: 汇总建议        ← 依赖 sub-1~4 全部完成
```

### Phase 3: Sub-Task Execution（单个子任务的内部循环）

```
子任务调度触发:
  AcquireLock(fencing_token) → Backup(workspace) → gRPC GenerateThought → ExecuteTool
  
Python Agent Loop 执行:
  Thought → ToolCall → [FencingToken校验] → ToolResult
  → 每个事件流式回传 Go 层
  → Event Router: State通道落盘 + Stream通道推送
  → ACK → Python 继续下一步
  → 收敛判断 → ReactWorkflow 返回最终结果 → DAGWorkflow 记录 SubTaskCompleted

Decide 返回结果:
  立即 INSERT event_log + COMMIT → CleanBackup → ReleaseLock
```

### Phase 4: Convergence & Completion

```
全部子任务完成 → sub-5 汇总 → TaskCompleted 事件落盘
→ Token Budget 聚合 → 结果返回 CLI
→ 快照检查（满足条件时触发 Projection Snapshot）
```

### Phase 5: Crash Recovery（横切关注点）

```
假设 sub-3 执行中 Go 进程 OOM 崩溃:

重启恢复:
  event_log 中已有: TaskCreated, ComplexityAnalyzed, SubTaskDispatched(sub-1), SubTaskCompleted(sub-1), SubTaskDispatched(sub-2), SubTaskCompleted(sub-2)
  sub-3 未落盘 → event_log 中无记录

Workflow Engine 回放:
  跳过 seq 1-4（从事件中取缓存结果）→ 进入首次执行模式
  → sub-3 调度: Workspace Restore（如备份存在）→ 重新 Decide
  → sub-4 正常执行 → sub-5 收敛 → 完成

结果: 不丢事件，不重复消费 token，workspace 不脏
```

---

## 6. Deployment Topology

```
┌─────────────────────────────────────────┐
│              宿主机 / VM                  │
│                                         │
│  ┌──────────┐  ┌──────────┐            │
│  │ Go 二进制 │  │  Python   │            │
│  │ :50051   │◄─┤ 进程      │            │
│  │          │gRPC│          │            │
│  │ SQLite   │  │ LLM API  │            │
│  │ Redis    │  │ 调用      │            │
│  └──────────┘  └──────────┘            │
│       │                                 │
│       ▼                                 │
│  tenet.db    (SQLite 单文件)             │
│  workspace/  (Agent 工作目录)             │
└─────────────────────────────────────────┘
```

Go 层编译为单二进制，Python 层为独立进程。SQLite 单文件存储全部状态。Redis 为可选的加速层（不可用时降级运行）。

---

## 7. Documentation Matrix

21 个规格文件，按物理分层。✅ = 已完成，所有文档均已完成最终修订。

### 全局与协议层（`01_Protocols_&_Overview/`）

| # | 文档 | 功能 | 状态 |
|---|---|---|---|
| 01 | `01-tenet-overview.md` | 全局架构、设计哲学、物理边界、端到端生命周期 | ✅ |
| 02 | `02-tenet-proto-spec.md` | gRPC 契约：GenerateThought + ExecuteTool + HealthCheck 三个 Unary RPC、消息体字段 + Tag 编号、Go/Python 代码生成命令 | ✅ |
| 03 | `03-tenet-config-spec.md` | 三类配置（Static-Immutable/HotReload/Dynamic）、14 个模块的参数表（database/redis/grpc/workflow/workspace/skills/mcp_servers/agent/safety/interactive/rate_limits/llm_providers/coding/logging）、Configuration Freeze、env: 解析规则、双端校验 | ✅ |

### Go 层：状态机与事件存储（`02_Go_Core_Engine/`）

| # | 文档 | 功能 | 状态 |
|---|---|---|---|
| 04 | `04-go-event-store.md` | SQLite DDL（4 张表）、WAL 模式、MaxOpenConns(1) 铁律、WriteDaemon 单写协程、事务连续性断言、幂等写入 | ✅ |
| 05 | `05-go-workflow-context.md` | WorkflowContext 结构体（10 字段）、Decide/Record 双重人格、Immediate Commit 崩溃安全契约、ctx.Async 乱序缓冲区（sync.Cond）、GetVersion 版本门控 | ✅ |
| 06 | `06-go-engine-scheduler.md` | 调度主循环（4 channel select）、Worker Pool（无锁 TryAcquire）、ctx.Sleep 哨兵错误退出（Exit-on-Suspend）+ 定时器最小堆、Graceful Shutdown 四阶段 | ✅ |
| 07 | `07-go-versioning-forking.md` | GetVersion + VersionMarker 新旧代码共存、Fork 物理复制（SQL 事务 COPY + Workspace 快照还原）、树形 StreamID 谱系查询（GetLineage/GetChildStreams）、故障恢复完整轨迹 | ✅ |
|| 08 | `08-go-workflows-strategies.md` | 五大顶层 Workflow + 一个内部执行单元：Simple（1 次 LLM）/ React（Go 侧 for 循环每步落盘，DAG 内部执行单元，非顶层路由目标）/ DAG（拓扑+Async+Data Relay）/ Interactive（人机协同 git diff 注入）/ Scientific（CoT→Debate→ToT→Reflection 链）/ Coding（7 Phase + autoFix 回滚） | ✅ |
| 09 | `09-go-reasoning-patterns.md` | 四种 Pattern：CoT（分步推理）/ Debate（Pro/Con/Judge 多方对抗，同一 RPC 不同 SystemPrompt）/ ToT（Go 侧 BFS 搜索树，生成+评估+剪枝）/ Reflection（产出→自评→改进循环）。Pattern 可组合 | ✅ |
| 10 | `10-go-projection-engine.md` | Projection 接口 + 三种投影（Task/Timeline/Token）、Projection Engine fold 逻辑、快照双触发器（事件数+时间）、快照恢复流程（按 snapshot_type 分叉）、Hybrid 驱动自动判定 | ✅ |
| 11 | `11-go-workspace-manager.md` | 工作区目录结构、Git 驱动（commit/checkout/diff/resetHard）+ Archive 驱动（tar.gz 打包解压）、Backup/Restore/Cleanup 生命周期、Fencing Token 校验、路径防越权、文本占比检测 | ✅ |

### Go 层：网关分流与控制层（`03_Go_API_&_Control/`）

| # | 文档 | 功能 | 状态 |
|---|---|---|---|
| 12 | `12-go-event-router.md` | State/Stream 物理双通道：State 同步阻塞（SQLite 落盘后才 ACK Python）+ Stream 异步非阻塞（Redis Pub/Sub fire-and-forget）、ACK 完整链路、降级矩阵、EventChannel 可插拔接口 | ✅ |
| 13 | `13-go-grpc-gateway.md` | gRPC Gateway 实现：TenetOrchestrator Server 端、超时 Middleware、重试 Middleware（指数退避）、断路器状态机（CLOSED→OPEN→HALF_OPEN）、Health Check | ✅ |
| 14 | `14-go-cobra-cli.md` | Cobra 命令树：tenet serve / task run / task replay / task fork / task list / config validate。每命令 Flag + 输出规范 | ✅ |
| 15 | `15-go-lock-budget.md` | Session Lock（SETNX+TTL+心跳续约）+ Fencing Token（INCR+写前校验+脑裂防御）+ Tool Rate Limit（Lua 原子令牌桶）+ Token Budget（Guard Pattern 防重复记账）。运维状态不经过事件溯源 | ✅ |
| 16 | `16-go-strategy-router.md` | 复杂度分析路由（调 GenerateThought 评估 → 持久化 ComplexityAnalyzed 事件）→ 选 Workflow 类型。DAGWorkflow 内部可委托 React/Interactive/Scientific/Coding。Workflow Registry 注册表 | ✅ |

### Python 层：AI 认知与物理执行（`04_Python_Agent_Worker/`）

| # | 文档 | 功能 | 状态 |
|---|---|---|---|
| 17 | `17-py-grpc-client.md` | TenetWorker gRPC Server 启动 + RegisterAgent 向 Go 注册。Stateless 设计：不持有循环、不维护历史 | ✅ |
| 18 | `18-py-llm-adapters.md` | BaseAdapter 抽象 + Jinja2 Prompt 编译 + MCP 客户端（Stdio 子进程 + tools/list 动态发现 + JSON-RPC 代理调用）+ OpenAI/Anthropic/DeepSeek 适配器 + Pydantic v2 参数校验门控 | ✅ |
| 19 | `19-py-native-tool-executor.md` | ExecuteTool 路由（MCP/Skill/标准工具）+ 4 个内置工具（ReadFile/Shell 含危险命令拦截/WriteFile/WebSearch）+ 路径防越权双重校验 + Fencing Token 集成 | ✅ |

### 部署、运维与质量保障（`05_Ops_&_Testing/`）

| # | 文档 | 功能 | 状态 |
|---|---|---|---|
| 20 | `20-ops-database-schemas.md` | SQLite DDL（5 表完整建表语句）+ Redis Key 命名空间规范（session_lock / session_fencing / tool_rl / sse）+ 迁移策略与备份方案 | ✅ |
| 21 | `21-ops-testing-strategy.md` | 测试分层（单元 70% / 集成 20% / 回放回归 10%）+ Go 核心引擎高精细度单元测试 + Python 安全对抗测试 + 确定性 Three-Zero 断言 + 混沌测试 + CI 集成 | ✅ |
