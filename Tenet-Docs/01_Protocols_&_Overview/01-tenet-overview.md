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
│   Simple   DAG     Swarm                         │
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
│     LLM Provider  Swarm Engine  Tool Registry    │
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
| Agent Loop | ReAct 循环（Thought → Action → Observation），收敛控制 |
| LLM Adapter | 统一 OpenAI / Anthropic / DeepSeek 等 Provider 接口 |
| Swarm Engine | Lead Agent 事件驱动：任务分配、质量检查、进度跟踪 |
| Tool Executor | 本地工具执行（文件/Shell/搜索），目录安全防越权 |

### 2.3 Communication Contract

```
Go → Python  ExecuteAgent(task_id, agent_config, messages, tools)
Python → Go  stream AgentEvent { ThoughtGenerated, ToolCalled,
                                  ToolResulted, AgentCompleted, AgentFailed }
```

proto 定义是两层的唯一接口契约，位于 `proto/tenet/v1/tenet.proto`。

---

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

### 3.4 Two-Level Stream Separation

Task 流与 Agent 流是两条**物理分离的流**——这是避免事件溯源「存储膨胀」和「回放退化」的关键设计。

- **Task Stream**（`task:<uuid>`）：Go 层编排逻辑产生。一个 Task 通常只有几个到十几个事件（TaskCreated → ComplexityAnalyzed → AgentExecuted × N → TaskCompleted）。Workflow 引擎只回放 Task Stream。
- **Agent Stream**（`agent:<name>`）：Python 层执行逻辑产生。一个 Agent 执行可能产生几十到上百个事件（每次 Thought、每次 ToolCall、每次 ToolResult）。Agent Stream 不参与 Workflow 回放——它存在的唯一目的是 Web UI 可视化和 Agent 级调试。

如果不分离，一个包含 5 个子任务的 DAG Workflow 回放时需要加载 500+ 个 Agent 内部事件。分离后只需加载 10 个 Task 级事件。

---

## 4. Dynamic Swarm Engine

Python 层的多智能体协作采用 **Lead Agent 事件驱动架构**（领航者模式），灵感来自 Shannon 的 Swarm 设计。

### 4.1 Lead-Worker Topology

```
Lead Agent（事件驱动循环）
  │
  ├── initial_plan 事件    → 任务分解，生成 Worker Agent，分配工具集
  ├── agent_idle 事件      → 将待分配任务推送给空闲 Agent
  ├── agent_completed 事件 → 质量检查：Key Findings 验证 → 接受 / 退回重试
  ├── checkpoint 事件       → 每 N 分钟审查整体进度
  └── closing_checkpoint    → 最终综合，产出汇总报告
```

### 4.2 File-as-Memory Pattern

Agent 之间的通信不走消息队列，而是通过**共享 workspace 文件系统**。每个 Agent 将发现写入 `findings/agent-<topic>.md`。完成时必须输出 3-5 条带数据的 Key Findings——Lead Agent 靠摘要做质量判断，不需要读全文。

### 4.3 Convergence Control

连续 N 次无工具调用 → 强制收敛。死循环检测（重复调用同一工具）→ 中断并上报。Checkpoint 超时 → 强制进入 closing 阶段。

### 4.4 External Governance

Go 层作为「投资人」在 Python 层外部施加约束：
- **Token Budget**：每次 LLM 调用记账，Guard Pattern 防重复。聚合到 Task 级，超预算时熔断。
- **HITL（Human-in-the-Loop）断点**：关键决策点（如生产环境部署）可配置为需要人工审批后才能继续。
- **Fencing Token**：Python 层每次文件写入前校验锁的 Fencing Token，防止脑裂导致的双写。

---

## 5. Safety Mechanisms

在设计阶段已识别并解决五个分布式系统陷阱：

| 陷阱 | 根因 | 解决方案 |
|---|---|---|
| 崩溃丢事件 | 延迟 Flush 导致已执行步骤的记录未落盘 | Decide 立即落盘 + 事务提交 |
| 脏工作区 | Agent 执行失败，workspace 残留半成品文件 | Decide 前自动备份，失败后物理还原 |
| 回放乱序 | Workflow 内并发 goroutine 导致事件顺序不确定 | 禁止原生 `go`，提供 `ctx.Async()` 确定性并发原语 |
| 脑裂双写 | Redis 锁过期后原持有者继续写入 | Fencing Token：写前校验，不匹配即自终止 |
| SQLite 写锁竞争 | 高并发下多 goroutine 竞争 SQLITE_BUSY | 单写守护协程（WriteDaemon）：所有写操作排队串行 |

---

## 6. End-to-End Lifecycle

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
Decide("AgentExecuted") 触发:
  AcquireLock(fencing_token) → Backup(workspace) → gRPC ExecuteAgent
  
Python Agent Loop 执行:
  Thought → ToolCall → [FencingToken校验] → ToolResult
  → 每个事件流式回传 Go 层
  → Event Router: State通道落盘 + Stream通道推送
  → ACK → Python 继续下一步
  → 收敛判断 → AgentCompleted

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
  event_log 中已有: TaskCreated, ComplexityAnalyzed, AgentExecuted(sub-1), AgentExecuted(sub-2)
  sub-3 未落盘 → event_log 中无记录

Workflow Engine 回放:
  跳过 seq 1-4（从事件中取缓存结果）→ 进入首次执行模式
  → sub-3 调度: Workspace Restore（如备份存在）→ 重新 Decide
  → sub-4 正常执行 → sub-5 收敛 → 完成

结果: 不丢事件，不重复消费 token，workspace 不脏
```

---

## 7. Deployment Topology

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

## 8. Documentation Matrix

完整的 Tenet 设计文档分布在 17 个规格文件中，按物理分层组织。本文件（`01-tenet-overview.md`）是全局概念锚点——读者应从此文件出发，按需深入子文档。

### 全局与协议层

| 文档 | 内容 |
|---|---|
| `01-tenet-overview.md` | **本文件** — 全局架构、设计哲学、物理边界 |
| `02-tenet-proto-spec.md` | gRPC / Protobuf 接口契约详细定义 |
| `03-tenet-config-spec.md` | 静态 YAML 配置 + 动态事件驱动配置规范 |

### Go 层：确定性状态机与存储

| 文档 | 内容 |
|---|---|
| `04-go-event-store.md` | SQLite 账本表结构、写入幂等、事务控制、WriteDaemon 单写协程 |
| `05-go-workflow-context.md` | WorkflowContext 核心抽象：Decide 立即落盘、Record 延迟缓冲、回放指针 |
| `06-go-versioning-forking.md` | GetVersion 版本门控、Fork 物理分支路由、谱系查询 |
| `07-go-projection-snapshot.md` | 状态投影引擎、快照双触发器、工作区备份与恢复 |
| `08-go-event-router.md` | State/Stream 双通道物理分流、ACK 链路、降级策略 |

### Go 层：网关、风控与脚手架

| 文档 | 内容 |
|---|---|
| `09-grpc-gateway-middleware.md` | gRPC Gateway 实现、超时/重试/断路器中间件链 |
| `10-go-cobra-cli.md` | Cobra 命令行树、命令规范、Flag 设计 |
| `11-go-lock-budget.md` | Session Lock + Fencing Token、Tool Rate Limit、Token Budget 记账 |

### Python 层：AI 认知与物理执行

| 文档 | 内容 |
|---|---|
| `12-py-grpc-client.md` | gRPC 客户端：事件流回传、ACK 等待、连接恢复 |
| `13-py-llm-adapters.md` | LLM Provider 适配器：OpenAI / Anthropic / DeepSeek |
| `14-py-swarm-engine.md` | Swarm 引擎：Lead Agent 循环、任务分配、Handoff、收敛控制 |
| `15-py-native-tool-executor.md` | 本地工具执行器：文件/Shell/搜索、目录防越权、Fencing Token 集成 |

### 部署、运维与质量保障

| 文档 | 内容 |
|---|---|
| `16-ops-database-schemas.md` | SQLite 物理 DDL、Redis Key 命名与结构规范 |
| `17-ops-testing-strategy.md` | 测试分层策略、确定性回放回归测试、混沌测试 |
