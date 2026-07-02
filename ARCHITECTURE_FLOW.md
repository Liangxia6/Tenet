# Tenet: Architecture & Full Call Flow

> 轻量级 · 事件溯源 · 确定性回放 · 多智能体
>
> Go（Durable Orchestration）+ Python（Stateless Worker）双层物理架构
>
> 当前版本：v1.1.0（Design B：Go Deterministic Loop）

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────┐
│                  Go 层 — Durable Orchestration     │
│                                                  │
│  CLI → Config → Storage Init → Scheduler          │
│                     │                            │
│    ┌────────────────┼────────────────┐           │
│    ▼                ▼                ▼           │
│  gRPC Client    Workflow Engine   Redis Pool     │
│  (call Python)  (Strategies +      (PubSub/Lock)  │
│                  Patterns)                       │
│    │                │                            │
│    │                ├─ SimpleWorkflow             │
│    │                ├─ ReactWorkflow  ← for循环    │
│    │                ├─ DAGWorkflow    ← 拓扑+Async │

│    │                ├─ InteractiveWorkflow         │
│    │                ├─ ScientificWorkflow          │
│    │                └─ CodingWorkflow  ← 7Phase    │
│    │                │                            │
│    │                ├─ ChainOfThought             │
│    │                ├─ Debate                     │
│    │                ├─ TreeOfThoughts              │
│    │                └─ Reflection                 │
│    │                │                            │
│    │           WorkflowContext                    │
│    │           Decide(立即落盘) · Record(延迟缓冲)  │
│    │           Fork · Replay · GetVersion        │
│    │                │                            │
│    │    ┌───────────┼───────────┐                │
│    │    ▼           ▼           ▼                │
│    │  Event Store  Projection  WorkspaceMgr      │
│    │  (SQLite)     Engine      (Git/Archive快照)   │
│    │    │                       │                │
│    │  WriteDaemon             Git Commit/Tar     │
│    │  (单写协程)               (双策略快照)        │
│    │                                            │
│    │  Lock Manager · Token Budget               │
│    │  (Fencing Token)(Guard Pattern)             │
│    │                                            │
│    │  Event Router                              │
│    │  State通道(SQLite) + Stream通道(Redis SSE)   │
│    └────────────────────┬───────────────────────┘
│                         │ gRPC (Unary)
│      GenerateThought ───┤    ExecuteTool
│      (messages→LLM)     │    (tool→OS)
│                         ▼
│  ┌──────────────────────────────────────────┐
│  │          Python 层 — Stateless Worker      │
│  │                                          │
│  │  GenerateThought:                        │
│  │    messages → LLM API → Thought+ToolCalls│
│  │    → 返回 → 释放内存                      │
│  │                                          │
│  │  ExecuteTool:                            │
│  │    FencingToken校验 → OS执行 → stdout    │
│  │    → 返回 → 释放内存                      │
│  └──────────────────────────────────────────┘
```

**核心原则**：

- **Event Log is the single source of truth** — SQLite append-only 账本是系统唯一权威。任何状态可以从事件流重建。
- **Go 驱动所有循环** — ReAct、ToT、Debate 的 `for` 循环全在 Go 进程中。Python 被调 → 执行一次操作 → 返回 → 忘记一切。
- **Decide 立即落盘** — 每次外部调用（调 LLM、执行工具）完成后，事件在函数返回前已写入 SQLite + 事务提交。崩溃后从最后落盘事件精确恢复。

---

## 2. Storage

| 数据库 | 角色 | 内容 |
|---|---|---|
| SQLite | 权威状态 | sessions / event_log(append-only) / snapshots(多态: git\|archive) / token_telemetry |
| Redis | 实时 + 锁 | SSE Pub/Sub / Session Lock + Fencing Token / Tool Rate Limit |
| Qdrant | 向量记忆(暂不实现) | Embeddings / Episodic Memory / Long-Term Memory |

---

## 3. Full Call Flow: "分析 Go 项目的性能瓶颈"

```
═══════════════════════════════════════════════════════════
Phase 1: Task 创建与策略路由
═══════════════════════════════════════════════════════════

CLI:  tenet task run "分析 ~/code/goserver 的性能瓶颈"

1. Cobra 解析 → Scheduler.Submit(task)

2. Strategy Router:
   → Go 调 Python: GenerateThought(system="评估任务复杂度", messages=[user_query])
   → Python: 调 LLM → 返回 {complexity: 0.65, strategy: "dag"}
   → Go: Decide 立即落盘 ComplexityAnalyzed 事件
   → 选定 DAGWorkflow


═══════════════════════════════════════════════════════════
Phase 2: DAG 任务分解
═══════════════════════════════════════════════════════════

3. DAGWorkflow(Go侧 for 循环):
   → Go 调 Python: GenerateThought(system="分解任务为子任务", messages=[query])
   → Python: 调 LLM → 返回 5 个子任务 + 依赖关系
   → Go: Record("TaskDecomposed", {subtasks, deps})

4. 拓扑排序:
   Layer 0: sub-1(CPU分析), sub-2(内存分析), sub-3(goroutine检查), sub-4(IO/锁分析)
   Layer 1: sub-5(汇总, depends_on: [1,2,3,4])


═══════════════════════════════════════════════════════════
Phase 3: 并发执行 Layer 0 的 4 个子任务（以 sub-1 "CPU分析" 为例）
═══════════════════════════════════════════════════════════

5. ctx.Async("sub-1", fn)  — 4 个子任务并发启动

6. sub-1 内部 — ReactWorkflow(Go侧 for 循环):

   ┌─ Loop 第1轮 ────────────────────────────────────┐
   │                                                │
   │ Step A: Generate Thought                       │
   │   Go → Python: GenerateThought(                │
   │     system="你是 Go 性能分析专家...",           │
   │     messages=[{user:"分析 CPU 性能瓶颈"}],       │
   │     tools=["read_file","shell","write_file"])   │
   │                                                │
   │   Python:                                      │
   │     → 拼装 messages                            │
   │     → 调 LLM API (OpenAI/DeepSeek)              │
   │     → LLM 返回: "需要先看 main.go"              │
   │     → 返回 GenerateThoughtResponse{            │
   │          thought: "需要先看 main.go",            │
   │          tool_calls: [{read_file, path:"main.go"}],│
   │          is_final: false,                      │
   │          usage: {prompt:1200, completion:50, cost:$0.003}}│
   │     → 释放所有内存                              │
   │                                                │
   │   回到 Go:                                     │
   │     → ctx.Decide 内部:                         │
   │         ★ INSERT event_log(ThoughtGenerated) + COMMIT│
   │     → history 追加本事件                         │
   │     → messages 追加 assistant{thought + tool_calls}│
   │     → TokenUsed 记录落盘                         │
   │                                                │
   │ Step B: Execute Tool                           │
   │   对每个 tool_call:                              │
   │                                                │
   │   Go → Python: ExecuteTool(                    │
   │     session_id,                                │
   │     fencing_token=5,                           │
   │     tool_name="read_file",                     │
   │     arguments='{"path":"main.go"}')             │
   │                                                │
   │   Python:                                      │
   │     → GET Redis: session_fencing → token==5 ✓   │
   │     → open("main.go").read()                   │
   │     → 返回 ExecuteToolResponse{stdout:"package main\n\nfunc main()...",exit_code:0}│
   │     → 释放所有内存                              │
   │                                                │
   │   回到 Go:                                     │
   │     → ctx.Decide 内部:                         │
   │         ★ INSERT event_log(ToolExecuted) + COMMIT│
   │     → messages 追加 tool{result}                │
   │                                                │
   │   HITL 安全拦截点（如果需要）:                    │
   │     如果是危险命令 → Go 暂停 → 等待人工审批       │
   └────────────────────────────────────────────────┘

   ┌─ Loop 第2轮 ────────────────────────────────────┐
   │ Step A: Generate Thought                       │
   │   Go → Python: GenerateThought(                │
   │     messages=[                                   │
   │       {system:"你是专家..."},                     │
   │       {user:"分析 CPU"},                         │
   │       {assistant:"需要看 main.go", tool_calls:[..]},│
   │       {tool:"package main\nfunc main()..."}])    │
   │                                                │
   │   Python: 调 LLM → "运行 pprof"                 │
   │   → 返回 {thought:"运行 pprof", tool_calls:[{shell:"go tool pprof..."}]}│
   │                                                │
   │   回到 Go:                                     │
   │     → ★ INSERT event_log(ThoughtGenerated) + COMMIT│
   │                                                │
   │ Step B: Execute Tool                           │
   │   Go → Python: ExecuteTool(shell, "go tool pprof...")│
   │   Python: Fencing Token校验 → subprocess.run → stdout│
   │   回到 Go: ★ INSERT event_log(ToolExecuted) + COMMIT│
   └────────────────────────────────────────────────┘

   ... 持续 N 轮，直到 LLM 返回 is_final=true 或
   连续 N 次无 tool_call → 收敛 ...

   ┌─ 最终轮 ────────────────────────────────────────┐
   │ Go → Python: GenerateThought(...)               │
   │ Python: 调 LLM → "分析完成。发现 3 个瓶颈: ..."   │
   │   → 返回 {thought:"分析完成...", is_final:true}   │
   │   回到 Go: ★ INSERT event_log(ThoughtGenerated) + COMMIT│
   │   → ReactWorkflow 返回 "分析完成..."              │
   └────────────────────────────────────────────────┘


═══════════════════════════════════════════════════════════
Phase 4: 依赖收敛
═══════════════════════════════════════════════════════════

7. sub-1, sub-2, sub-3, sub-4 全部完成
   → Go 的 DAGWorkflow 检查依赖: sub-5.depends_on 全部满足
   → 执行 sub-5(汇总)


═══════════════════════════════════════════════════════════
Phase 5: 完成
═══════════════════════════════════════════════════════════

8. Go: Record("TaskCompleted", {result, artifacts[...], token_usage})
   → Commit: flushRecords → INSERT event_log + COMMIT

9. CLI 输出:
     task_id: task:a1b2c3   status: COMPLETED
     workflow: dag (5 subtasks)
     tokens: 45,230 ($0.83)
     artifacts: [findings/summary.md, ...]


═══════════════════════════════════════════════════════════
崩溃恢复示例: sub-3 执行到一半 Go 进程 OOM
═══════════════════════════════════════════════════════════

崩溃瞬间 event_log:
  seq=1: TaskCreated
  seq=2: ComplexityAnalyzed
  seq=3: AgentExecuted{sub-1 result}           ← 已落盘
  seq=4: AgentExecuted{sub-2 result}           ← 已落盘
  seq=5: ThoughtGenerated{sub-3, step=1}       ← 已落盘
  seq=6: ToolExecuted{sub-3, read_file}        ← 已落盘
  seq=7: ThoughtGenerated{sub-3, step=2}       ← 已落盘
  (ToolExecuted for sub-3 step=2 未落盘 — 崩溃)

重启恢复:
  1. Workflow Engine 加载 task:a1b2c3 的事件流
  2. DAGWorkflow 回放:
     → 跳过 seq 1-2
     → 跳过 sub-1 的全部内部事件(seq 3)
     → 跳过 sub-2 的全部内部事件(seq 4)
     → sub-3 的 ReactWorkflow:
        historyPos=0 → 5: 跳过 seq 5(ThoughtGenerated step=1)
                    → 6: 跳过 seq 6(ToolExecuted read_file)
                    → 7: 跳过 seq 7(ThoughtGenerated step=2)
        historyPos=7, 进入首次执行
        → 重新执行 sub-3 step=2 的 ToolExecuted(go tool pprof)
          (上次崩溃时未落盘, 这次重新执行)
        → 继续 sub-3 的后续循环
     → sub-4 首次执行(sub-3 的 workspace 在崩溃前可能已脏,
                     WorkspaceManager 从备份还原)
     → sub-5 收敛

结果: 不丢已落盘事件, 精确恢复到崩溃点, 只重做未完成的一个工具调用
      (不是整个 sub-3 重跑, 更不是整个 Task 重跑)
```

---

## 4. Key Design Decisions

| 决策 | 内容 |
|---|---|
| 确定性引擎 | Go 自建, 不用 Temporal |
| Python 无状态 | 每次 RPC 是一次原子操作: 调 LLM 或执行工具 → 返回 → 释放内存 |
| Go 驱动所有循环 | ReAct/ToT/Debate 的 for 循环在 Go 进程中 |
| 每步立即落盘 | Decide 完成后 INSERT event_log + COMMIT, 然后才返回 |
| 事件溯源 | Event Sourcing(存结果), 不存意图 |
| Fork 分支 | 物理复制前 N 个事件 + Git/Archive 双策略快照 |
| 双通道分流 | State(SQLite,同步) + Stream(Redis SSE,异步) |
| 两层事件分离 | Task Stream(Go回放,~10事件) + Agent Stream(UI渲染,~100事件) |
| 版本控制 | 每个代码变更独立命名(GetVersion), 去中心化管理 |
| 配置冻结 | Session 启动时快照配置, 整个生命周期不变 |
| Fencing Token | 每次文件操作前校验, 防脑裂双写 |
| WriteDaemon | SQLite 单写协程, 防 SQLITE_BUSY |
| 双策略快照 | Git 增量(文本代码,毫秒级) + tar.gz(二进制) |
