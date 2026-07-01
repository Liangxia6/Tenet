# Python Swarm Engine

> Swarm 多智能体：Lead Agent 循环、任务分配、Handoff

---

## 1. Lead Agent Event Loop

（待展开：事件驱动的 Lead Agent 循环——initial_plan → agent_idle → agent_completed → checkpoint → closing_checkpoint。每个事件的处理逻辑）

## 2. Task Decomposition

（待展开：initial_plan 阶段——LLM 分析任务、分解为子任务、为每个子任务选择合适的 Agent 角色、分配工具集）

## 3. Agent Lifecycle

（待展开：Agent 的创建（角色+prompt+工具集）、状态管理（running/idle/completed/failed）、空闲检测与任务再分配）

## 4. Quality Gate

（待展开：agent_completed 事件的质量检查——Key Findings 验证（3-5 条带数据的结论）、结果完整性检查、决定接受还是重试）

## 5. Convergence Control

（待展开：连续 N 次无工具调用 → 强制收敛。死循环检测——重复调用同一工具的检测与打断。Checkpoint 超时强制进入 closing）

## 6. Inter-Agent Communication

（待展开：共享 workspace 文件——Agent 把发现写入 `findings/agent-<topic>.md`。Key Findings 摘要——Lead Agent 靠摘要做决策，不读全文。Handoff 动态转交任务给其他 Agent）
