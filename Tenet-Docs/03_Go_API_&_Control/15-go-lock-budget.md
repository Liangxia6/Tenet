# Go Lock & Budget

> Session Lock + Fencing Token · Tool Rate Limit · Token Budget
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 0. 运维状态 vs 事件溯源边界

Session Lock、Fencing Token、Tool Rate Limit 计数器属于**运维状态（Operational State）**，存储在 Redis 中，**不经过 SQLite Event Log**。这与「Event Log is single source of truth」的关系如下：

- **事件溯源状态**（SQLite event_log）：Agent 的思考、工具调用、任务推进——需要确定性回放的一切。
- **运维状态**（Redis）：锁、令牌、限流计数器——这些是**瞬时协调原语**，存在于执行时刻，不需要回放。Redis 崩溃 → 所有锁丢失 → 正在执行的 Session 自终止并创建新 Session。这是**设计决策，不是漏洞**。

**一致性边界**：Redis 运维状态永不写入 SQLite。Go 层重启后，所有历史锁/令牌/计数器均不复存在——新进程从零开始获取锁、生成新 Fencing Token。不会出现「旧事件流中的 Token 与新进程冲突」的情况，因为 Token 只在当前进程生命周期内有效。

---

## 1. Lock Manager

Go 层的并发保护层。所有 `Decide` 和 `ExecuteTool` 前被调用。

## 2. Session-Workspace Lock + Fencing Token

**获取**：`SETNX session_lock:{session_id} {agent_id} EX 30`。成功 → `INCR session_fencing:{session_id}` → 返回 `FencingLease{Token}`。失败 → 锁被其他 Agent 持有。

**续约**：每 10s 调 `EXPIRE session_lock:{session_id} 30` + `INCR session_fencing:{session_id}` → 更新 lease.Token。

**释放**：Lua 原子 `if GET(key) == agent_id then DEL(key) end`。

**校验**：文件操作前 `GET session_fencing:{session_id}` 与本地 lease.Token 比对。不相等 → 锁已被抢占（GC 卡顿 40s 导致过期）→ 拒绝操作。

## 3. Tool Rate Limit

Lua 原子令牌桶：`INCR tool_rl:{tool}:{window} → EXPIRE → 检查 > limit`。每个工具不同参数（来自 `rate_limits` 配置）。

## 4. 降级

Redis 不可用时的降级策略（所有降级路径保持在 Go 层，Python 保持无状态）：

**Session Lock 降级**：本地 `map[sessionID]*sync.Mutex` 排它锁。仅在单 Go 进程部署时有效——多进程部署降级到本地锁意味着跨进程无锁保护，需接受「同一 Session 可能被多个 Go 进程同时执行」的风险。Fencing Token 在降级模式下不可用（无共享计数器）→ `ExecuteTool` 跳过 Fencing Token 校验，信任本地锁。

**Tool Rate Limit 降级**：Go 进程内维护 `map[toolName]*rate.Limiter`（Go 标准库 `golang.org/x/time/rate` 的令牌桶实现）。每个工具的 `burst` 和 `rate` 参数从配置读取。降级后的限流仅在当前 Go 进程内有效，跨进程不共享。

**Key principle**：降级状态不涉及 Python 层。Python 只在 Go 调用 `ExecuteTool` 时被动执行，不维护任何限流计数器。这保持了 Python 的完全无状态契约。

## 5. Token Budget

**RecordUsage(taskID, agent, model, prompt, completion, cost)**：
- 调用 `ctx.Record("TokenUsed", {task_id, agent, model, prompt_tokens, completion_tokens, cost_usd})`——**走 Record 延迟批写路径，不立即落盘**。
- Guard Pattern：同一 task+agent+step 只记录一次。Record 内部通过 `versionMarkers` 类似的去重 map（key = `{task_id}:{agent}:{stream_seq}`）保证幂等。
- 在下次 `Decide` 前，`flushRecords()` 将 TokenUsed 事件批量写入 event_log + token_telemetry 表（同一 WriteDaemon 事务）。
- 零 token 不记录（ExecuteTool 没有 LLM 调用）。

**GetTaskUsage(taskID)**：通过 `TokenProjection.Project(taskID)` 返回累计用量。注意：由于 Record 延迟批写，未 flush 的 Record 不反映在 Projection 中——这是预期行为（Projection 反映已落盘事件）。

## 6. 集成点

| 调用点 | 操作 |
|---|---|
| Decide("GenerateThought") 前 | AcquireSessionLock |
| Decide 内部 LLM 调用后 | RecordUsage |
| Decide 内部 ExecuteTool 前 | CheckToolRateLimit + ValidateFencingToken |
| Workflow 结束时 | GetTaskUsage → 超预算则熔断 |
