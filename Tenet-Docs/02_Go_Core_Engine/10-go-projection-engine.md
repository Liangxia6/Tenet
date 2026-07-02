# Go Projection Engine

> fold 事件流 → 当前状态 · 快照双触发器 · Hybrid Snapshot 驱动
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 1. Projection 接口

```go
type Projection interface {
    Apply(event Event) error          // 消费一个事件，更新自身状态。纯函数
    State() ProjectionState           // 返回当前状态
    Snapshot() ([]byte, error)        // 序列化为 JSON blob
    Restore(data []byte) error        // 从 JSON blob 恢复
}
```

---

## 2. 三种投影

### 2.1 TaskProjection

消费 Task Stream 事件 → Task 当前运行状态。

fold 哪些事件：`TaskCreated`（初始化 Status=RUNNING）→ `TaskDecomposed`（填充 Subtasks map）→ `SubTaskDispatched` / `SubTaskCompleted`（更新完成进度）→ `TaskCompleted/Failed`（设置 Status 终态）。

产出字段：`StreamID`、`Status`（RUNNING/PAUSED/COMPLETED/FAILED）、`Subtasks map[string]*SubTaskState`（每个子任务的 ID/AgentRole/Status/Result）`Progress`（CompletedSteps/TotalSteps）、`CurrentPhase`（仅 CodingWorkflow）。

### 2.2 TimelineProjection

消费事件流中的 **Agent 执行事件** → Web UI 时间线。从统一的 event_log 中过滤 `GenerateThought`、`ToolExecuted`、`TaskCompleted`、`TaskFailed` 等事件类型，构建时间线步骤。

fold 哪些事件：

| event_log 事件 | 映射为 TimelineStep |
|---|---|
| `GenerateThought` | `{type: "thought", content: payload.thought, step: N, timestamp}` |
| `ToolExecuted` | `{type: "tool", tool_name: payload.tool_name, content: payload.stdout, stderr: payload.stderr, duration_ms: payload.duration_ms, timestamp}` |
| `TaskCompleted` | `{type: "completed", content: payload.result, timestamp}` |
| `TaskFailed` | `{type: "failed", content: payload.error, timestamp}` |
| `WaitingForHumanInput` | `{type: "waiting", round: payload.round, timestamp}` |

产出字段：`StreamID`、`Steps []TimelineStep`（按事件顺序排列）、`TotalSteps`、`Duration`（首个 GenerateThought 到 TaskCompleted/Failed 的时间跨度）。

### 2.3 TokenProjection

消费所有流的 `TokenUsed` 事件 → 累计 token 用量。

产出字段：`TotalTokens`、`TotalCostUSD`、`ByAgent map[string]int64`、`ByModel map[string]int64`、`BudgetLimit`（来自 config）、`BudgetExceeded bool`。

---

## 3. Projection Engine

```go
type ProjectionEngine struct {
    store       EventStore
    projections map[string]Projection   // streamID → Projection（lazy init）
    mu          sync.RWMutex
    config      *RuntimeConfig
}
```

ProjectionEngine 提供两种访问模式，**互补而非冲突**：

**（A）实时增量更新 — Apply(event)**：Event Router 每落盘一个事件后调用 `engine.Apply(event)`，在持有 `mu` 写锁的情况下对对应 Projection 做增量 Apply（O(1) 单事件处理）。用于 Web UI 实时刷新和快照条件实时检测。这便是「推送」路径。

**（B）按需全量折叠 — Project(streamID)**：从 Event Store 读取 streamID 的全部事件，从最新快照恢复起始状态，fold 后续事件构建完整 Projection。用于首次访问、缓存未命中、或恢复 restarted Engine。这便是「拉取」路径。

```go
// Apply 处理单条事件，增量更新对应 Projection 的内存状态。
// Event Router 在每个事件落盘后调用。不阻塞 Router——Apply 失败不影响事件已落盘的事实。
func (pe *ProjectionEngine) Apply(event *InternalEvent) error
```

**Project(streamID) 流程**（与 Apply 互补，互不干扰）：
1. 尝试从 `projections` 内存 map 获取已有实例
2. 无 → 根据 streamID 前缀创建对应类型（含 "agent:" → TimelineProjection，其他 → TaskProjection）
3. 查询 `snapshots` 表：`SELECT stream_seq, state_blob FROM snapshots WHERE stream_id = ? ORDER BY stream_seq DESC LIMIT 1`
4. 有快照 → `projection.Restore(stateBlob)`，`fromSeq = stream_seq + 1`
5. 无快照 → `fromSeq = 1`
6. `events = store.Read(streamID, fromSeq, 0)` → 遍历 Apply
7. 更新 `projections[streamID]`，返回 `State()`

---

## 4. 快照管理

### 4.1 双触发器

| 触发器 | 条件 | 默认值 |
|---|---|---|
| Event-Count | `stream_seq % snapshot_event_interval == 0` | 50 |
| Time-Based | 距上次快照 > `snapshot_time_interval_seconds` | 300s |

取先满足者。时间触发器防止低流量流长时间无快照。

### 4.2 快照创建流程

1. 序列化状态：`stateBlob = projection.Snapshot()`
2. 判定快照驱动：
   - WorkspaceManager.AnalyzeTextRatio(workspace) >= 0.9 → `snapshot_type = "git"`, `snapshot_ref = WorkspaceManager.GitCommit(...)`
   - < 0.9 → `snapshot_type = "archive"`, `snapshot_ref = WorkspaceManager.CreateArchive(...)`
3. 通过 WriteDaemon 单事务写入：INSERT snapshots + INSERT event_log(SnapshotCreated, payload={snapshot_type, snapshot_ref, stream_seq})

### 4.3 快照恢复

读最新快照 → Restore(stateBlob) → 根据 snapshot_type 还原 workspace（git checkout / tar 解压）→ 从 stream_seq+1 fold 后续事件。
