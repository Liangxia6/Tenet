# Go Versioning & Forking

> GetVersion 版本门控 · Fork 物理分支 · 谱系查询 · 故障恢复
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 1. GetVersion — 代码演进而不丢历史

### 1.1 问题

Workflow 代码会演进。v2 增加了新的 Decide 调用（如 AgentRetry），但 v1 创建的旧事件流中没有对应的 VersionMarker。用 v2 代码回放 v1 事件流 → 确定性检查在新增 Decide 的位置 panic——代码期望某个决策类型，但历史事件流中对应的位置是不同类型的事件。

### 1.2 GetVersion 函数签名

```go
func (ctx *WorkflowContext) GetVersion(changeID string, minVersion int) int
```

- `changeID`：变更的唯一名称（如 `"add-retry-logic"`），用于跨执行追踪
- `minVersion`：该变更引入时的最小版本号
- 返回：版本号。首次执行 = `ctx.version`；回放 = 从事件流中读取的版本号或默认版本 1

### 1.3 内部行为

**首次执行模式**：
1. 检查 `ctx.versionMarkers[changeID]`——如果已存在，直接返回 `ctx.version`（同一变更不重复写入 VersionMarker）
2. 断言 `historyPos >= len(history)`（不在回放模式）
3. 构造 VersionMarkerPayload: `{change_id: changeID, version: ctx.version}`
4. `ctx.Record("VersionMarker", marker)` → 通过 flushRecords 落盘
5. `ctx.versionMarkers[changeID] = true`
6. 返回 `ctx.version`

**回放模式**：
1. 检查 `ctx.versionMarkers[changeID]`——如果已经处理过，直接返回（被去重跳过）
2. 从 history **索引 0** 开始向后扫描：查找 `event_type == "VersionMarker"` 且 `payload.change_id == changeID` 的事件（不能从 historyPos 开始扫描——VersionMarker 可能在 historyPos 之前，已被先前的回放步骤消耗）
3. **找到** → 从 history 切片中移除该事件（`history = append(history[:i], history[i+1:]...)`）。**如果被移除的索引 `i < historyPos`，必须 `historyPos--` 补偿索引偏移，否则后续 Decide 的 `history[historyPos]` 会读到错误事件导致确定性检查失败。** → `ctx.versionMarkers[changeID] = true` → 返回 `payload.version`
4. **未找到** → 该事件流是在此代码变更引入之前创建的 → `ctx.versionMarkers[changeID] = true` → 返回默认版本 `1`（走旧逻辑分支）

### 1.4 VersionMarker Payload

| 字段 | 类型 | 说明 |
|---|---|---|
| `change_id` | `string` | 变更唯一标识 |
| `version` | `int` | 引入该变更时的代码版本号 |

### 1.5 去中心化版本管理

版本号不是全局递增的。每个代码变更独立命名、独立管理。一个 SimpleWorkflow 可以有 5 个不同的 GetVersion 调用，各自独立判断。回放一条旧事件流时，有些变更的 VersionMarker 存在（走新逻辑），有些不存在（走旧逻辑），共存于同一次回放。

---

## 2. Fork — 物理分支路由

### 2.1 ForkWorkflow 函数签名

```go
func (e *Engine) ForkWorkflow(parentStreamID string, forkFromSeq int, newQuery string) (newStreamID string, err error)
```

- `parentStreamID`：原流 ID
- `forkFromSeq`：从该 seq 之后分叉。新流将复制前 forkFromSeq 个事件
- `newQuery`：新分支的任务指令（可修改原指令）
- 返回：新流的 streamID（如 `task:abc/fork:3`）

### 2.2 流 ID 树形谱系

```
task:<uuid>                         // 主分支
task:<uuid>/fork:<N>                // 从第 N 个事件分叉
task:<uuid>/fork:<N>/replay:<R>     // 回放子分支
```

流 ID 自身编码了谱系——不需要查数据库就能知道执行从哪里来。

### 2.3 Fork 执行契约

**Step 1 — 数据库物理拷贝**：
- 生成新流 ID：`newStreamID = parentStreamID + "/fork:" + forkFromSeq`
- 在单个 SQL 事务中执行：
  ```sql
  INSERT INTO event_log (stream_id, stream_seq, event_type, payload, parent_id, timestamp)
  SELECT ?, stream_seq, event_type, payload, ?, timestamp
  FROM event_log WHERE stream_id = ? AND stream_seq <= ?
  ```
  —将父流前 forkFromSeq 个事件物理复制到新流，`parent_id` 设置为父流 ID
- 写入 `ForkCreated` 事件（seq=forkFromSeq+1）：`{forked_from: parentStreamID, fork_at_seq: forkFromSeq}`

**Step 2 — 文件系统拷贝（按快照类型分叉）**：
- WorkspaceManager 查询 `snapshots` 表获取父流在第 forkFromSeq 步时的快照类型：
  - `snapshot_type == "git"` → `git checkout -b fork-{newSessionID} {snapshot_ref}`。毫秒级，零额外磁盘开销
  - `snapshot_type == "archive"` → 从 `snapshot_ref`（tar.gz 路径）解压到新 Session 的 workspace 目录

### 2.4 物理复制 vs 逻辑引用

Tenet 选择物理复制——每个 Fork 分支完全独立。父流删除不影响子流。SQLite 单文件下存储成本极低（每条事件 ~500B）。

### 2.5 核心场景

- **What-If 探索**：从第 5 步分叉，修改第 5 步的参数为方案 B，新分支独立执行。原分支保留方案 A 的完整结果
- **故障恢复**：第 3 步读错文件导致后续全错 → Fork 到第 3 步修正 → 新分支重新执行。原分支保留错误证据

---

## 3. 谱系查询

### 3.1 GetLineage

```go
func (s *Store) GetLineage(streamID string) ([]string, error)
```

沿 `parent_id` 递归向上，返回完整祖先链（从根到当前流）。SQL 实现：`WITH RECURSIVE lineage AS (SELECT stream_id, parent_id FROM event_log WHERE stream_id = ? UNION ALL SELECT e.stream_id, e.parent_id FROM event_log e INNER JOIN lineage l ON e.stream_id = l.parent_id WHERE e.parent_id != '') SELECT stream_id FROM lineage ORDER BY depth DESC LIMIT 50`

### 3.2 GetChildStreams

```go
func (s *Store) GetChildStreams(streamID string) ([]string, error)
```

`SELECT DISTINCT stream_id FROM event_log WHERE parent_id = ? AND stream_id != ?` ——只查直接子流，不递归。


## 4. 故障恢复完整轨迹

以「第 3 步读错文件，Fork 到第 3 步修正后重跑」为例：

1. 原始流 task:go-analyze：seq=1-2 正确 → seq=3 子任务因 workspace 中 `config.yaml` 是旧版本而产出错误结论（GenerateThought 返回错误分析）→ seq=4-5 基于错误结论继续分析 → TaskCompleted（错误结果）
2. 用户 Fork：`ForkWorkflow("task:go-analyze", 3, "修正 config.yaml 后重新分析")`
3. 新流 task:go-analyze/fork:3 创建：seq=1-2 物理复制 → seq=3 ForkCreated
4. WorkspaceManager 检测到 forkFromSeq=3 → 查 snapshots 表获取 seq=3 时的快照 → git checkout 或 tar 解压还原 workspace（此时 config.yaml 是修正前的版本——正确版本）
5. 用户或 Agent 修正 config.yaml → 新流从 seq=4 开始以首次执行模式运行 → Agent 读到正确的 config.yaml → 分析正确 → TaskCompleted
6. 两条流独立共存：`GetLineage("task:go-analyze/fork:3")` → `["task:go-analyze", "task:go-analyze/fork:3"]`
