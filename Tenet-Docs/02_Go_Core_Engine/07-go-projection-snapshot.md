# Go Projection & Snapshot

> Projection Engine、快照策略、双触发器、工作区备份

---

## 1. Projection Interface

（待展开：Project(streamID) → ProjectionState 和 Snapshot(streamID, state) → error 的接口定义。Apply(event) 逐个消费事件的 fold 逻辑）

## 2. Projection Types

（待展开：TaskProjection（Task 流 fold → 进度/子任务/结果）、TimelineProjection（Agent 流 fold → Web UI 时间线）、TokenProjection（TokenUsed 事件 fold → 累计 token））

## 3. Snapshot Trigger Strategy

（待展开：双触发器并存——事件数触发器（每 50 事件）和时间触发器（每 5 分钟）。触发条件、并发控制、避免低流量时长时间无快照）

## 4. Snapshot Recovery

（待展开：恢复流程——查 snapshots 表最新快照 → 反序列化 → 读快照 seq 之后的事件 fold → 无快照则从头 fold。快照本身也是 event_log 中的事件）

## 5. Snapshot Storage

（待展开：snapshots 表结构（stream_id/seq/state_blob/timestamp）、序列化格式选择、快照文件归档策略）

## 6. Workspace Backup Manager

（待展开：与 WorkflowContext 的协作——执行前 Backup、成功 CleanBackup、失败 Restore。备份目录 `.backup/<session_id>/`。与 Fencing Token 的集成校验）
