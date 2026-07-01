# Database Schemas

> SQLite 物理 DDL、Redis Key 设计规范

---

## 1. SQLite DDL

（待展开：四张表的完整 CREATE TABLE 语句——event_log / sessions / snapshots / token_telemetry。索引设计（stream_id+stream_seq 联合索引、timestamp 索引）。约束（NOT NULL、UNIQUE））

## 2. event_log Table

（待展开：字段级规范——id INTEGER PK、stream_id TEXT NOT NULL、stream_seq INTEGER NOT NULL、event_type TEXT NOT NULL、payload JSON、parent_id TEXT、timestamp TEXT。UNIQUE(stream_id, stream_seq) 约束）

## 3. sessions Table

（待展开：会话元数据——id/workspace_path/status（active/completed/failed）/agent_config JSON/created_at/updated_at）

## 4. snapshots Table

（待展开：快照存储——stream_id/stream_seq/state_blob BLOB/created_at。用于 Projection 恢复时的快速定位）

## 5. token_telemetry Table

（待展开：Token 用量记录——task_id/agent_id/model/prompt_tokens/completion_tokens/cost_usd/timestamp。聚合查询索引）

## 6. Redis Key Schema

（待展开：session_lock:<session_id>——Session Lock + Fencing Token。session_fencing:<session_id>——自增 Token 计数器。tool_rl:<tool_name>:<window_ts>——工具限频计数器。sse:task:<task_id>——SSE 推送频道）
