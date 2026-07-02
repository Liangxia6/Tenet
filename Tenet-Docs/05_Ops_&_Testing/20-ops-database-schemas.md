# Database Schemas

> SQLite 物理 DDL · Redis Key 命名空间规范
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 1. SQLite DDL

### 1.1 `sessions`

```sql
CREATE TABLE IF NOT EXISTS sessions (
    id              TEXT PRIMARY KEY NOT NULL,
    workspace_path  TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'RUNNING'
                        CHECK (status IN ('RUNNING', 'PAUSED', 'COMPLETED', 'FAILED')),
    agent_config    TEXT,                               -- JSON：Session 配置快照（Configuration Freeze）
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_sessions_status ON sessions(status);
CREATE INDEX idx_sessions_created_at ON sessions(created_at);
```

### 1.2 `event_log`

```sql
CREATE TABLE IF NOT EXISTS event_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id       TEXT NOT NULL,
    stream_seq      INTEGER NOT NULL,
    event_type      TEXT NOT NULL,
    payload         TEXT NOT NULL,                      -- JSON：完整事件数据
    parent_id       TEXT,                               -- Fork 来源流 ID
    timestamp       TEXT NOT NULL DEFAULT (datetime('now')),

    CONSTRAINT uq_stream_seq UNIQUE (stream_id, stream_seq)
);

CREATE INDEX idx_event_log_stream_id ON event_log(stream_id);
CREATE INDEX idx_event_log_stream_id_seq ON event_log(stream_id, stream_seq);
CREATE INDEX idx_event_log_type ON event_log(event_type);
CREATE INDEX idx_event_log_timestamp ON event_log(timestamp);
CREATE INDEX idx_event_log_parent_id ON event_log(parent_id);
```

### 1.3 `snapshots`

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id       TEXT NOT NULL,
    stream_seq      INTEGER NOT NULL,
    snapshot_type   TEXT NOT NULL CHECK (snapshot_type IN ('git', 'archive')),
    snapshot_ref    TEXT NOT NULL,                       -- git: commit hash | archive: tar.gz 路径
    state_blob      TEXT NOT NULL,                      -- JSON：Projection 序列化状态
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
    -- NOTE: 不设 FOREIGN KEY stream_id → event_log(stream_id)，因为 event_log.stream_id 不是唯一列
    -- （一个流包含多条事件，唯一约束是 (stream_id, stream_seq) 复合键）。
    -- 引用完整性（流存在性 + 快照 seq 对应实际事件）由应用层 Workflow Engine 保证。
);

CREATE INDEX idx_snapshots_stream_id ON snapshots(stream_id);
CREATE INDEX idx_snapshots_stream_id_seq ON snapshots(stream_id, stream_seq DESC);
CREATE INDEX idx_snapshots_type ON snapshots(snapshot_type);
```

### 1.4 `token_telemetry`

```sql
CREATE TABLE IF NOT EXISTS token_telemetry (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id        TEXT NOT NULL,
    task_id           TEXT NOT NULL,
    agent_name        TEXT NOT NULL,
    model             TEXT NOT NULL,
    prompt_tokens     INTEGER NOT NULL CHECK (prompt_tokens >= 0),
    completion_tokens INTEGER NOT NULL CHECK (completion_tokens >= 0),
    cost_usd          REAL NOT NULL DEFAULT 0.0 CHECK (cost_usd >= 0.0),
    event_id          INTEGER NOT NULL,
    recorded_at       TEXT NOT NULL DEFAULT (datetime('now')),

    FOREIGN KEY (event_id) REFERENCES event_log(id) ON DELETE CASCADE
);

CREATE INDEX idx_token_telemetry_session ON token_telemetry(session_id);
CREATE INDEX idx_token_telemetry_task ON token_telemetry(task_id);
CREATE INDEX idx_token_telemetry_agent ON token_telemetry(agent_name);
CREATE INDEX idx_token_telemetry_recorded ON token_telemetry(recorded_at);
```

### 1.5 `schema_migrations`

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    applied_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### 1.6 PRAGMA Configuration

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;          -- 事件溯源必须 FULL：每次事务提交后数据已写入磁盘 platter
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;
PRAGMA cache_size=-64000;
PRAGMA temp_store=MEMORY;
```

---

## 2. Redis Key Schema

### 2.1 Session Lock & Fencing Token

| Key | 类型 | 值 | TTL | 操作 |
|---|---|---|---|---|
| `session_lock:{session_id}` | String | `agent_id` | 30s（可配） | `SETNX` 获取 · `EXPIRE` 续约 · Lua `DEL` 释放 |
| `session_fencing:{session_id}` | Integer | 单调递增 Token | 永久 | `INCR` 每次锁获取/续约时递增 |

### 2.2 Tool Rate Limit

| Key | 类型 | 值 | TTL | 操作 |
|---|---|---|---|---|
| `tool_rl:{tool_name}:{window_ts}` | Integer | 当前窗口内计数 | 窗口时长+1s | Lua 原子 `INCR` + `EXPIRE` + 阈值检查 |

### 2.3 SSE Stream

| Key | 类型 | 值 | TTL | 操作 |
|---|---|---|---|---|
| `sse:{stream_id}` | Pub/Sub Channel | JSON 事件消息 | 无 | `PUBLISH` 推送 · Web UI `SUBSCRIBE` 消费 |

---

## 3. Migration Scripts

迁移文件存放在 `go/internal/storage/migrations/`，按版本号命名：

```
001_initial_schema.sql
002_add_snapshot_type_column.sql
```

每个迁移在自己的事务中执行，成功后 `INSERT INTO schema_migrations(version)`。
