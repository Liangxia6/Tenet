# Go Event Store

> SQLite Event Store：DDL · WriteDaemon · 事务连续断言 · 幂等写入
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 1. Why SQLite

Tenet 选择 SQLite 而非 PostgreSQL/EventStoreDB/Kafka：单文件零部署、本地文件系统零网络延迟、完整 ACID 事务。低并发高可靠场景下，SQLite 的单写者模型恰好匹配——不需要多写者并发，只需要不会丢数据的单写者。

---

## 2. Engine Configuration

**WAL 模式**（`PRAGMA journal_mode=WAL`）：写追加到 WAL 文件末尾，读直接访问数据库文件。多读单写互不阻塞——WriteDaemon 高频写入时，Projection 查询和 Web UI 历史回看不被拖慢。

**连接池**：`SetMaxOpenConns(1)` 铁律——物理上禁止两个连接同时竞争写锁。配合 WriteDaemon 单 goroutine 写入，彻底消除 `SQLITE_BUSY`。

**其他 Pragma**：`synchronous=FULL`（事件溯源要求每次事务提交后数据必须已写入磁盘 platter，不能只到 OS 缓冲——NORMAL 在 OS 崩溃时可能丢失最后一个事务）、`foreign_keys=ON`、`busy_timeout=5000`、`cache_size=-64000`（64MB）、`temp_store=MEMORY`。

**WAL Checkpoint**：被动 checkpoint（`wal_autocheckpoint=1000`，每 1000 页触发一次自动 checkpoint）。主动 checkpoint 由 `WriteDaemon` 在每次关闭时调用 `PRAGMA wal_checkpoint(TRUNCATE)` 确保 WAL 文件被吸收回主数据库。

---

## 3. DDL

### 3.1 event_log

```sql
CREATE TABLE IF NOT EXISTS event_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id       TEXT NOT NULL,
    stream_seq      INTEGER NOT NULL,
    event_type      TEXT NOT NULL,
    payload         TEXT NOT NULL,           -- JSON：完整事件数据含 LLM 原始输出
    parent_id       TEXT,                    -- Fork 来源流 ID
    timestamp       TEXT NOT NULL DEFAULT (datetime('now')),
    CONSTRAINT uq_stream_seq UNIQUE (stream_id, stream_seq)
);
CREATE INDEX idx_event_log_stream_id ON event_log(stream_id);
CREATE INDEX idx_event_log_stream_id_seq ON event_log(stream_id, stream_seq);
CREATE INDEX idx_event_log_type ON event_log(event_type);
CREATE INDEX idx_event_log_timestamp ON event_log(timestamp);
CREATE INDEX idx_event_log_parent_id ON event_log(parent_id);
```

`UNIQUE(stream_id, stream_seq)` 是回放正确性的数学底线——防止空洞和重复。

### 3.2 sessions

```sql
CREATE TABLE IF NOT EXISTS sessions (
    id              TEXT PRIMARY KEY NOT NULL,
    workspace_path  TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'RUNNING'
                        CHECK (status IN ('RUNNING','PAUSED','COMPLETED','FAILED')),
    agent_config    TEXT,                    -- JSON：配置快照
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### 3.3 snapshots

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id       TEXT NOT NULL,
    stream_seq      INTEGER NOT NULL,
    snapshot_type   TEXT NOT NULL CHECK (snapshot_type IN ('git','archive')),
    snapshot_ref    TEXT NOT NULL,           -- git: commit hash | archive: tar.gz 路径
    state_blob      TEXT NOT NULL,           -- JSON：Projection 序列化状态
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
    -- NOTE: 不设 FOREIGN KEY stream_id → event_log(stream_id)，因为 event_log.stream_id 不是唯一列
    -- （一个流包含多条事件，唯一约束是 (stream_id, stream_seq) 复合键）。
    -- 引用完整性（流存在性 + 快照 seq 对应实际事件）由应用层 Workflow Engine 保证，而非数据库约束。
);
CREATE INDEX idx_snapshots_stream_id_seq ON snapshots(stream_id, stream_seq DESC);
```

恢复时 `ORDER BY stream_seq DESC LIMIT 1` 取最新快照。

### 3.4 token_telemetry

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
```

每条记录对应一个 `TokenUsed` 事件。`CHECK >= 0` 防负值写入。聚合查询按 session/task/agent/时间窗口均被索引覆盖。

### 3.5 schema_migrations

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    applied_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
```

---

## 4. Store Interface

```go
type Store interface {
    Append(streamID, parentID, eventType, payload string) (streamSeq int64, err error)
    Read(streamID string, fromSeq, toSeq int64) ([]Event, error)
    ReadAll(streamID string) ([]Event, error)
    GetLineage(streamID string) ([]string, error)
    GetChildStreams(streamID string) ([]string, error)
    GetLatestSeq(streamID string) (int64, error)
}
```

- `Append` 通过 WriteDaemon 提交——每次是一个 WriteRequest（含连续性检查 + INSERT + 可选 token_telemetry/sessions 更新）
- `Read` 类方法直接查 SQLite（不经过 WriteDaemon）——WAL 模式下读不冲突写
- `GetLineage` 通过 `WITH RECURSIVE` CTE 沿 parent_id 递归

---

## 5. WriteDaemon — 单写守护协程

### 5.1 问题

10 个 Workflow 并发，Python 每秒回传 30-50 个事件 → 多个 goroutine 竞争 SQLite 写锁 → `SQLITE_BUSY` 雪崩。

### 5.2 WriteRequest 结构

```go
type WriteRequest struct {
    Statements []WriteStatement    // 同一事务中执行的 SQL 列表
    ResultCh   chan WriteResult    // 调用方阻塞等待
}
type WriteStatement struct {
    SQL  string
    Args []any
}
type WriteResult struct {
    LastInsertID int64
    RowsAffected int64
    Err          error
}
```

### 5.3 WriteDaemon 行为

```go
func (wd *WriteDaemon) Run() {
    for req := range wd.writeCh {
        tx := db.Begin()
        for _, stmt := range req.Statements {
            result, err := tx.Exec(stmt.SQL, stmt.Args...)
            if err != nil {
                tx.Rollback()
                req.ResultCh <- WriteResult{Err: err}
                continue  // 处理下一个请求
            }
        }
        tx.Commit()
        req.ResultCh <- WriteResult{...}
    }
}
```

启动：`writeCh = make(chan WriteRequest, queueSize)`，`go runWriteDaemon()`。关闭：`close(writeCh)` → WriteDaemon 消费完剩余后退出。

### 5.4 调用方视角

```go
result := wd.Submit(statements...)  // 同步阻塞，等待事务完成
```

调用方不需要知道连接、事务、锁的细节。

---

## 6. 事务完整性

### 6.1 原子追加

向 event_log 写入事件时，与相关写入（token_telemetry、sessions 更新）包裹在同一事务中。失败 → 无条件 ROLLBACK。不存在「event_log 成功但 token_telemetry 失败」的中间态。

### 6.2 连续性断言

INSERT 前在事务内执行：`SELECT COALESCE(MAX(stream_seq), 0) FROM event_log WHERE stream_id = ?`。断言 `stream_seq == maxSeq + 1`。失败 → ROLLBACK + panic——防止空洞和重复。

### 6.3 幂等写入

Decide 的 Immediate Commit 保证同一 Decide 只执行一次。gRPC 超时重试时，`UNIQUE(stream_id, stream_seq)` 约束拒绝重复写入——Go 视为成功。

---

## 7. Migration

迁移脚本 `NNN_description.sql` 放在 `go/internal/storage/migrations/`。启动时按版本号升序执行未应用的迁移，每个在自己的事务中，成功后 INSERT schema_migrations。

---

## 8. Backup & Recovery

SQLite 单文件是系统唯一的权威数据源。备份策略：

**在线备份**：SQLite 提供 `sqlite3_backup_init()` API，可在 WriteDaemon 运行时进行热备份——锁定数据库的瞬间复制数据页，不影响并发读。Go 层通过 `go-sqlite3` 的 `Backup()` 方法实现。

**备份触发**：
- **自动**：Go 进程每小时执行一次本地备份，写入 `data/backups/tenet_{timestamp}.db`。保留最近 24 个备份（滚动覆盖）。
- **手动**：CLI 提供 `tenet backup` 命令，按需生成完整快照。

**恢复**：
- 停止 Go + Python 进程
- 将备份文件复制到 `data/tenet.db`
- 启动 Go 进程 → WAL 文件自动重建 → 业务恢复

**注意**：Redis 中的运维状态（Session Lock、Fencing Token、Rate Limit 计数器）**不备份**。恢复后所有活跃 Session 的锁丢失，正在执行的任务会自终止并创建新 Session。事件流（event_log）完整保留，任务可从中断点恢复。
