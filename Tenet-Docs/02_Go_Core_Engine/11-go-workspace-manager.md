# Go Workspace Manager

> workspaces/ 物理目录 · Git/Archive 双策略 · Backup/Restore/Cleanup · 安全校验
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 1. 目录结构

`workspaces/{session_id}/`——每个 Session 独立目录。内含：Agent 工作文件、`findings/`（Agent 间 File-as-Memory 共享区）、`.git/`（Git 快照仓库）、`.backup/`（临时备份目录）。

## 2. Hybrid Snapshot 双策略

### 2.1 Git 驱动

用于纯文本工作空间（≥90% 文本文件或 `snapshot_driver="git"`）。

**操作**：
- `GitCommit(path, message) → commitHash`：`git add -A && git commit -m message`——仅存增量 delta
- `GitCheckout(path, hash)`：`git checkout hash`——毫秒级 HEAD 跳转，零额外磁盘开销
- `GitDiff(path, baseHash) → diffText`：`git diff baseHash`——InteractiveWorkflow 用
- `GitResetHard(path, hash)`：`git reset --hard hash`——CodingWorkflow 的 autoFix 失败后物理回滚

### 2.2 Archive 驱动

用于非文本/混合工作空间（<90% 文本或 `snapshot_driver="archive"`）。

**操作**：
- `CreateArchive(path, sessionID, seq, excludePatterns) → archivePath`：`tar -czf` 打包，排除 `exclude_patterns` 匹配的文件（`.venv/`、`node_modules/`、`*.bin`、`*.exe` 等）
- `ExtractArchive(archivePath, destPath)`：`tar -xzf` 解压

### 2.3 文本占比检测

`AnalyzeTextRatio(path) → float64`：遍历工作空间中所有文件，统计扩展名在文本白名单（.go/.py/.rs/.js/.ts/.md/.yaml/.json/.toml/.txt/.sh/.sql 等）中的占比。

---

## 3. 备份生命周期

| 操作 | 触发时机 | 前置校验 | 行为 |
|---|---|---|---|
| `Backup(sessionID)` | Decide 前 | `ValidateFencingToken` | workspace → `.backup/{sessionID}/`（Git commit 或 tar.gz） |
| `Restore(sessionID)` | Decide 失败、准备重试 | `ValidateFencingToken` | 清空 workspace → 从 `.backup/` 还原 |
| `CleanBackup(sessionID)` | Decide 成功后 | — | `rm -rf .backup/{sessionID}/` |
| `Cleanup(sessionID)` | Session 结束 | `ValidateFencingToken`（如果 `cleanup_on_session_end`） | `rm -rf workspaces/{sessionID}/` |

---

## 4. Fencing Token 集成

Backup/Restore/Cleanup/GitCommit/GitResetHard/CreateArchive 操作前必须调用 `LockManager.ValidateFencingToken(lease)`——校验本地持有的 token 与 Redis 当前值一致。Redis 不可用时降级到 Go 进程内本地 `sync.Mutex`。

---

## 5. 路径防越权

所有文件操作经 `validatePath(sessionID, relativePath)` 双重校验：
1. 前缀校验：`filepath.Clean(join(workspace, relative))` 必须以 workspace 根路径开头
2. 符号链接追踪：`filepath.EvalSymlinks()` 解析后再次前缀校验
