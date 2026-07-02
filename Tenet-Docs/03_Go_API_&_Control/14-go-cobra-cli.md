# Go Cobra CLI

> Cobra 命令树 · tenet serve / task run / task replay / task fork · Flag 规范
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 1. Command Tree

```
tenet
├── serve             启动 gRPC Server + Workflow Engine + Redis Pool
├── task
│   ├── run           创建并执行一个 Task
│   ├── replay        回放指定 Task 流
│   ├── fork          从指定流的指定 seq 分叉新 Task
│   ├── list          列出最近的 Task
│   └── inspect       查看指定 Task 的事件流和状态
├── session
│   ├── list          列出活跃 Session
│   └── inspect       查看 Session 的工作区和 Agent 状态
├── config
│   ├── validate      校验 tenet.yaml 配置
│   └── show          显示当前生效的配置（静态 + 动态合并）
└── version           打印版本号
```

---

## 2. Global Flags

所有命令共用的 Flag，在根命令 `tenet` 上定义：

| Flag | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `--config` | String | `config/tenet.yaml` | 配置文件路径 |
| `--verbose` | Bool | `false` | 输出 DEBUG 级别日志 |
| `--output` | String | `text` | 输出格式：`text` / `json` |

---

## 3. `tenet serve`

启动 Go 层的所有运行时组件。

```
tenet serve [--port 50051] [--worker-port 50052] [--db data/tenet.db]

启动流程:
  1. 加载 tenet.yaml
  2. Storage Init: SQLite 建库 + 迁移
  3. Redis Pool 初始化（PING 验证，不可用则 Warn + 降级）
  4. 组件组装（依赖注入）: Config → Storage → EventStore → ProjectionEngine
     → WorkflowEngine → EventRouter → gRPC Gateway → LockManager → TokenBudget
  5. TenetOrchestrator gRPC Server 启动（:50051）
  6. Health check → 打印 "tenet ready"
```

| Flag | 类型 | 默认值 | 覆盖配置项 |
|---|---|---|---|
| `--port` | Int | 50051 | `grpc.orchestrator_port` |
| `--worker-port` | Int | 50052 | `grpc.worker_port` |
| `--db` | String | — | `database.path` |
| `--redis` | String | — | `redis.addr` |

---

## 4. `tenet task run`

创建并执行一个 Task。

```
tenet task run "分析 ~/code/goserver 的性能瓶颈" --workspace ~/code/goserver --workflow auto
```

| Flag | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `--workspace` | String | `./` | 工作区路径（绝对或相对） |
| `--workflow` | String | `auto` | 覆盖策略路由：`auto` / `simple` / `dag` / `react` / `coding` / `scientific` |
| `--model` | String | — | 覆盖 LLM 模型 |
| `--max-steps` | Int | — | 覆盖最大步数 |
| `--token-budget` | Int | — | 覆盖 Token 预算 |
| `--wait` | Bool | `true` | 等待 Task 完成后退出（false = 提交后立即返回 task_id） |

输出（`--output=text`）：

```
task_id:    task:a1b2c3d4
status:     RUNNING → COMPLETED
workflow:   dag (auto-detected, complexity: 0.65)
subtasks:   5 (4 parallel + 1 aggregate)
tokens:     45,230 ($0.83)
duration:   2m34s
result:     分析完成，发现 7 个性能瓶颈
artifacts:  findings/summary.md
            findings/profiler-cpu.md
            findings/profiler-mem.md
            findings/reviewer-goroutine.md
            findings/reviewer-io-lock.md
```

输出（`--output=json`）：

```json
{
  "task_id": "task:a1b2c3d4",
  "status": "COMPLETED",
  "workflow": "dag",
  "complexity_score": 0.65,
  "subtasks": 5,
  "tokens": 45230,
  "cost_usd": 0.83,
  "duration_ms": 154000,
  "result": "分析完成，发现 7 个性能瓶颈",
  "artifacts": ["findings/summary.md", "findings/profiler-cpu.md", "..."]
}
```

---

## 5. `tenet task replay`

回放指定 Task 流，验证确定性。

```
tenet task replay --stream task:a1b2c3d4
```

| Flag | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `--stream` | String | **必填** | 要回放的 stream_id |
| `--from-seq` | Int | 0 | 从指定 seq 开始回放 |
| `--dry-run` | Bool | `false` | 仅验证确定性，不执行新 Decide |

输出：

```
Replaying task:a1b2c3d4 (17 events)
  seq=1   TaskCreated           ✓ (skipped)
  seq=2   ComplexityAnalyzed    ✓ (skipped)
  seq=3   SubTaskDispatched{sub-1}  ✓ (skipped)
  ...
Deterministic check: PASSED
Replay duration: 12ms
```

---

## 6. `tenet task fork`

从指定流的指定 seq 分叉。

```
tenet task fork --stream task:a1b2c3d4 --at-seq 3 "修正 config.yaml 后重新分析"
```

| Flag | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `--stream` | String | **必填** | 父流 ID |
| `--at-seq` | Int | **必填** | 从该 seq 之后分叉 |
| `--workspace` | String | — | 新工作区路径（默认继承父流） |
| `--wait` | Bool | `true` | 等待完成后退出 |

输出：

```
Forked from:  task:a1b2c3d4 at seq=3
New stream:   task:a1b2c3d4/fork:3
Status:       RUNNING → COMPLETED
...
```

---

## 7. `tenet task list` / `tenet task inspect`

```
tenet task list [--limit 10] [--status completed]
```

输出：

```
STREAM ID              STATUS      WORKFLOW  TOKENS   DURATION  CREATED
task:a1b2c3d4          COMPLETED   dag       45,230   2m34s     2026-07-03T10:23:00Z
task:e5f6g7h8          COMPLETED   simple    1,200    8s        2026-07-03T09:15:00Z
task:i9j0k1l2/fork:3   RUNNING     dag       —        —         2026-07-03T10:45:00Z
```

```
tenet task inspect --stream task:a1b2c3d4
```

输出完整事件流（seq、type、timestamp）和 Task 当前状态（Progress、Subtasks、Findings）。

---

## 8. `tenet config validate`

校验配置文件格式和值域。

```
tenet config validate [--config config/tenet.yaml]
```

校验项：
- YAML 语法正确性
- 必填字段非空（`database.path`、`grpc.orchestrator_port`）
- 类型正确（`busy_timeout_ms` 是 Integer，非 String）
- 数值范围（`TTL > heartbeat`、`execute_timeout > control_timeout`）
- `env:` 引用的环境变量存在
- Provider 配置完整（至少一个 `llm_providers.*.api_key`）

输出：
```
✓ YAML syntax
✓ database.path: data/tenet.db
✓ grpc.orchestrator_port: 50051
✓ grpc.worker_port: 50052
✓ redis.addr: 127.0.0.1:6379
✓ llm_providers.openai.api_key: env:OPENAI_API_KEY → resolved
✓ llm_providers.anthropic.api_key: env:ANTHROPIC_API_KEY → resolved
All checks passed.
```

---

## 9. `tenet config show`

显示当前生效的完整配置（静态基线 + 动态覆盖合并结果）。

```
tenet config show [--format json]
```

只显示非敏感字段（`api_key` 显示为 `****`）。需要 `--include-secrets` Flag 才显示明文（仅用于调试）。
