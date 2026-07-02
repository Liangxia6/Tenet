# Tenet Configuration Specification

> 静态与动态 YAML 配置文件规范
>
> 文档状态：DRAFT · 版本：v1.1.0
>
> 配置分为「静态基础配置」与「动态行为配置」两层。静态决定系统如何启动，动态通过事件溯源持久化，保证变更历史可追溯。

---

## 1. Config Classification

每个配置项属于三类之一：

| 类别 | 修改方式 | 生效时机 | 示例 |
|---|---|---|---|
| Static-Immutable | 编辑 yaml + 重启 | 进程重启后 | `database.path`、`grpc.orchestrator_port` |
| Static-HotReload | 编辑 yaml + SIGHUP | 下一个新 Session 启动时 | `rate_limits.*`、`llm_providers.*.default_model` |
| Dynamic | 通过事件溯源写入 `system:config` 流 | 下一个新 Session 启动时 | `workflow.default_strategy`、模型路由映射 |

**Configuration Freeze**：Session 启动时，当前 EffectiveConfig 被捕获为只读快照，绑定到该 Session 的 WorkflowContext。Session 整个生命周期只读此快照，SIGHUP 和动态变更只影响此后新启动的 Session。

---

## 2. Static Configuration 参数表

### 2.1 database（SQLite）

| 参数路径 | 类型 | 默认值 | 类别 | 说明 |
|---|---|---|---|---|
| `database.path` | String | `data/tenet.db` | Static-Immutable | 数据库文件路径 |
| `database.max_open_conns` | Integer | `1` | Static-Immutable | 必须为 1——强制单连接，配合 WriteDaemon |
| `database.busy_timeout_ms` | Integer | `5000` | Static-Immutable | 被锁时的等待超时 |
| `database.write_queue_size` | Integer | `1000` | Static-Immutable | WriteDaemon 缓冲 channel 大小 |
| `database.wal_mode` | Boolean | `true` | Static-Immutable | 启用 WAL 模式 |

### 2.2 scheduler

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `scheduler.queue_size` | Integer | `100` | Static-HotReload |

`scheduler.queue_size` 控制调度器 `workflowQueue` 的容量。调度队列与 SQLite 写入队列物理解耦：数据库的 `database.write_queue_size` 仅影响 WriteDaemon，调度器缓存高峰任务时不再受数据库配置牵连。

### 2.3 redis

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `redis.addr` | String | `127.0.0.1:6379` | Static-Immutable |
| `redis.password` | String | — | Static-Immutable（支持 `env:REDIS_PASSWORD`） |
| `redis.db` | Integer | `0` | Static-Immutable |
| `redis.session_lock_ttl_seconds` | Integer | `30` | Static-HotReload |
| `redis.session_heartbeat_seconds` | Integer | `10` | Static-HotReload |

**Redis 不可用时降级**：Go 进程内 `map[session_id]*sync.Mutex` 本地锁。SQLite 的 WriteDaemon 保护数据库，本地锁保护 `workspaces/` 物理文件。

### 2.3 grpc

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `grpc.orchestrator_port` | Integer | `50051` | Static-Immutable |
| `grpc.worker_port` | Integer | `50052` | Static-Immutable |
| `grpc.control_timeout_seconds` | Integer | `60` | Static-HotReload |
| `grpc.execute_timeout_seconds` | Integer | `300` | Static-HotReload |
| `grpc.retry_max_attempts` | Integer | `3` | Static-HotReload |
| `grpc.retry_backoff_base_ms` | Integer | `1000` | Static-HotReload |
| `grpc.circuit_breaker_threshold` | Integer | `5` | Static-HotReload |
| `grpc.circuit_breaker_timeout_seconds` | Integer | `30` | Static-HotReload |

### 2.4 workflow

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `workflow.max_concurrent_tasks` | Integer | `10` | Static-HotReload |
| `workflow.default_strategy` | String | `"auto"` | Dynamic（`auto`/`simple`/`dag`） |
| `workflow.complexity_threshold_dag` | Float | `0.3` | Dynamic（>= 此值走 DAGWorkflow，否则 Simple） |
| `workflow.snapshot_event_interval` | Integer | `50` | Static-HotReload |
| `workflow.snapshot_time_interval_seconds` | Integer | `300` | Static-HotReload |
| `workflow.record_batch_size` | Integer | `20` | Static-Immutable |

**Record 批量提交约束**：任何 `Decide` 发起前，必须 `flushRecords()` 清空所有 pending Records。原因：Record 是回放确定性检查的物理指纹——如果 Decide 已落盘但其前面的 Record 因崩溃丢失，重放指针偏移导致 panic。

### 2.5 workspace

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `workspace.base_path` | String | `workspaces/` | Static-Immutable |
| `workspace.snapshot_driver` | String | `"auto"` | Dynamic（`auto`/`git`/`archive`） |
| `workspace.exclude_patterns` | List[String] | `["node_modules/", ".venv/", "*.bin", "*.exe", "*.dylib", ".DS_Store"]` | Static-HotReload |
| `workspace.backup_enabled` | Boolean | `true` | Static-HotReload |
| `workspace.backup_retention_count` | Integer | `3` | Static-HotReload |
| `workspace.cleanup_on_session_end` | Boolean | `true` | Static-HotReload |

**snapshot_driver 自适应逻辑**（`auto` 模式）：WorkspaceManager 检测工作空间中 >= 90% 文件为文本类型 → Git 增量驱动；否则 → Archive 物理打包。

### 2.6 skills

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `workspace.skills_path` | String | `config/skills/` | Static-Immutable |
| `workspace.skills_auto_discover` | Boolean | `true` | Static-HotReload |

Skill 文件格式：Markdown + YAML Frontmatter（`name`/`version`/`description`/`requires_tools`/`triggers`）。Go 层 Phase 1 扫描 `config/skills/core/` 和 `config/skills/user/`，解析 YAML 头部，审计高危工具权限，根据 Task query 语义匹配相关 Skill，将匹配的 Markdown 文本注入 system prompt。

### 2.7 mcp_servers

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `mcp_servers[].name` | String | — | Static-Immutable |
| `mcp_servers[].command` | String | — | Static-Immutable |
| `mcp_servers[].args` | List[String] | — | Static-Immutable |
| `mcp_servers[].env` | Map[String,String] | — | Static-Immutable |
| `mcp_servers[].enabled` | Boolean | `true` | Static-HotReload |

Python 层在首次 GenerateThought 时，对每个 enabled MCP Server 通过 `asyncio.create_subprocess_exec` 拉起 Stdio 子进程，通过 `tools/list` 发现工具 Schema，翻译为 `ToolDefinition` 列表，通过 `GenerateThoughtResponse.discovered_tools` 回传给 Go 层。Go 层收到后写入 `ToolsDiscovered` 事件到 event_log，**下一轮** GenerateThought 的 `request.tools` 才包含 MCP 工具。首轮不包含 MCP 工具是预期行为——避免 Schema 尚未就绪时 LLM 尝试调用。

### 2.8 agent

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `agent.default_max_steps` | Integer | `50` | Dynamic |
| `agent.default_temperature` | Float | `0.7` | Dynamic |
| `agent.convergence_no_tool_calls` | Integer | `3` | Dynamic（连续 N 次无工具调用 → 强制收敛） |
| `agent.loop_detection_repeat_threshold` | Integer | `3` | Dynamic（连续 N 次同一工具 → 死循环检测） |
| `agent.default_token_budget` | Integer | `100000` | Dynamic |

### 2.9 safety

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `safety.require_approval` | List[String] | `["shell"]` | Static-HotReload（需人工审批的工具列表。在 ReactWorkflow 中，匹配的工具调用前触发 HITL 挂起） |
| `safety.max_auto_fix_retries` | Integer | `3` | Dynamic（CodingWorkflow autoFix 最大重试次数） |
| `safety.shell_dangerous_patterns` | List[String] | `["rm -rf /", "mkfs.", "dd if=", "> /dev/", "fork bomb"]` | Static-HotReload |

### 2.10 interactive

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `interactive.human_timeout_seconds` | Integer | `300` | Dynamic（HITL 等待人工审批的超时秒数。超时后自动拒绝） |
| `interactive.inject_prefix` | String | `"[Human Feedback]\n"` | Dynamic（人工反馈注入到 messages 的前缀文本） |

### 2.11 rate_limits

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `rate_limits.shell.max_per_minute` | Integer | `30` | Static-HotReload |
| `rate_limits.shell.max_per_second` | Integer | `5` | Static-HotReload |
| `rate_limits.web_search.max_per_minute` | Integer | `10` | Static-HotReload |
| `rate_limits.write_file.max_per_second` | Integer | `20` | Static-HotReload |
| `rate_limits.llm_call.max_per_minute` | Integer | `60` | Static-HotReload |

### 2.12 llm_providers

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `llm_providers.<name>.adapter` | String | — | Static-Immutable（`openai` / `anthropic` / `openai_compatible` / `ollama`） |
| `llm_providers.<name>.base_url` | String | — | Static-HotReload |
| `llm_providers.<name>.api_key` | String | — | Static-Immutable（格式：`env:OPENAI_API_KEY`） |
| `llm_providers.<name>.default_model` | String | — | Dynamic |
| `llm_providers.<name>.models` | List[String] | — | Dynamic |
| `llm_providers.<name>.max_concurrency` | Integer | `5` | Static-HotReload |
| `llm_providers.<name>.timeout_seconds` | Integer | `120` | Static-HotReload |

### 2.13 coding

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `coding.static_check_cmd` | String | `"go vet ./..."` | Dynamic（CodingWorkflow Phase 4 运行的静态检查命令） |
| `coding.test_cmd` | String | `"go test ./..."` | Dynamic（CodingWorkflow Phase 5 运行的测试命令） |
| `coding.auto_fix_max_retries` | Integer | `3` | Dynamic |

### 2.14 logging

| 参数路径 | 类型 | 默认值 | 类别 |
|---|---|---|---|
| `logging.level` | String | `"info"` | Static-HotReload |
| `logging.format` | String | `"json"` | Static-Immutable |
| `logging.output` | String | `"stdout"` | Static-Immutable |

---

## 3. Dynamic Configuration（事件驱动）

涉及业务策略的配置变更严禁直接编辑 yaml。必须通过事件溯源持久化：

- **系统流**：`stream_id = "system:config"`，`event_type = "ConfigChanged"`
- **Payload**：`param_path`、`old_value`、`new_value`、`operator_id`、`reason`
- **Config Projection**：启动时 fold `system:config` 流的全部事件 → 得到 DynamicConfig 覆盖层 → 合并到 EffectiveConfig
- **Dynamic-Only Keys 白名单**：仅 `workflow.*`、`agent.*`、`llm_providers.*.default_model`、`llm_providers.*.models`、`rate_limits.*` 允许动态变更。Static-Immutable key 的变更被静默忽略

---

## 4. Environment Variable Resolution

**Go 端解析，单向传递**。`env:OPENAI_API_KEY` → Go 在 Phase 1 加载 yaml 时调 `os.Getenv("OPENAI_API_KEY")` → 解析为明文 → 存入 RuntimeConfig → 通过 gRPC 将明文传给 Python。Python 的 Pydantic 只校验明文格式，不做 env var 二次解析。Go 和 Python 可能在不同容器/主机上——Python 本地可能没有该环境变量。

---

## 5. Validation

**Go 端启动时**：`yaml.Unmarshal` 类型绑定 + 必填字段 + `env:` 环境变量存在性 + 数值范围（TTL > heartbeat）。失败 → panic 阻断启动。

**Python 端运行时**：Pydantic v2 校验 GenerateThoughtRequest 和 ExecuteToolRequest 消息体。失败 → `INVALID_ARGUMENT` gRPC 错误 → Go 不重试。
