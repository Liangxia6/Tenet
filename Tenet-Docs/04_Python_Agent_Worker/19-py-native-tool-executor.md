# Python Native Tool Executor

> ExecuteTool 路由 · 内置工具 · MCP 代理 · Skill 脚本 · 安全校验
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 1. 总体路由

Python 层收到 Go 的 `ExecuteTool` RPC 后，按以下顺序处理：

1. **Fencing Token 校验**——查询 Redis 当前值，与请求中的 token 比对。不匹配 → `PERMISSION_DENIED`，拒绝执行
2. **工具名路由**——根据 `tool_name` 前缀选择执行路径
3. **物理执行**——在本地 OS 上运行工具
4. **返回**——构造 `ExecuteToolResponse`（stdout/stderr/exit_code/duration_ms）

工具名路由规则：

| 前缀 | 路由目标 | 说明 |
|---|---|---|
| `mcp:` 开头 | MCP 代理调用 | 如 `mcp:github:create_issue` → 向对应的 MCP Server 子进程发送 JSON-RPC |
| `skill:` 开头 | Skill 脚本执行 | 如 `skill:go-performance:run_pprof` → 查找并运行本地脚本 |
| 其他 | 标准工具注册表 | 如 `read_file`、`shell`、`write_file`、`web_search` |

---

## 2. 标准工具注册表

Python 启动时，`ToolRegistry` 注册内置工具。每个工具实现统一的 `Tool` 接口：

**`Tool` 接口契约**：
- `name: str` — 工具唯一名称（LLM 通过此名调用）
- `description: str` — 工具功能描述（注入 LLM prompt）
- `parameters_schema: dict` — JSON Schema（定义参数结构）
- `execute(arguments: dict, workspace: str) -> ToolResult` — 物理执行

**`ToolResult` 结构**：`stdout: str`、`stderr: str`、`exit_code: int`、`is_error: bool`、`duration_ms: int`

### 2.1 ReadFile

| 属性 | 值 |
|---|---|
| name | `read_file` |
| 参数 | `path`（必填，相对路径）、`offset`（默认 1）、`limit`（默认 500） |
| 行为 | 读取 workspace 内指定文件的内容。offset/limit 控制分页 |
| 安全 | 路径必须通过防越权校验 |

### 2.2 WriteFile

| 属性 | 值 |
|---|---|
| name | `write_file` |
| 参数 | `path`（必填）、`content`（必填） |
| 行为 | 覆盖写入文件，自动创建父目录 |
| 安全 | 路径防越权校验 + Fencing Token 校验 |

### 2.3 Shell

| 属性 | 值 |
|---|---|
| name | `shell` |
| 参数 | `command`（必填）、`timeout`（默认 60s） |
| 行为 | 在 workspace 目录下执行 Shell 命令，捕获 stdout/stderr/exit_code |
| 安全 | 危险命令黑名单拦截（`rm -rf /`、`mkfs.`、`dd if=`、fork bomb 模式） |

黑名单匹配逻辑：在传入的 `command` 字符串中搜索预定义的危险模式，命中任一 → 拒绝执行，返回错误结果。

### 2.4 WebSearch

| 属性 | 值 |
|---|---|
| name | `web_search` |
| 参数 | `query`（必填）、`limit`（默认 5） |
| 行为 | 向搜索引擎发起查询，返回前 N 条结果的摘要 |
| 安全 | 无特殊约束——只读操作 |

---

## 3. MCP 工具代理执行

`tool_name` 格式：`mcp:{server_name}:{tool_name}`（如 `mcp:github:create_issue`）。

**执行流程**：
1. 从 `tool_name` 中解析 `server_name` 和 `tool_name`
2. 在 `MCPClientManager` 中查找对应的 `MCPServerProcess`
3. 通过 Stdio 向子进程发送 JSON-RPC `tools/call` 请求
4. 等待子进程返回结果
5. 将返回的 `content` 数组序列化为 JSON 字符串作为 stdout

详细 MCP 子进程管理见 `18-py-llm-adapters.md` 第 3 节。

---

## 4. Skill 脚本执行（Design B：Go 管选择，Python 管执行）

`tool_name` 格式：`skill:{skill_name}:{script_name}`（如 `skill:go-performance:run_pprof`）。

**Skill 两阶段分工**：
- **Phase 1 — Go 层（Skill Management）**：Go 在 Session 启动时加载 `config/skills/` 下的 Skill 定义文件（Markdown+YAML frontmatter），解析工具权限清单，执行安全审计（拦截危险工具声明），匹配当前任务需要的 Skill，将匹配到的 Skill 指令文本注入 `GenerateThoughtRequest.system_prompt`。Go 决定「用哪些 Skill、何时注入」。
- **Phase 2 — Python 层（Skill Execution）**：LLM 在执行过程中选择调用某个 skill 的工具 → Go 发出 `ExecuteTool(skill:{name}:{script}, args)` → Python 作为执行层运行该脚本。Python 只负责「执行」，不管理 Skill 的生命周期或选择逻辑。

脚本存放位置：`config/skills/core/{skill_name}/scripts/` 或 `config/skills/user/{skill_name}/scripts/`，优先查找 user 路径。

**执行流程**：
1. 在 `config/skills/` 目录下查找脚本文件
2. 路径防越权校验
3. 以异步子进程方式运行脚本（`asyncio.create_subprocess_exec`），工作目录设为 workspace
4. 将工具参数序列化为 JSON 写入子进程的 stdin
5. 捕获 stdout/stderr，超时后强制终止

---

## 5. 安全校验

### 5.1 Fencing Token 校验

每次工具执行前，查询 Redis 的 `session_fencing:{session_id}` 值，与请求中的 `fencing_token` 比对。不匹配 = 锁已被抢占（脑裂场景）→ 拒绝执行。

### 5.2 路径防越权（双重校验）

所有文件操作必须经过：
1. **前缀校验**：拼接后的绝对路径必须以 workspace 根路径开头
2. **符号链接追踪**：`os.path.realpath()` 解析后再次做前缀校验——防止通过符号链接跳出 workspace

### 5.3 Shell 危险命令黑名单

预定义的危险模式列表，在 `command` 字符串中扫描匹配。命中 → 拒绝执行，返回错误。
