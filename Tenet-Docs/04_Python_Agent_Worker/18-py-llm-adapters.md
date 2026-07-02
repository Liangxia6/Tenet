# Python LLM Adapters

> BaseAdapter 契约 · Jinja2 编译 · MCP 集成 · Provider 适配 · Pydantic 校验
>
> 文档状态：DRAFT · 版本：v1.1.0
>
> **物理红线**：Python 层 100% 无状态。每次 `GenerateThought` 是一次独立的、原子化的 LLM 推理。不管理消息窗口、不执行历史压缩、不缓存上下文。Go 层负责全部循环、记账和窗口监控。

---

## 1. BaseAdapter — 统一适配器契约

Python 层通过 `BaseAdapter` 抽象类定义所有 LLM Provider 的统一接口。每个 Provider（OpenAI、Anthropic、DeepSeek、Ollama）实现此接口。

**入参（AdapterRequest）**——强类型、单步、不可变：

| 字段 | 类型 | 来源 | 说明 |
|---|---|---|---|
| `system_prompt` | `str` | Go 层组装 | 已渲染完毕的完整 system prompt，含 workspace 文件快照和 Skill 文本 |
| `user_query` | `str` | Go 层传入 | 当前步骤的具体任务指令 |
| `history` | `list[Message]` | Go 从 SQLite 读取 | 该流的完整消息历史（user/assistant/tool 消息） |
| `tools` | `list[ToolDefinition]` | Go 层组装 | 当前 Agent 被允许使用的工具声明列表 |
| `model_name` | `str` | Go 从 Config 读取 | 物理模型 ID，如 `"gpt-4o"` |
| `temperature` | `float` | Go 从 Config 读取 | 0.0-2.0 |

**出参（AdapterResponse）**——标准化、不可变：

| 字段 | 类型 | 说明 |
|---|---|---|
| `content` | `str` | LLM 的文本推理输出（Thought） |
| `tool_calls` | `list[StandardToolCall]` | 标准化的工具调用列表（空 = 纯文本/最终答案） |
| `is_final` | `bool` | 是否最终答案（`finish_reason=stop` 且无 tool_calls） |
| `finish_reason` | `str` | 原始 stop reason |
| `usage` | `TokenUsage` | 本次 API 调用的 token 用量 |

`StandardToolCall` 标准化工具调用：`call_id`（LLM 生成的唯一 ID）、`tool_name`（工具名，可能带 `mcp:` 前缀）、`arguments`（已解析的参数字典）。

`TokenUsage`：`prompt_tokens`、`completion_tokens`、`total_tokens`。Go 层用于全局记账。

---

## 2. Jinja2 Prompt Compiler

Go 层已将 Skill 文本和 workspace 快照拼入 system_prompt，Python 端在最后一次 API 调用前用 Jinja2 做物理渲染。

**模板结构**（`templates/system_prompt.j2`）：

```
{{ system_prompt }}                    ← Go 传来的基础 prompt

<workspace_context>                    ← 可选：当前工作区文件快照
{{ workspace_context }}
</workspace_context>

<available_skills>                     ← 可选：Go 匹配的 Skill Markdown 文本
{{ skills }}
</available_skills>

<instructions>                         ← 固定行为指令
你是 AI Agent。通过工具调用来完成任务。
先输出 Thought，然后决定是否调用工具。
</instructions>
```

**编译器行为**：
- 每次调用从头渲染，不缓存模板、不累积上下文
- `compile_user_message()`：接收 Go 传来的 `history` 数组 + 当前 `user_query`，输出 LLM API 可直接使用的消息列表
- 对 `history` 中的每条 `Message`，复制 `role`/`content`/`tool_call_id`/`tool_calls`

---

## 3. MCP 客户端集成（官方 SDK）

Tenet 的 Python 端集成官方 `mcp` SDK（`ClientSession` + `StdioServerParameters`）。MCP Server 作为本地 Stdio 子进程运行。

### 3.1 架构

Python 进程内维护一个 `MCPClientManager`，管理多个 `MCPServerProcess`。每个 `MCPServerProcess` 对应 `tenet.yaml` 中配置的一个 MCP Server，通过 `stdio_client()` 拉起子进程，通过 `ClientSession` 做协议通信。

### 3.2 工具发现流程

1. **延迟初始化**：首次 `GenerateThought` 调用时，`MCPClientManager` 遍历所有 `enabled` 的 MCP Server 配置
2. **拉起子进程**：对每个 Server，通过 `stdio_client(command, args, env)` 启动本地子进程
3. **协议握手**：发送 `initialize` JSON-RPC 请求
4. **工具发现**：发送 `tools/list`，收到每个 Server 的原生工具 Schema 列表
5. **命名空间前缀**：每个工具名添加 `mcp:{server_name}:` 前缀（如 `mcp:github:create_issue`），防止与内置工具冲突
6. **缓存**：工具列表缓存在内存中（进程生命周期内不变，除非显式刷新）

### 3.3 Schema 翻译

MCP 的 `tools/list` 返回格式与每个 LLM 的原生工具格式不同，需翻译：

| MCP 字段 | OpenAI function 格式 | Anthropic tool 格式 |
|---|---|---|
| `tool.name` | `function.name` | `name` |
| `tool.description` | `function.description` | `description` |
| `tool.inputSchema` | `function.parameters` | `input_schema` |

翻译后的 MCP 工具与 Go 传来的静态工具合并，一起传给 LLM API。

### 3.4 代理调用

当 LLM 返回的 `tool_name` 带 `mcp:` 前缀时，Python 的 Tool Executor 识别此前缀，通过 `MCPServerProcess.call_tool(name, arguments)` 向对应子进程的 Stdio 发送 `tools/call` JSON-RPC 请求，捕获返回内容。

---

## 4. Provider 适配实现

### 4.1 OpenAI 兼容适配器（OpenAI / DeepSeek / Ollama）

三者共享 OpenAI Chat Completions API 格式。

**`translate_tools()` 行为**：
- 遍历所有 `ToolDefinition`，为每个生成 `{"type": "function", "function": {"name": ..., "description": ..., "parameters": ...}}`
- MCP 工具的 `parameters` 使用 `inputSchema` 字段，静态工具使用 `parameters_schema` 字段

**`generate()` 行为**（7 步）：
1. 通过 `PromptCompiler.compile()` 渲染 system prompt
2. 通过 `PromptCompiler.compile_user_message()` 将历史 + 当前 query 转为消息数组
3. 将 system prompt 作为第一条消息（`role: "system"`）
4. 调 `self.client.chat.completions.create()`，传入 model、messages、tools、temperature
5. 从 `response.choices[0].message` 提取 `content` 和 `tool_calls`
6. 对每个 `tool_call`：`call_id` = `tc.id`，`tool_name` = `tc.function.name`，`arguments` = `json.loads(tc.function.arguments)`
7. 返回 `AdapterResponse`

### 4.2 Anthropic 适配器

**`translate_tools()` 行为**：
- 生成 `{"name": ..., "description": ..., "input_schema": ...}` 格式

**`generate()` 行为**（7 步）：
1. 渲染 system prompt（Anthropic 的 system prompt 在 API 顶层参数，不在 messages 中）
2. 编译历史 + 当前 query 为 messages 数组（不含 system 角色）
3. 调 `self.client.messages.create()`，传入 model、system、messages、tools、temperature、max_tokens
4. 遍历 `response.content` 数组：`block.type == "text"` → 累积到 content；`block.type == "tool_use"` → 创建 `StandardToolCall(call_id=block.id, tool_name=block.name, arguments=block.input)`
5. `is_final` 判定：`stop_reason == "end_turn"` 且无 tool_calls
6. `finish_reason` = `response.stop_reason`
7. 返回 `AdapterResponse`

---

## 5. Pydantic v2 校验门控

**为什么需要**：LLM 返回的工具参数是未格式化的 JSON 字符串，可能缺字段、类型错误、包含幻觉参数。Python 端必须在向 OS 执行器下发工具调用前，对参数做物理拦截校验。

**校验流程**：
1. 从 `ToolDefinition.parameters_schema` 提取 JSON Schema（含 `properties` 和 `required` 列表）
2. 根据 Schema 动态创建 Pydantic v2 模型：每个 property 映射为 field，类型从 JSON type 映射为 Python type（string→str, integer→int, number→float, boolean→bool, array→list, object→dict）
3. `required` 中的字段设为必填（`...`），非必填字段设默认值 `None`
4. 用动态模型校验 `tool_call.arguments`
5. 校验通过 → 返回验证后的 `dict`
6. 校验失败 → 抛出 `InvalidArgumentError`

**校验失败后的链路**：
- `InvalidArgumentError` 被 gRPC handler 捕获 → 返回 `status.INVALID_ARGUMENT`
- Go 层收到 `INVALID_ARGUMENT` → 不重试（重试不会改变 LLM 的输出格式）
- Go 记录 `AgentFailed` 事件
- Workflow 可选择：用「参数格式错误，请修正」作为新消息重新调 `GenerateThought`

---

## 6. Provider 注册与路由

系统启动时，`ProviderRegistry` 从 `tenet.yaml` 的 `llm_providers` 段读取配置，实例化对应的 Adapter。

**路由规则**（基于配置中的显式 `adapter` 字段，不使用 model_name 字符串匹配）：

`llm_providers` 配置格式：
```yaml
llm_providers:
  - name: "openai-gpt4o"
    adapter: "openai"                    # ← 显式指定适配器类型
    model_name: "gpt-4o"
    api_key: "env:OPENAI_API_KEY"
  - name: "anthropic-claude"
    adapter: "anthropic"
    model_name: "claude-sonnet-4"
    api_key: "env:ANTHROPIC_API_KEY"
  - name: "deepseek-v3"
    adapter: "openai_compatible"         # 通用 OpenAI 兼容接口
    model_name: "deepseek-chat"
    base_url: "https://api.deepseek.com/v1"
```

**路由逻辑**：
1. Go 层传入 `model_name`（如 `"gpt-4o"`）
2. Python 在 `ProviderRegistry` 中查找 `model_name` 匹配的配置项
3. 读取该配置项的 `adapter` 字段 → 选择对应的 Adapter 实现
4. 找不到匹配 → 返回 `INVALID_ARGUMENT` 错误

**支持的 adapter 类型**：`openai`、`anthropic`、`openai_compatible`（需配置 base_url）、`ollama`（本地）。

这种配置驱动的方式避免了 `if "gpt" in model_name` 类字符串匹配的误判（如 `gpt2-finetuned` 不会错误路由到 OpenAI Adapter）。

---

## 7. Stateless 契约总结

| 操作 | Go 层职责 | Python 层职责 |
|---|---|---|
| 历史管理 | 维护完整 `messages` 数组，每次传入 | 不做修改，直接传给 LLM API |
| 窗口压缩 | 监控 token 消耗，超过阈值调 summarizer 压缩 | 不感知窗口大小 |
| Skill 匹配 | Phase 1 扫描、审计、语义匹配，拼入 system_prompt | Jinja2 拼入 |
| MCP 工具发现 | 接收 `discovered_tools` → 持久化为 `ToolsDiscovered` 事件 → 下一轮合并到 `request.tools` | 拉起子进程、`tools/list`、合并到 Response.discovered_tools |
| MCP 工具调用 | 通过 `ExecuteTool(mcp:name:tool)` 发起 | Stdio JSON-RPC 代理调用 |
| Token 记账 | 接收 `TokenUsage`，Guard Pattern 防重复落盘 | 返回准确 token 数 |
| 参数校验 | 不做 | Pydantic v2 动态校验，失败 → `INVALID_ARGUMENT` |
