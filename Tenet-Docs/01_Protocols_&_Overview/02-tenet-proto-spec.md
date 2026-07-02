# Tenet Physical Communication Contract

> gRPC / Protobuf 详细契约定义
>
> 文档状态：DRAFT · 版本：v1.1.0（Design B：Go Deterministic Loop）
>
> **架构红线**：Go 层驱动所有微观循环。Python 层彻底无状态——不持有 Agent Loop、不维护历史、不自主决策。每次 RPC 是单次原子操作：调一次 LLM 或执行一次工具。

---

## 1. Protocol Design Principles

### 1.1 Version

- Protocol Buffers v3（proto3）。
- 命名空间：`tenet.v1`。
- proto 源文件：`proto/tenet/v1/tenet.proto`。

### 1.2 gRPC Communication Model

双服务模型。Go 和 Python 各自运行一个 gRPC Server：

| 服务 | Server 运行在 | Client | 职责 |
|---|---|---|---|
| `TenetOrchestrator` | Go 层 | Python 层 | 接收 Python 注册 |
| `TenetWorker` | Python 层 | Go 层 | 单次 LLM 调用、单次工具执行 |

**所有 RPC 均为 Unary（同步阻塞）**——Go 发起调用后阻塞等待 Python 返回。没有 Streaming——Steps 之间的控制权完全在 Go 层的 `for` 循环中。

### 1.3 The Stateless Contract

Python 层不持有任何跨 RPC 的状态。每次 `GenerateThought` 或 `ExecuteTool` 调用是独立的、原子的、无记忆的。Go 层通过 `Decide` 的 Immediate Commit 在每次 RPC 返回后立即落盘——崩溃后从事件流精确恢复到最后一次成功返回的 RPC。

---

## 2. Service: TenetOrchestrator（Go 层 Server）

### 2.1 RPC Matrix

| RPC 方法 | 调用方向 | 传输模式 | 职责 |
|---|---|---|---|
| `RegisterAgent` | Python → Go | Unary | Python 启动后注册自身 |

### 2.2 RegisterAgent

**RegisterAgentRequest**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `agent_id` | `string` | 1 | Python 实例唯一标识 |
| `listen_port` | `int32` | 2 | Python 的 TenetWorker gRPC Server 监听端口 |
| `max_concurrency` | `int32` | 3 | 该实例最大并发 LLM 调用数 |

**RegisterAgentResponse**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `accepted` | `bool` | 1 | Go 是否接受注册 |
| `orchestrator_id` | `string` | 2 | Go 实例标识 |
| `message` | `string` | 3 | 拒绝原因 |

---

## 3. Service: TenetWorker（Python 层 Server）

Python 层暴露三个无状态 RPC。Go 层高频调用。

### 3.1 RPC Matrix

| RPC 方法 | 调用方向 | 传输模式 | 职责 |
|---|---|---|---|
| `GenerateThought` | Go → Python | Unary | 单次 LLM 调用：发送 messages + tools，返回 Thought 或 ToolCalls |
| `ExecuteTool` | Go → Python | Unary | 单次工具执行：发送 tool_name + arguments，返回 stdout/stderr |
| `HealthCheck` | Go → Python | Unary | 健康探测：Go 的断路器用此 RPC 检测 Python 是否存活 |

### 3.2 GenerateThought

Go 层在 ReAct 循环的每一步调用此 RPC。Python 拼接 messages、调 LLM API、解析响应、返回 Thought 文本 + 可能的 ToolCalls。Python 不做任何循环——一次调用，一次返回。

**GenerateThoughtRequest**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `session_id` | `string` | 1 | 会话标识 |
| `task_id` | `string` | 2 | 所属 Task stream_id |
| `model` | `string` | 3 | LLM 模型名 |
| `temperature` | `double` | 4 | LLM 温度 |
| `system_prompt` | `string` | 5 | Go 层组装好的完整 system prompt |
| `messages` | `repeated Message` | 6 | 对话历史（Go 层维护，每次调用传入完整历史） |
| `tools` | `repeated ToolDefinition` | 7 | 本步可用的工具列表 |

**GenerateThoughtResponse**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `thought` | `string` | 1 | LLM 输出的推理文本（非流式，完整返回） |
| `tool_calls` | `repeated ToolCall` | 2 | LLM 决定调用的工具列表（空 = 纯文本回复/最终答案） |
| `is_final` | `bool` | 3 | 是否为最终答案（LLM 不再需要工具调用） |
| `finish_reason` | `string` | 4 | LLM 返回的 finish_reason：stop / tool_calls / length |
| `usage` | `TokenUsage` | 5 | 本次 LLM 调用的 token 用量 |
| `discovered_tools` | `repeated ToolDefinition` | 6 | Python 发现的新 MCP 工具列表（首次调用或 MCP 服务重启后）。Go 收到后写入 `ToolsDiscovered` 事件到 event_log，后续 `GenerateThoughtRequest.tools` 自动合并这些工具 |

### 3.3 ExecuteTool

Go 层通过此 RPC 指挥 Python 在宿主机上物理执行单个工具。Python 校验 Fencing Token、执行工具、捕获 stdout/stderr、返回结果。

**ExecuteToolRequest**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `session_id` | `string` | 1 | 会话标识（用于 workspace 路径定位） |
| `fencing_token` | `int64` | 2 | 当前 Session Lock 的 Fencing Token——Python 执行前必须校验 |
| `tool_name` | `string` | 3 | 工具名称 |
| `arguments` | `string` | 4 | JSON 序列化的工具参数 |

**ExecuteToolResponse**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `stdout` | `string` | 1 | 工具标准输出 |
| `stderr` | `string` | 2 | 工具标准错误 |
| `exit_code` | `int32` | 3 | 进程退出码 |
| `is_error` | `bool` | 4 | 执行是否报错 |
| `duration_ms` | `int64` | 5 | 工具执行耗时 |

### 3.4 HealthCheck

Go 层定期（默认 10s）调用此 RPC 探测 Python 进程存活。返回值用于驱动断路器状态机和 Worker Registry 健康状态。

**HealthCheckRequest**（空消息，无字段）

**HealthCheckResponse**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `status` | `string` | 1 | `"SERVING"` / `"NOT_SERVING"` |
| `worker_count` | `int32` | 2 | 该 Python 实例当前活跃的 Worker 数 |
| `uptime_seconds` | `int64` | 3 | Python 进程启动以来的秒数 |

---

## 4. Shared Message Types

### 4.1 Message（对话历史条目）

Go 层维护完整的消息历史，每次 `GenerateThought` 调用时传入。

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `role` | `string` | 1 | `"system"` / `"user"` / `"assistant"` / `"tool"` |
| `content` | `string` | 2 | 消息文本 |
| `tool_call_id` | `string` | 3 | 当 role="tool" 时，关联的 call_id |
| `tool_calls` | `repeated ToolCall` | 4 | 当 role="assistant" 且调用了工具时 |

### 4.2 ToolCall（工具调用）

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `call_id` | `string` | 1 | 唯一调用 ID（LLM 生成） |
| `tool_name` | `string` | 2 | 工具名称 |
| `arguments` | `string` | 3 | JSON 序列化的工具参数 |

### 4.3 ToolDefinition（工具注册描述）

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `name` | `string` | 1 | 工具名称 |
| `description` | `string` | 2 | 工具功能描述 |
| `parameters_schema` | `string` | 3 | JSON Schema 字符串 |

### 4.4 TokenUsage

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `prompt_tokens` | `int32` | 1 | 输入 token 数 |
| `completion_tokens` | `int32` | 2 | 输出 token 数 |
| `total_tokens` | `int32` | 3 | 总计 |
| `cost_usd` | `double` | 4 | 预估成本（美元） |

---

## 5. Code Generation Specification

### 5.1 Go

```
输出目录：go/internal/gateway/gen/tenet/v1/
编译参数：paths=source_relative
插件：    protoc-gen-go + protoc-gen-go-grpc
```

### 5.2 Python

```
输出目录：python/tenet/proto/
编译工具：grpc_tools.protoc
生成文件：tenet_pb2.py + tenet_pb2_grpc.py
Import 修复：生成的 import tenet_pb2 需手动修正为 from . import tenet_pb2
```

---

## 6. gRPC Error Handling Convention

| gRPC Status Code | 含义 | Go 层行为 |
|---|---|---|
| `OK` | 正常 | — |
| `DEADLINE_EXCEEDED` | 调用超时 | 重试（可配置次数） |
| `UNAVAILABLE` | Python 层不可达 | 断路器逻辑 |
| `INTERNAL` | Python 内部错误 | 记录错误事件，根据 retryable 决定 |
| `INVALID_ARGUMENT` | 请求参数非法 | 不重试，直接标记失败 |
| `PERMISSION_DENIED` | Fencing Token 校验失败 | 不重试，标记为脑裂冲突 |
