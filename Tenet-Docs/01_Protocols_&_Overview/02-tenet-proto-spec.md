# Tenet Physical Communication Contract

> gRPC / Protobuf 详细契约定义
>
> 文档状态：DRAFT · 版本：v1.0.0
>
> Go（Orchestrator）与 Python（Worker）之间的唯一接口契约。
> 所有跨进程调用必须完全对齐本协议。任何一端内部重构，只要 proto 不变，另一端不受影响。

---

## 1. Protocol Design Principles

### 1.1 Version

- 采用 Protocol Buffers v3（proto3）语法。
- 命名空间：`tenet.v1`。包路径完全隔离，禁止跨版本引用。
- proto 源文件位置：`proto/tenet/v1/tenet.proto`。

### 1.2 gRPC Communication Model

Tenet 使用**双服务模型**——Go 和 Python 各自运行一个 gRPC Server：

| 服务 | Server 运行在 | Client | 职责 |
|---|---|---|---|
| `TenetOrchestrator` | Go 层 | Python 层 | 接收 Python 注册、接收 Agent 事件推送 |
| `TenetWorker` | Python 层 | Go 层 | 接收 Go 的 Agent 执行请求，流式回传执行过程 |

```
Go 层 :50051                    Python 层 :50052
┌─────────────────┐             ┌─────────────────┐
│ TenetOrchestrator│◄──RegisterAgent──│ gRPC Client      │
│ (gRPC Server)   │◄──PublishEvent───│                  │
│                 │             │                 │
│ gRPC Client     │──ExecuteAgent──►│ TenetWorker      │
│                 │  stream events  │ (gRPC Server)   │
└─────────────────┘             └─────────────────┘
```

### 1.3 Streaming Mode

- **Unary RPC**：用于控制流——注册、复杂度分析等一次性请求-响应场景。
- **Server Streaming RPC**：用于 `ExecuteAgent`——Python 在执行 Agent Loop 过程中，通过 stream 持续向 Go 推送 `AgentEvent`。Go 不逐事件发送 ACK——gRPC 连接本身的存在即表示 Go 存活且正在接收。若 Go 崩溃，连接断开，Python 自然停止执行。

### 1.4 Naming Conventions

- Service 名：`Tenet` + 职责（`Orchestrator` / `Worker`）。
- RPC 名：动词 + 名词（`ExecuteAgent`、`RegisterAgent`、`PublishEvent`）。
- Message 名：名词 + 方向后缀（`Request` / `Response` / `Event`）。
- 所有字段使用 `snake_case`。

---

## 2. Service: TenetOrchestrator（Go 层 Server）

Go 层在 `:50051` 监听。Python 层启动后主动连接此端口。

### 2.1 RPC Matrix

| RPC 方法 | 调用方向 | 传输模式 | 职责 |
|---|---|---|---|
| `RegisterAgent` | Python → Go | Unary | Python 启动后注册自身：宣告监听的端口、支持的 Agent 角色列表 |
| `PublishEvent` | Python → Go | Unary | Python 向 Go 推送一个 Agent 执行事件（备选路径，当不使用 ExecuteAgent stream 时） |

### 2.2 RegisterAgent

**RegisterAgentRequest**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `agent_id` | `string` | 1 | Python 实例的唯一标识（进程 ID + 启动时间戳） |
| `listen_port` | `int32` | 2 | Python 的 TenetWorker gRPC Server 监听端口 |
| `supported_roles` | `repeated string` | 3 | 该 Python 实例支持的 Agent 角色列表（如 `["coder", "reviewer", "lead"]`） |
| `max_concurrency` | `int32` | 4 | 该实例最大并发 Agent 执行数 |

**RegisterAgentResponse**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `accepted` | `bool` | 1 | Go 是否接受该 Worker 注册 |
| `orchestrator_id` | `string` | 2 | Go 层实例标识（用于日志关联） |
| `message` | `string` | 3 | 拒绝原因（当 accepted=false 时） |

### 2.3 PublishEvent

备选事件推送路径。正常情况下 Agent 事件通过 `ExecuteAgent` 的 stream 回传。此 RPC 用于 Python 需要独立于 Agent Loop 向 Go 推送事件时（如心跳、系统告警）。

**PublishEventRequest**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `session_id` | `string` | 1 | 会话标识 |
| `event` | `AgentEvent` | 2 | 事件载荷（复用同一消息体） |

**PublishEventResponse**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `ack` | `bool` | 1 | 事件已接收 |

---

## 3. Service: TenetWorker（Python 层 Server）

Python 层在 `RegisterAgentRequest.listen_port` 监听。Go 层通过此服务发起 Agent 执行。

### 3.1 RPC Matrix

| RPC 方法 | 调用方向 | 传输模式 | 职责 |
|---|---|---|---|
| `ExecuteAgent` | Go → Python | **Server Streaming** | Go 请求 Python 执行一个完整的 Agent Loop。Python 通过 stream 持续回传 AgentEvent |

### 3.2 ExecuteAgent

这是 Tenet 中最关键的 RPC。Go 层一次性发送完整的 Agent 执行请求，Python 层在内部运行 ReAct 循环，将每一步的思考、工具调用、结果通过 stream 持续推回 Go 层。stream 关闭 = Agent 执行结束。

**ExecuteAgentRequest**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `session_id` | `string` | 1 | 会话标识——用于 workspace 目录定位和锁命名 |
| `task_id` | `string` | 2 | 所属 Task 的 stream_id（如 `"task:a1b2c3"`） |
| `agent_name` | `string` | 3 | Agent 名称（如 `"profiler"`、`"coder"`），用于 Agent Stream 命名和日志 |
| `agent_role` | `string` | 4 | Agent 角色（如 `"Lead"`、`"Coder"`、`"Validator"`），决定 system prompt 和行为约束 |
| `system_prompt` | `string` | 5 | Go 层组装好的完整 system prompt（含角色定义、工具描述、workspace 上下文） |
| `user_query` | `string` | 6 | 本次执行的具体任务指令 |
| `history` | `repeated Message` | 7 | 对话历史（user/assistant/tool 消息序列） |
| `tools` | `repeated ToolDefinition` | 8 | 本 Agent 可用的工具列表（名称 + 描述 + JSON Schema） |
| `config` | `AgentConfig` | 9 | 执行参数（model、max_steps、temperature、收敛阈值） |
| `fencing_token` | `int64` | 10 | 当前 Session Lock 的 Fencing Token——Python 每次文件写入前必须校验 |

**AgentEvent（stream 中的每条消息）**

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `session_id` | `string` | 1 | 会话标识 |
| `task_id` | `string` | 2 | 所属 Task stream_id |
| `agent_name` | `string` | 3 | 产生此事件的 Agent 名称 |
| `seq` | `int64` | 4 | Agent 流内序号（自增，从 1 开始） |
| `type` | `AgentEventType` | 5 | 事件类型枚举 |
| `timestamp` | `google.protobuf.Timestamp` | 6 | 事件产生时间 |
| `payload` | `oneof` | 7 | 类型相关的载荷（见下方各类型消息体） |

**AgentEventType 枚举**

| 值 | 名称 | 含义 | 携带载荷 |
|---|---|---|---|
| 0 | `UNSPECIFIED` | 未指定（proto3 要求） | — |
| 1 | `AGENT_STARTED` | Agent Loop 开始执行 | `AgentStartedPayload` |
| 2 | `THOUGHT_GENERATED` | LLM 输出的推理文本（支持增量流式） | `ThoughtPayload` |
| 3 | `TOOL_CALLED` | Agent 决定调用一个工具 | `ToolCallPayload` |
| 4 | `TOOL_RESULTED` | 工具执行完成 | `ToolResultPayload` |
| 5 | `TOKEN_USED` | 本次 LLM 调用的 token 用量 | `TokenUsagePayload` |
| 6 | `AGENT_PAUSED` | Agent 进入等待状态（如等待 HITL 审批） | `AgentPausedPayload` |
| 7 | `AGENT_COMPLETED` | Agent 成功完成 | `AgentCompletedPayload` |
| 8 | `AGENT_FAILED` | Agent 执行失败 | `AgentFailedPayload` |

**oneof payload 各类型**

`AgentStartedPayload`：

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `agent_name` | `string` | 1 | — |
| `role` | `string` | 2 | — |
| `model` | `string` | 3 | 使用的 LLM 模型 |

`ThoughtPayload`：

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `content` | `string` | 1 | 推理文本（可能是增量片段） |
| `is_partial` | `bool` | 2 | 是否为流式增量片段（true = 后续还有，false = 本段完成） |

`ToolCallPayload`：

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `call_id` | `string` | 1 | 唯一调用 ID（LLM 生成，用于关联 ToolResult） |
| `tool_name` | `string` | 2 | 工具名称 |
| `arguments` | `string` | 3 | JSON 序列化的工具参数 |

`ToolResultPayload`：

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `call_id` | `string` | 1 | 对应的 ToolCall ID |
| `tool_name` | `string` | 2 | 工具名称 |
| `output` | `string` | 3 | 工具 stdout / 返回文本 |
| `is_error` | `bool` | 4 | 执行是否报错 |
| `duration_ms` | `int64` | 5 | 工具执行耗时（毫秒） |

`TokenUsagePayload`：

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `model` | `string` | 1 | LLM 模型名 |
| `prompt_tokens` | `int32` | 2 | 输入 token 数 |
| `completion_tokens` | `int32` | 3 | 输出 token 数 |
| `cost_usd` | `double` | 4 | 预估成本（美元） |

`AgentCompletedPayload`：

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `final_answer` | `string` | 1 | Agent 的最终答案文本 |
| `key_findings` | `repeated string` | 2 | 3-5 条带数据的结论摘要 |
| `artifacts` | `repeated string` | 3 | 产生的文件路径列表（相对于 workspace） |
| `total_tokens` | `int32` | 4 | 本次执行的累计 token 数 |
| `total_cost_usd` | `double` | 5 | 本次执行的累计成本 |

`AgentFailedPayload`：

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `error_code` | `string` | 1 | 错误码 |
| `error_message` | `string` | 2 | 错误描述 |
| `failed_at_step` | `int64` | 3 | 失败时的 loop 步数 |
| `retryable` | `bool` | 4 | 是否可重试 |

---

## 4. Shared Message Types

### 4.1 Message（对话历史条目）

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `role` | `string` | 1 | `"user"` / `"assistant"` / `"tool"` |
| `content` | `string` | 2 | 消息文本（或 tool result 文本） |
| `tool_call_id` | `string` | 3 | 当 role="tool" 时，关联的 call_id |
| `tool_calls` | `repeated ToolCallPayload` | 4 | 当 role="assistant" 且调用了工具时 |

### 4.2 ToolDefinition（工具注册描述）

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `name` | `string` | 1 | 工具名称（LLM 通过此名调用） |
| `description` | `string` | 2 | 工具功能描述（注入 system prompt） |
| `parameters_schema` | `string` | 3 | JSON Schema 字符串（定义参数结构） |

### 4.3 AgentConfig（Agent 执行参数）

| 字段 | 类型 | Tag | 说明 |
|---|---|---|---|
| `model` | `string` | 1 | LLM 模型名 |
| `max_steps` | `int32` | 2 | 最大 ReAct 循环步数（默认 50） |
| `temperature` | `double` | 3 | LLM 温度参数 |
| `convergence_threshold` | `int32` | 4 | 连续无工具调用的收敛阈值（默认 3） |
| `token_budget` | `int32` | 5 | 本 Agent 的 token 预算上限（0 = 无限制） |

---

## 5. Code Generation Specification

### 5.1 Go

- **输出目录**：`go/internal/gateway/gen/tenet/v1/`
- **编译参数**：`paths=source_relative`
- **插件**：`protoc-gen-go`（消息体）+ `protoc-gen-go-grpc`（服务 stub）
- **生成的 Go package**：`tenetv1`
- **生成的 Go module 路径**：在 `go.mod` 中由项目根路径推导

```
protoc \
  --proto_path=proto \
  --go_out=go/internal/gateway/gen \
  --go_opt=paths=source_relative \
  --go-grpc_out=go/internal/gateway/gen \
  --go-grpc_opt=paths=source_relative \
  proto/tenet/v1/tenet.proto
```

### 5.2 Python

- **输出目录**：`python/tenet/proto/`
- **编译工具**：`grpc_tools.protoc`
- **生成文件**：`tenet_pb2.py`（消息体）+ `tenet_pb2_grpc.py`（服务 stub）

```
python -m grpc_tools.protoc \
  --proto_path=proto \
  --python_out=python/tenet/proto \
  --grpc_python_out=python/tenet/proto \
  proto/tenet/v1/tenet.proto
```

- **Import 修复约束**：生成的 `tenet_pb2_grpc.py` 中 `import tenet_pb2` 可能因 Python 路径问题失败。AI 生成代码时必须添加 `sys.path` 修正或使用相对 import。

---

## 6. gRPC Error Handling Convention

| gRPC Status Code | 含义 | Go 层行为 |
|---|---|---|
| `OK` | 正常 | — |
| `DEADLINE_EXCEEDED` | Agent 执行超时 | 重试（可配置次数） |
| `UNAVAILABLE` | Python 层不可达 | 断路器逻辑 |
| `INTERNAL` | Python 内部错误 | 记录错误事件，根据 AgentFailedPayload.retryable 决定 |
| `INVALID_ARGUMENT` | 请求参数非法 | 不重试，直接标记 TaskFailed |
| `RESOURCE_EXHAUSTED` | Python 并发 Agent 数满 | 排队等待或路由到其他 Worker |
