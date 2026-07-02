# Go gRPC Gateway

> 双 Server 模型 · Middleware 链（超时/重试/断路器）· Worker Registry
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 1. 双 Server 模型

Go :50051 — TenetOrchestrator Server（接收 Python RegisterAgent）。Python :50052 — TenetWorker Server（接收 Go GenerateThought/ExecuteTool）。

Go 的 gRPC Gateway 包含 Server 端 + Client 端：Server 端处理 Python 注册，Client 端通过 Middleware 链调用 Python。

## 2. Server 端：RegisterAgent

Python 启动后调 `RegisterAgent(agent_id, listen_port, max_concurrency)` → Go 存入 WorkerRegistry → 返回 accepted。

## 3. Client 端：Middleware 链

每次 GenerateThought/ExecuteTool 调用经三层 Middleware：

### Timeout
- 控制面 RegisterAgent：60s
- 执行面 GenerateThought/ExecuteTool：300s
- `context.WithTimeout` 包装

### Retry
- 指数退避：base(1s) × 2^attempt
- 最多 3 次
- 可重试：Unavailable、DeadlineExceeded、ResourceExhausted
- 不可重试：InvalidArgument、PermissionDenied、Internal

### Circuit Breaker
- 状态机：CLOSED → (连续失败 5 次) → OPEN → (冷却 30s) → HALF_OPEN → (成功→CLOSED, 失败→OPEN)
- OPEN 状态：所有请求立即返回错误，不经网络

## 4. Worker Registry

维护已注册 Python Worker 列表。负载均衡：选择 `ActiveCalls` 最少且 < MaxConcurrency 的 Worker。GenerateThought/ExecuteTool 调用前后 `AtomicAddInt32(&w.ActiveCalls, ±1)`。

## 5. 错误码处理

| gRPC Status | Middleware | 结果 |
|---|---|---|
| OK | 通过 | 返回 |
| DEADLINE_EXCEEDED | Retry→断路器计数 | 重试耗尽→TaskFailed |
| UNAVAILABLE | Retry→断路器计数 | 断路器 OPEN→快速失败 |
| INTERNAL | 不重试 | TaskFailed |
| INVALID_ARGUMENT | 不重试 | TaskFailed |
| PERMISSION_DENIED | 不重试 | 脑裂→TaskFailed |
| RESOURCE_EXHAUSTED | Retry | 排队等 Worker |
