# Python gRPC Client

> gRPC 客户端：无状态单步服务
>
> 文档状态：DRAFT · 版本：v1.1.0（Design B：Stateless Python Worker）
>
> **架构红线**：Python 层是纯粹的无状态服务。不持有 Agent Loop、不维护对话历史、不自主决策。被调起 → 执行一次操作 → 返回 → 释放所有内存。

---

## 1. Service Implementation

Python 层作为 `TenetWorker` gRPC Server，暴露三个 Unary RPC：

```python
class TenetWorkerServicer(tenet_pb2_grpc.TenetWorkerServicer):
    
    async def GenerateThought(self, request, context):
        """单次 LLM 调用。拼接 messages → 调 LLM API → 返回 Thought。"""
        ...
    
    async def ExecuteTool(self, request, context):
        """单次工具执行。校验 Fencing Token → 执行 → 返回 stdout/stderr。"""
        ...
    
    async def HealthCheck(self, request, context):
        """健康探测。返回服务状态，供 Go 层断路器使用。"""
        ...
```

## 2. GenerateThought — Stateless LLM Gateway

Python 不维护任何跨调用的状态。每次 `GenerateThought` 从零开始：

1. 接收 Go 传来的完整 `messages` 数组（含 system prompt + 历史）
2. 通过 LLM Adapter 调一次 LLM API
3. 解析响应中的 `thought` 文本和可能的 `tool_calls`
4. 提取 `finish_reason` 和 `token_usage`
5. 构造 `GenerateThoughtResponse` 返回
6. **释放所有内存，不保存任何变量**

没有任何 `while True`，没有任何 `self.history`，没有任何跨调用缓存。

## 3. ExecuteTool — Stateless Tool Executor

1. 校验 `fencing_token` 与 Redis 当前值一致
2. 根据 `tool_name` 找到已注册的工具函数
3. 执行工具（Shell 命令 / 文件操作 / HTTP 请求）
4. 捕获 stdout / stderr / exit_code
5. 返回 `ExecuteToolResponse`
6. **释放所有内存**

## 4. Fencing Token Validation

每次文件写入类操作前：

```python
async def ExecuteTool(self, request, context):
    # 校验 Fencing Token
    current_token = redis.get(f"session_fencing:{request.session_id}")
    if int(current_token) != request.fencing_token:
        context.abort(grpc.StatusCode.PERMISSION_DENIED, 
                       "Fencing token mismatch — lock was preempted")
    
    # 通过 → 执行工具
    result = await tool_registry.execute(request.tool_name, request.arguments)
    return ExecuteToolResponse(...)
```

## 5. Startup & Registration

```python
async def main():
    # 启动 TenetWorker gRPC Server
    server = grpc.aio.server()
    tenet_pb2_grpc.add_TenetWorkerServicer_to_server(TenetWorkerServicer(), server)
    server.add_insecure_port(f"[::]:{worker_port}")
    await server.start()
    
    # 向 Go 注册
    async with grpc.aio.insecure_channel(f"localhost:{orchestrator_port}") as channel:
        stub = tenet_pb2_grpc.TenetOrchestratorStub(channel)
        await stub.RegisterAgent(RegisterAgentRequest(
            agent_id=f"worker-{os.getpid()}",
            listen_port=worker_port,
            max_concurrency=10,
        ))
    
    await server.wait_for_termination()
```
