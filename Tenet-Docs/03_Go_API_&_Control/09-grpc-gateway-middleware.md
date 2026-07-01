# gRPC Gateway & Middleware

> gRPC Gateway、超时/重试/断路器中间件

---

## 1. Gateway Architecture

（待展开：gRPC Server 在 Go 层的定位——Python 层的唯一入口、事件接收的第一站。proto 服务定义与 Go 实现的关系）

## 2. Service Implementation

（待展开：RegisterAgent——Python 层启动后注册自身。ExecuteAgent——Go 向 Python 发起 Agent 执行请求。PublishAgentEvent——Python 向 Go 回传 Agent 事件）

## 3. Middleware Pipeline

（待展开：三层中间件链——请求进入 → 超时检查 → 重试判定 → 断路器 → 实际处理。中间件的顺序和可组合性）

## 4. Timeout Middleware

（待展开：context deadline 传递、超时配置（默认 300s，可配）、超时后的清理逻辑）

## 5. Retry Middleware

（待展开：可重试错误类型枚举、重试策略（线性/指数退避）、最大重试次数。幂等性要求——哪些 RPC 可以安全重试）

## 6. Circuit Breaker

（待展开：断路器状态机——CLOSED → (连续失败 N 次) → OPEN → (等待 M 秒) → HALF_OPEN → (成功 → CLOSED | 失败 → OPEN)。配置参数、监控指标）
