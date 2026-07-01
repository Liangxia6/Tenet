# Python gRPC Client

> gRPC 客户端接收器与任务解压

---

## 1. Client Initialization

（待展开：连接 Go 层 gRPC Server、RegisterAgent 注册自身、连接保活与断线重连）

## 2. ExecuteAgent Request Handler

（待展开：接收 Go 层的 ExecuteAgent 请求——解析 task_id、agent_config、messages、tools。启动 Agent Loop。将请求参数解压为 Python 层内部结构）

## 3. Event Stream Push

（待展开：Agent Loop 执行过程中通过 gRPC stream 实时回传 AgentEvent。事件类型映射（Python 内部事件 → proto 枚举）。流式推送的背压处理）

## 4. ACK Handling

（待展开：等待 Go 层对每个事件的 ACK（State 通道落盘确认）。ACK 超时处理。不收到 ACK 不继续执行下一步——保证事件不丢）

## 5. Error Handling

（待展开：gRPC 连接断开的恢复策略、Python 进程异常退出时的清理、ExecuteAgent 超时处理）

## 6. Proto Stub Generation

（待展开：从 proto/tenet/v1/tenet.proto 生成 Python stub 的流程、依赖管理、版本对齐策略）
