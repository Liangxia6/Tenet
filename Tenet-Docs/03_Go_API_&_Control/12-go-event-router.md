# Go Event Router

> State/Stream 双通道 · ACK 链路 · 降级矩阵 · EventChannel 接口
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 1. 双通道问题

Python 返回的结果既要写入 SQLite（持久化——崩溃安全），又要推送 Web UI（实时展示——用户体验）。如果共享同一调用链，SQLite 写延迟会阻塞实时推送。

**解决**：Event Router 物理分叉两条通道——State 通道同步阻塞（落盘才 ACK），Stream 通道异步非阻塞（fire-and-forget）。

---

## 2. Event Router 结构

```go
type EventRouter struct {
    state  *StateChannel   // 同步阻塞，保证落盘
    stream *StreamChannel  // 异步非阻塞
}

func (er *EventRouter) Route(event *InternalEvent) error {
    stateErr := er.state.Process(event)  // 同步等待落盘
    go er.stream.Process(event)          // 异步推送
    return stateErr                       // 仅 State 错误影响 ACK
}
```

## 3. State Channel

**处理流程**：
1. 通过 WriteDaemon.Submit() 同步写入 event_log（INSERT + COMMIT）
2. ProjectionEngine.Apply(event)——更新对应 Projection 内存状态
3. 检查快照触发条件
4. 返回 nil → Router ACK → Python

阻塞语义：Process 返回 nil 前，事件已提交——物理上不可能丢失。

## 4. Stream Channel

构造 JSON 消息 → `PUBLISH sse:{stream_id}` → fire-and-forget。独立 goroutine 异步执行。Redis 不可用 → Warn 日志。

## 5. ACK 完整链路

Python 返回 gRPC response → Event Router.Route → State Channel.Process 同步落盘 → Router 返回 nil → gRPC handler 返回 → Python 收到。Python 收到响应的瞬间，事件已在 SQLite 中提交。

## 6. 降级矩阵

| 故障 | State | Stream |
|---|---|---|
| SQLite 写失败 | 返回 error → TaskFailed | 不受影响 |
| Redis 不可达 | 不受影响 | Warn + 跳过 |
| State 慢（磁盘） | 同步等待 | 不受影响 |

## 7. EventChannel 接口

```go
type EventChannel interface {
    Process(event *InternalEvent) error
}
```

State/Stream 各自实现。未来可添加 Webhook/S3 通道。
