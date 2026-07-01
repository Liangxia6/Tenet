# Go Event Router

> State/Stream 物理双通道分流、Event Router 内部设计

---

## 1. The Dual-Channel Problem

（待展开：实时回传事件后既要持久化又要在 UI 实时可见——同一路径会导致 SQLite 写延迟阻塞 UI。物理分叉的必要性）

## 2. Event Router Design

（待展开：Router 的核心逻辑——收到事件后同步分叉到 State 和 Stream 两个子组件。分叉在同一个调用栈内完成，但子组件的处理在独立 goroutine 中）

## 3. State Channel

（待展开：同步阻塞语义——通过 WriteDaemon channel 写 SQLite → 等待 resultCh → ACK 回 Router → Router ACK 给 Python。保证事件不丢）

## 4. Stream Channel

（待展开：异步非阻塞语义——Redis PUBLISH → fire-and-forget。独立 goroutine。Redis 不可用时降级（Warn 日志），不影响 State 通道）

## 5. ACK Flow

（待展开：Python → gRPC → Event Router → State Channel → SQLite → ACK 链。State 通道未确认前 Python 层等待，保证执行一致性）

## 6. Channel Interface Summary

（待展开：Event Router 对上游（gRPC Gateway）和下游（State/Stream Channel）的接口契约）
