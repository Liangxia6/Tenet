# Go Engine Scheduler

> 调度主循环 · Worker Pool · TimerService 最小堆 · ctx.Sleep 哨兵退出 · Graceful Shutdown
>
> 文档状态：DRAFT · 版本：v1.1.0

---

## 1. Scheduler 结构

Go 层的中央调度器。不执行 Workflow——只决定「何时、由哪个 Worker、执行哪个 Task」。

```go
type Scheduler struct {
    workflowQueue chan *TaskHandle       // buffered, capacity = queueSize（默认 100）
    workerPool    *WorkerPool
    timerService  *TimerService
    ctx           context.Context
    cancel        context.CancelFunc
    wg            sync.WaitGroup         // 等待所有 Worker 退出

    activeTasks   map[string]*TaskHandle // streamID → 正在执行的 Task
    mu            sync.RWMutex

    maxConcurrent int                    // 来自 config.Workflow.MaxConcurrentTasks
    queueSize     int                    // 来自 database.write_queue_size

    store         EventStore             // 用于 TimerService 写 TimerFired 事件
    config        *RuntimeConfig
}
```

---

## 2. 主循环 — 四通道 select

```go
func (s *Scheduler) Run() {
    for {
        select {
        case <-s.ctx.Done():
            s.gracefulShutdown()
            return

        case task := <-s.workflowQueue:
            s.dispatch(task)

        case fired := <-s.timerService.FireCh():
            s.handleTimerFired(fired)

        case result := <-s.workerPool.CompletionCh():
            s.handleCompletion(result)
        }
    }
}
```

| Channel | 生产者 | 触发条件 | 处理动作 |
|---|---|---|---|
| `ctx.Done()` | OS 信号 / cancel() | SIGINT/SIGTERM | Graceful Shutdown |
| `workflowQueue` | CLI / Timer / DAG 分解 | 新 Task 入队 | `dispatch(task)` |
| `timerService.FireCh()` | TimerService 后台 goroutine | 定时器到期 | 写 TimerFired 事件 → Task 重新入队 |
| `workerPool.CompletionCh()` | Worker goroutine | Workflow 完成/挂起 | 回收 Worker，处理结果 |

---

## 3. dispatch — 非阻塞分配

1. 调用 `workerPool.TryAcquire()` —— 通过 `select { case w := <-pool.workers: return w, true; default: return nil, false }` 非阻塞获取空闲 Worker
2. 有 Worker → `activeTasks[task.StreamID] = task` → 在独立 goroutine 中启动 `go worker.Run(task)`
3. 无 Worker → **任务留在队列中**（不从 channel 取出）——Scheduler 不做忙等，等待下一个 Worker 释放时通过 `CompletionCh` 触发重新调度

**为什么是「任务留在队列中」**：从 `workflowQueue` channel receive 后如果分配失败，不能简单地放回——那会丢失 FIFO 顺序。正确做法是 `TryAcquire` 在 channel receive 之前执行——但这会引入竞态。实际实现中，`dispatch` 在 `select` 内被调用：Worker 完成事件和 workflowQueue 事件在同一个 `select` 中竞争，Worker 完成时 Scheduler 先处理完成（回收 Worker），然后 select 回到顶部，workflowQueue 中的下一个 Task 自动被取出并分配。

---

## 4. Worker Pool

固定大小的 goroutine 池。Worker 数量 = `max_concurrent_tasks`。

### 4.1 WorkerPool 结构

```go
type WorkerPool struct {
    workers    chan *Worker        // buffered, cap = maxConcurrent。空闲 Worker Token 池
    completion chan *TaskResult    // Worker 完成通知，buffered, cap = maxConcurrent
    store      EventStore
    config     *RuntimeConfig
}
```

### 4.2 Worker 结构

```go
type Worker struct {
    id   int
    pool *WorkerPool
}
```

Worker 无独立状态——所有状态在 WorkflowContext 中。Worker 是纯执行器。

### 4.3 Worker.Run 流程

```go
func (w *Worker) Run(task *TaskHandle) {
    // 1. 创建 WorkflowContext
    ctx := NewWorkflowContext(w.pool.store, task.StreamID, task.Mode, 
                               task.ForkFromSeq, task.ParentID, task.Config)
    
    // 2. 从 Registry 取 Workflow 函数
    wf := task.Config.Registry.Get(task.WorkflowType)
    
    // 3. 执行 Workflow 函数
    result, err := wf(ctx, task)
    
    // 4. 检查哨兵错误
    if errors.Is(err, ErrWorkflowSuspended) {
        // 正常挂起——TimerStarted 已在 ctx.Sleep 内部落盘
        w.pool.workers <- w   // 放回空闲池
        return                // 不发送 CompletionCh（不是完成，是挂起）
    }
    
    // 5. 正常完成或失败
    if err != nil {
        ctx.Record("TaskFailed", TaskFailedPayload{Error: err.Error()})
    }
    ctx.Commit()  // flush 剩余 Record 事件
    
    // 6. 回收 + 通知
    w.pool.workers <- w
    w.pool.completion <- &TaskResult{
        StreamID: task.StreamID,
        Result:   result,
        Err:      err,
    }
}
```

### 4.4 启动与生命周期

系统启动时创建 `maxConcurrent` 个 Worker，全部放入 `workers` channel（空闲池）。Worker 在系统关闭时由 `wg.Wait()` 等待退出。Worker 本身不持有 goroutine——goroutine 在 `Run` 调用时由调用方（Scheduler）创建。

---

## 5. TimerService — 确定性定时器

### 5.1 数据结构

```go
type TimerService struct {
    timers    *MinHeap          // 按到期时间排序。堆元素：{TimerID, StreamID, FireAt, Duration}
    fireCh    chan *TimerFired  // buffered, cap = 100。通知 Scheduler
    mu        sync.Mutex
    ctx       context.Context
}
```

### 5.2 Add 操作

接收 `(timerID, duration, streamID)`：
1. `mu.Lock()`
2. 计算 `FireAt = time.Now().Add(duration)`
3. `timers.Push(TimerEntry{TimerID, StreamID, FireAt, Duration})`
4. `mu.Unlock()`

### 5.3 后台 goroutine 行为

1. `mu.Lock()`，检查堆是否为空
2. 空 → `mu.Unlock()` → 每秒 sleep 1s（被 `ctx.Done()` 中断）
3. 非空 → 取堆顶 `next`，计算 `waitDuration = next.FireAt - time.Now()`
4. `mu.Unlock()` → `select { case <-ctx.Done(): return; case <-time.After(waitDuration): }`
5. `mu.Lock()`，弹出所有 `FireAt <= time.Now()` 的条目
6. 对每个到期的条目 → `fireCh <- &TimerFired{TimerID, StreamID, FiredAt}`
7. `mu.Unlock()` → 回到步骤 1

### 5.4 Scheduler.handleTimerFired

1. 收到 `TimerFired` 事件
2. 通过 EventStore 写入 `TimerFired` 事件到 event_log：`stream_id = fired.StreamID`, `event_type = "TimerFired"`, `payload = {timer_id, fired_at}`
3. 从 `activeTasks` 查找对应的 `TaskHandle`
4. 将 `TaskHandle` 重新放入 `workflowQueue`

---

## 6. Graceful Shutdown

| 阶段 | 操作 | 超时 | 超时后行为 |
|---|---|---|---|
| **Drain** | 停止接收新 Task。CLI/gRPC 返回 "shutting down" | — | — |
| **Wait** | 不再从 workflowQueue 取新 Task。等待所有活跃 Worker 完成 | 30s | Warn 日志，强制取消 |
| **Flush** | Worker 的 ctx.Commit() 将 Record 事件落盘。WriteDaemon 消费完 writeCh 中剩余请求 | — | 剩余请求直接丢弃（已落盘的 Decide 不受影响，未完成的 Decide 重启后回放） |
| **Cleanup** | TimerService.Stop() → 关闭 SQLite 连接池 → 关闭 Redis 连接池 | — | — |

**超时后的安全性**：强制退出时，每个 `Decide` 内部已通过 Immediate Commit 落盘的事件不会丢失。未完成的 `Decide` 在重启后回放从最后一个落盘事件继续。

---

## 7. TaskHandle 结构

Scheduler 传递的调度单元：

| 字段 | 类型 | 说明 |
|---|---|---|
| `StreamID` | `string` | Task 流 ID |
| `ParentID` | `string` | Fork 来源（空 = 首次执行） |
| `Mode` | `ContextMode` | Execution / Replay / Fork |
| `ForkFromSeq` | `int` | Fork 点（仅 Fork 模式） |
| `WorkflowType` | `string` | "simple" / "react" / "dag" / "interactive" / "scientific" / "coding" |
| `SessionID` | `string` | 会话 ID（用于 workspace 路径和 Lock 命名） |
| `Query` | `string` | 用户原始指令 |
| `Workspace` | `string` | workspace 绝对路径 |
| `SystemPrompt` | `string` | Go 层组装的完整 system prompt |
| `Messages` | `[]Message` | 对话历史（Go 层维护） |
| `Tools` | `[]ToolDefinition` | 本 Agent 可用的工具列表 |
| `AgentRole` | `string` | Agent 角色名 |
| `Config` | `*RuntimeConfig` | 配置快照（Configuration Freeze） |
| `Subtasks` | `[]*SubTaskHandle` | 子任务列表（DAG 模式） |
