# Go Workflow Context

> WorkflowContext：Decide/Record、Immediate Commit、回放指针逻辑

---

## 1. WorkflowContext Structure

（待展开：完整字段定义——store/streamID/parentID/history/historyPos/pendingRecords/version。三种初始化模式：首次执行/回放/Fork）

## 2. Decide: The Decision Point

（待展开：Decide 的完整逻辑——回放路径的类型比对（确定性检查）、首次执行路径的 立即调用+落盘+提交。决策点类型枚举（ExecuteAgent/Sleep/Now/Random/SideEffect））

## 3. Record: Deterministic State Markers

（待展开：Record 的延迟批量提交策略——缓冲阈值、flushRecords 时机、与 Decide 的不同语义。崩溃后回放重建的幂等性保证）

## 4. Replay Pointer Logic

（待展开：historyPos 的移动规则、确定性检查失败时的 panic 语义、版本标记的指针跳过）

## 5. Commit Lifecycle

（待展开：Workflow 执行完毕后的 Commit 流程——flush 剩余 Record 事件、Decide 事件无需额外 flush、Commit 失败的处理）

## 6. Workspace Backup & Restore

（待展开：ExecuteAgent/SideEffect 类 Decide 前的自动备份——备份粒度、备份目录结构、失败后的恢复流程、成功后的清理）

## 7. Context Method Summary

（待展开：Decide/Record/GetVersion/Async/Commit 五个方法的签名、语义、调用约束汇总表）
