# Testing Strategy

> 单元/集成测试、确定性回放回归测试

---

## 1. Test Pyramid

（待展开：测试分层——单元测试（Go 各组件 / Python 各模块）→ 集成测试（gRPC 通信 / SQLite 写入 / Redis 锁）→ 端到端测试（完整 Task 执行 → 回放 → Fork 验证））

## 2. Go Unit Tests

（待展开：Event Store——Append/Read/GetLineage 的单元测试（SQLite in-memory）。WorkflowContext——Decide/Record/GetVersion 的 mock 测试。确定性检查的 panic 测试）

## 3. Python Unit Tests

（待展开：LLM Adapter——mock LLM 响应的适配器测试。Agent Loop——ReAct 循环的 mock 测试。Tool Registry——工具注册和执行的单元测试）

## 4. Deterministic Replay Regression Tests

（待展开：这是 Tenet 最关键的一类测试。给定一个已知的事件流文件（预生成的 event_log snapshot），回放 Workflow，断言控制流与预期一致、最终结果与预期一致。每次 Workflow 代码变更后必须跑通全部回归测试）

## 5. Chaos Tests

（待展开：模拟崩溃恢复——kill 进程在 Decide 执行中/Decide 刚结束/Commit 前。验证恢复后的状态一致性。模拟 Redis 不可用——验证降级行为。模拟 SQLite BUSY——验证 WriteDaemon 排队）

## 6. Fork Consistency Tests

（待展开：从指定事件点 fork → 验证基础事件复制正确 → 验证新流独立执行 → 验证原流不受影响 → 验证谱系查询）
