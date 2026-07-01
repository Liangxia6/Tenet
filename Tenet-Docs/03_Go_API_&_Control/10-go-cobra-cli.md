# Go Cobra CLI

> Cobra 命令行树、命令规范

---

## 1. Command Tree

（待展开：根命令 `tenet` 下的二级命令树——serve / task run / task replay / task list / task fork / session list / config validate）

## 2. `tenet serve`

（待展开：启动 gRPC Server + Workflow Engine + Redis pool + WriteDaemon。端口/配置路径等 Flag 定义。Health check 端点）

## 3. `tenet task run`

（待展开：创建并执行单个 Task。参数——query、workflow_type（可选覆盖）、config 路径。输出——task_id、实时事件流、最终结果）

## 4. `tenet task replay`

（待展开：回放指定 Task 流。参数——stream_id、回放模式（完整/从断点）。输出——事件序列、确定性检查结果）

## 5. `tenet task fork`

（待展开：从指定 stream_id 的第 N 个事件分叉。参数——stream_id、fork_seq、新的 task query。输出——新 stream_id）

## 6. Flag Conventions

（待展开：全局 Flag（--config/--verbose）vs 命令级 Flag。环境变量覆盖规则。Flag 命名规范）
