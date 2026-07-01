# Python Native Tool Executor

> 本地工具执行器、目录安全、工具注册

---

## 1. Tool Interface

（待展开：Python Tool 基类——name/description/parameters(JSON Schema)/async execute(params)。与 LLM tool_use 格式的映射关系）

## 2. Tool Registry

（待展开：启动时加载内置工具 + 用户自定义工具。按名称查找。工具的白名单/黑名单配置）

## 3. Built-in Tools

（待展开：ReadFile——路径校验/大小限制/分页。WriteFile——覆盖写入/目录创建/权限。Shell——命令执行/sandbox 限制/timeout。WebSearch——搜索引擎适配）

## 4. Directory Safety

（待展开：物理目录防越权——限制文件操作在 workspace 目录内。路径规范化 + 前缀校验。危险命令黑名单（rm -rf /、curl pipe bash 等））

## 5. Tool Execution Model

（待展开：同步 vs 异步执行、超时控制、执行结果的结构化返回（stdout/stderr/exit_code/files_modified））

## 6. Fencing Token Integration

（待展开：文件写入前的 Fencing Token 校验——与 Lock Manager 的集成。Token 不匹配时拒绝写入 + 自我终止）
