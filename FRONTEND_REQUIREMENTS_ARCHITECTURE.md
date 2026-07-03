# Tenet 前端需求与设计架构文档

## 1. 文档目标

Tenet 后端已经具备 Agent MVP 的核心能力：任务运行、工作流、工具调用、事件日志、状态投影、workspace snapshot、fork、gRPC worker、DeepSeek/OpenAI-compatible adapter、HTTP/SSE API。

前端的目标不是做一个简单 dashboard，而是把这些能力包装成用户可理解、可操作、可追踪、可回溯的 Agent 工作台。

核心目标：

- 让用户可以创建、观察、控制 Agent 任务。
- 让用户能看懂 Agent 正在做什么、为什么这么做、用了什么工具。
- 让用户能基于历史状态 fork、回溯、恢复 workspace。
- 让工程用户能验证任务是否可靠、可重放、可审计。
- 让 MVP 后端能力通过前端形成完整产品体验。

## 2. 产品定位

Tenet 前端是一个 Agent Runtime Console。

它面向：

- 开发者：让 Agent 修改代码、运行测试、查看工具调用。
- 研究/分析用户：让 Agent 分解复杂问题、执行多阶段推理。
- 平台工程用户：观察 worker、事件流、任务状态、资源消耗。
- Agent 调试者：回放事件、检查 prompt/context/tool trace。

前端首屏应该是“可操作工作台”，不是营销落地页。

## 3. 用户核心问题

前端需要回答这些问题：

1. 我现在有哪些任务？
2. 某个任务运行到哪一步了？
3. Agent 为什么做出这个决策？
4. 它调用了几次模型？
5. 它用了哪些工具？工具输入输出是什么？
6. 它读写了哪些文件？
7. 当前 workspace 处于什么状态？
8. 我能否从某个历史点 fork 出一个新分支？
9. 我能否恢复到某个 snapshot？
10. 当前 worker / model / provider 是否健康？
11. 任务失败在哪里？能否 resume？
12. token/cost/时间消耗是多少？

## 4. MVP 前端范围

第一版前端必须覆盖以下能力：

- 创建 Agent 任务
- 选择 workflow
- 选择 worker/provider/model
- 查看任务列表
- 查看任务详情
- 实时事件流
- Timeline / Trace 视图
- LLM 调用视图
- 工具调用视图
- Token usage 视图
- Workspace snapshot / fork 操作
- Task resume / cancel 操作
- HTTP API 健康状态
- 基础配置页

第一版不强制实现：

- 完整 Monaco 代码编辑器
- 文件 diff 三方合并
- 多用户权限
- 团队空间
- 复杂 billing
- 完整可视化 DAG 编辑器

## 5. 信息架构

推荐导航结构：

```text
Tenet Console
├── Tasks
│   ├── Task List
│   ├── Task Detail
│   ├── Trace
│   ├── Events
│   ├── Tools
│   ├── Tokens
│   └── Forks
├── Workspace
│   ├── Files
│   ├── Snapshots
│   └── Restores
├── Workers
│   ├── Registered Workers
│   ├── gRPC Health
│   └── Provider Status
├── Settings
│   ├── Models
│   ├── Providers
│   ├── Safety
│   └── Runtime Config
└── System
    ├── Event Streams
    ├── Database
    └── Diagnostics
```

## 6. 页面设计

### 6.1 Tasks 首页

目标：用户进入后立刻知道系统里有哪些任务、哪些正在运行、哪些失败、哪些可继续。

核心元素：

- 顶部工具栏：
  - New Task
  - Refresh
  - Filter
  - Worker health indicator
- 任务表格：
  - Task ID
  - Status
  - Workflow
  - Query
  - Current Phase
  - Progress
  - Tokens
  - Updated At
  - Actions
- 状态筛选：
  - Running
  - Paused
  - Completed
  - Failed
  - Cancelled
- 快捷动作：
  - View
  - Watch
  - Resume
  - Cancel
  - Fork

表格应偏工程工具风格，信息密度高，避免大卡片堆叠。

### 6.2 New Task 页面 / Drawer

目标：用户能创建一个 Agent 任务。

字段：

- Query / Instruction
- Workspace Path
- Workflow Type
  - auto
  - simple
  - react
  - dag
  - scientific
  - coding
  - interactive
- Worker Mode
  - echo
  - openai
  - deepseek
  - grpc
- Worker Address，选择 grpc 时显示
- Model
- Token Budget
- Max Steps
- Scheduled execution toggle
- Static check command，coding workflow 可选
- Test command，coding workflow 可选

提交后：

- 创建任务
- 跳转到 Task Detail
- 自动打开 live trace

### 6.3 Task Detail 页面

目标：这是前端最重要页面，用户需要在这里理解 Agent 任务全过程。

布局建议：

```text
┌──────────────────────────────────────────────────────┐
│ Header: Task ID / Status / Workflow / Actions        │
├───────────────┬──────────────────────────────────────┤
│ Left Rail     │ Main Panel                           │
│ - Overview    │ - Timeline / Trace                   │
│ - Trace       │ - Event Detail                       │
│ - Tools       │ - LLM Call Detail                    │
│ - Tokens      │ - Workspace Snapshot/Fork Context    │
│ - Forks       │                                      │
│ - Raw Events  │                                      │
└───────────────┴──────────────────────────────────────┘
```

Header 显示：

- Task ID
- Status badge
- Workflow type
- Current phase
- Progress
- Token total
- Runtime duration

Actions：

- Resume
- Cancel
- Fork
- Create Snapshot
- Replay / Inspect
- Open Workspace

### 6.4 Overview Tab

显示任务摘要：

- Query
- Final Answer
- Current Status
- Workflow Type
- Workspace Path
- Parent Task
- Fork Children
- Created At / Updated At
- Error

进度：

- Completed steps / total steps
- Current phase
- Subtask list

### 6.5 Trace Tab

目标：让用户理解 Agent 的思考和动作顺序。

Trace 不是简单 event list，而是归类后的 timeline。

Timeline step 类型：

- Task Started
- LLM Call
- Tool Call
- Subtask Dispatched
- Subtask Completed
- Coding Phase Started
- Coding Phase Completed
- Timer Scheduled
- Timer Fired
- Waiting For Human Input
- Workspace Snapshot
- Fork Created
- Task Completed
- Task Failed

每个 step 展示：

- step seq
- event type
- timestamp
- phase
- summary
- duration
- status

点击 step 后右侧显示详情：

- Raw payload
- Related model call
- Related tool call
- Related workspace snapshot
- Related fork action

### 6.6 LLM Calls Tab

目标：回答“调用了几次 LLM、用了哪个 model、输入输出是什么、token 多少”。

当前后端已经有 `GenerateThought` 和 `TokenUsed` 事件，但前端需要把它们聚合成 LLM Calls。

字段：

- Call index
- Model
- Agent role
- System prompt
- Messages count
- Prompt preview
- Response
- Tool calls requested
- Prompt tokens
- Completion tokens
- Total tokens
- Cost
- Finish reason
- Timestamp

当前后端缺口：

- `GenerateThought` 事件里未独立结构化保存完整 request messages 统计字段。
- 需要后端补充 `LLMCallStarted / LLMCallCompleted` 或增强 `GenerateThought` payload。

前端第一版可先从现有 `GenerateThought` 和 `TokenUsed` 尽量聚合。

### 6.7 Tools Tab

目标：回答“Agent 用了什么工具、输入是什么、输出是什么、是否成功”。

字段：

- Tool name
- Call ID
- Arguments
- Workspace
- Fencing token
- Stdout
- Stderr
- Exit code
- Duration
- Is error
- Timestamp

当前支持：

- `ToolExecuted` event
- 本地工具 `read_file / write_file / shell`
- gRPC tool executor

前端交互：

- stdout/stderr 可折叠
- JSON arguments 格式化
- shell 命令危险标记
- 文件路径可点击跳到 Workspace Files

### 6.8 Events Tab

目标：给工程用户原始可审计日志。

功能：

- Raw event list
- JSON payload viewer
- Stream seq
- Event type filter
- Copy event JSON
- Jump to seq
- Export JSONL

数据来源：

- `GET /events?stream_id=<id>&from=<seq>`
- SSE live stream

### 6.9 Workspace 页面

目标：管理 workspace 文件、snapshot、restore。

第一版可以不做完整在线编辑器，但需要展示：

- Workspace path
- Snapshot list
- Snapshot type
- Snapshot ref
- Stream seq
- Created at
- Restore action
- Fork from snapshot action

当前后端缺口：

- 还没有 HTTP 文件浏览 API。
- 还没有 snapshot list API。
- CLI 有 workspace snapshot/restore，但 HTTP API 需要补。

前端第一版可以先在 Task Detail 中显示 snapshot/fork 元信息，文件浏览作为第二阶段。

### 6.10 Fork / Replay 视图

目标：让用户基于历史事件创建新任务分支。

交互：

1. 用户在 timeline 上选择某个 seq。
2. 前端显示该 seq 附近事件。
3. 前端提示是否存在可用 workspace snapshot。
4. 用户输入新的 query。
5. 点击 Fork。
6. 系统创建 child task。
7. 跳转到 child task detail。

需要展示：

- Parent task
- Fork seq
- Fork query
- Restored workspace
- Snapshot used
- Child task id

当前后端支持：

- `task fork`
- `ForkStream`
- `ForkWorkspace`
- lineage

当前 HTTP API 缺口：

- 需要新增 `POST /tasks/{id}/fork`
- 需要新增 `GET /tasks/{id}/lineage`

### 6.11 Workers 页面

目标：查看 worker/provider 健康状态。

展示：

- Go orchestrator health
- Python gRPC worker health
- Registered workers
- Provider
- Model
- Max concurrency
- Uptime
- Last heartbeat

当前后端支持部分：

- gRPC HealthCheck
- WorkerRegistry snapshot

HTTP API 需要补：

- `GET /workers`
- `GET /workers/{id}/health`

### 6.12 Settings 页面

目标：配置 provider、model、workspace、安全策略。

第一版建议只读展示配置，避免误改运行时配置。

展示：

- Database path
- Workspace base path
- Redis addr
- gRPC ports
- Workflow defaults
- LLM providers
- Safety dangerous patterns
- Rate limits

第二阶段再支持编辑。

## 7. 前端核心用户流程

### 7.1 创建并观察任务

```text
New Task
→ 填 Query / Workflow / Worker
→ Submit
→ 跳 Task Detail
→ Timeline 实时滚动
→ 查看 LLM Calls / Tools / Tokens
→ Completed
```

### 7.2 复杂任务 Trace 调试

```text
Task Detail
→ Trace Tab
→ 点击 DAG SubTask
→ 查看 GenerateThought
→ 查看 ToolExecuted
→ 查看 TokenUsed
→ 定位失败/低质量步骤
```

### 7.3 从历史点 fork

```text
Task Detail
→ Timeline
→ 选择 seq
→ Fork
→ 输入新 query
→ 选择 restore latest snapshot
→ 创建 child task
→ 查看 lineage
```

### 7.4 恢复 workspace

```text
Workspace/Snapshots
→ 选择 snapshot
→ 查看 snapshot ref/seq
→ Restore
→ 确认
```

### 7.5 延迟恢复任务

```text
Task Detail
→ Resume
→ 选择 immediately 或 after duration
→ 查看 TimerScheduled
→ 查看 TimerFired
→ 查看 TaskResumed
```

## 8. 前端技术架构建议

推荐技术栈：

- React
- TypeScript
- Vite
- TanStack Query
- Zustand 或 Redux Toolkit
- React Router
- Tailwind CSS
- shadcn/ui 或 Radix UI
- Monaco Editor，第二阶段用于 JSON/code viewer
- Recharts 或 uPlot，用于 token/cost/time charts

不建议第一版引入过重框架。

## 9. 前端目录结构建议

```text
frontend/
├── src/
│   ├── app/
│   │   ├── router.tsx
│   │   └── providers.tsx
│   ├── api/
│   │   ├── client.ts
│   │   ├── tasks.ts
│   │   ├── events.ts
│   │   ├── workers.ts
│   │   └── workspace.ts
│   ├── features/
│   │   ├── tasks/
│   │   ├── trace/
│   │   ├── tools/
│   │   ├── tokens/
│   │   ├── workspace/
│   │   ├── workers/
│   │   └── settings/
│   ├── components/
│   │   ├── layout/
│   │   ├── data-table/
│   │   ├── json-viewer/
│   │   ├── timeline/
│   │   └── status-badge/
│   ├── stores/
│   ├── types/
│   └── main.tsx
├── package.json
└── vite.config.ts
```

## 10. 前端状态模型

### 10.1 TaskView

对应后端 Projection：

```ts
type TaskView = {
  stream_id: string;
  status: "RUNNING" | "PAUSED" | "COMPLETED" | "FAILED";
  query?: string;
  workspace?: string;
  workflow_type?: string;
  current_phase?: string;
  progress: {
    completed_steps: number;
    total_steps: number;
  };
  subtasks?: SubTaskState[];
  final_answer?: string;
  error?: string;
  timeline: TimelineState;
  tokens: TokenState;
};
```

### 10.2 Event

```ts
type StreamEvent = {
  id: number;
  stream_id: string;
  stream_seq: number;
  event_type: string;
  payload: string;
  parent_id?: string;
  timestamp: string;
};
```

### 10.3 TraceStep

前端从 Event 派生：

```ts
type TraceStep = {
  seq: number;
  kind:
    | "llm"
    | "tool"
    | "workflow"
    | "timer"
    | "workspace"
    | "fork"
    | "status";
  title: string;
  summary?: string;
  status?: "ok" | "error" | "running" | "paused";
  event: StreamEvent;
};
```

## 11. API 需求

当前后端已有：

- `GET /healthz`
- `GET /tasks`
- `GET /events?stream_id=&from=`

为了完整前端体验，需要新增：

### 11.1 Tasks

```http
POST /tasks
GET /tasks
GET /tasks/{stream_id}
GET /tasks/{stream_id}/events
GET /tasks/{stream_id}/status
POST /tasks/{stream_id}/cancel
POST /tasks/{stream_id}/resume
POST /tasks/{stream_id}/fork
GET /tasks/{stream_id}/lineage
```

### 11.2 Trace

```http
GET /tasks/{stream_id}/trace
GET /tasks/{stream_id}/llm-calls
GET /tasks/{stream_id}/tool-calls
GET /tasks/{stream_id}/tokens
```

第一版这些可以由前端从 events 聚合；第二版建议后端直接提供 projection。

### 11.3 Workspace

```http
GET /workspaces/{session_id}
GET /workspaces/{session_id}/snapshots
POST /workspaces/{session_id}/snapshots
POST /workspaces/{session_id}/restore
GET /workspaces/{session_id}/files
GET /workspaces/{session_id}/files/{path}
```

### 11.4 Workers

```http
GET /workers
GET /workers/{worker_id}/health
```

### 11.5 Config

```http
GET /config
GET /providers
GET /models
```

## 12. SSE 实时更新设计

前端连接：

```ts
const source = new EventSource(`/events?stream_id=${streamId}&from=${fromSeq}`);
source.onmessage = (event) => {
  const streamEvent = JSON.parse(event.data);
  applyEvent(streamEvent);
};
```

更新策略：

- 初次进入 Task Detail：
  - 先拉 `GET /tasks/{id}/status`
  - 再拉 `GET /events?from=1`
  - 然后打开 SSE，从 latest seq + 1 开始
- SSE 断线：
  - 记录 lastSeq
  - 重连时从 lastSeq + 1 拉取
- 避免重复事件：
  - 以 `stream_id + stream_seq` 去重

## 13. Trace 聚合规则

前端需要把 raw events 聚合成人可读 timeline。

规则示例：

| Event Type | Trace Kind | 展示 |
|---|---|---|
| TaskStarted | workflow | 任务开始 |
| GenerateThought | llm | 模型调用 |
| TokenUsed | llm | token usage |
| ToolExecuted | tool | 工具调用 |
| TaskDecomposed | workflow | DAG 分解 |
| SubTaskDispatched | workflow | 子任务开始 |
| SubTaskCompleted | workflow | 子任务完成 |
| CodingPhaseStarted | workflow | coding 阶段开始 |
| CodingPhaseCompleted | workflow | coding 阶段完成 |
| WorkspaceSnapshot | workspace | 工作区快照 |
| ForkCreated | fork | 创建 fork |
| ForkWorkspaceRestored | workspace | fork workspace 恢复 |
| TimerScheduled | timer | timer 计划 |
| TimerFired | timer | timer 触发 |
| TaskResumed | status | 任务恢复 |
| TaskCompleted | status | 任务完成 |
| TaskFailed | status | 任务失败 |

## 14. UI 组件设计

### 14.1 StatusBadge

状态：

- RUNNING：蓝色
- PAUSED：黄色
- COMPLETED：绿色
- FAILED：红色

### 14.2 WorkflowBadge

workflow 类型：

- simple
- react
- dag
- scientific
- coding
- interactive

### 14.3 TraceTimeline

要求：

- 固定宽度 seq 列
- 中间事件类型
- 右侧摘要
- 点击展开详情
- 支持 filter
- 支持 jump to seq

### 14.4 JsonPayloadViewer

要求：

- JSON 格式化
- copy
- collapse/expand
- 搜索 key

### 14.5 ToolCallPanel

展示：

- Tool
- Arguments
- stdout
- stderr
- exit code
- duration

### 14.6 TokenUsagePanel

展示：

- total tokens
- prompt tokens
- completion tokens
- cost
- by model
- by agent
- budget exceeded

### 14.7 ForkDialog

字段：

- selected seq
- parent task
- latest snapshot before seq
- new query
- restore workspace toggle

## 15. 页面路由

```text
/tasks
/tasks/new
/tasks/:streamId
/tasks/:streamId/trace
/tasks/:streamId/events
/tasks/:streamId/tools
/tasks/:streamId/tokens
/tasks/:streamId/forks
/workspace
/workspace/:sessionId
/workers
/settings
/system
```

## 16. 权限与安全

第一版本地使用，可以不做登录。

但 UI 必须明确危险操作：

- cancel task
- restore workspace
- cleanup workspace
- shell tool execution
- fork from old snapshot

危险操作要使用确认弹窗。

对于 shell 工具：

- 显示 command
- 标记 dangerous pattern
- 展示 exit code
- stderr 默认展开

## 17. 可观测性设计

前端应提供系统层 diagnostics：

- API health
- SSE connection status
- last event seq
- active tasks
- worker health
- db path
- config source

Task Detail 应提供：

- runtime duration
- model calls count
- tool calls count
- token total
- latest event seq
- projection updated at

## 18. 当前后端缺口清单

为了前端做得完整，后端建议补：

1. `POST /tasks` 创建任务 API
2. `GET /tasks/{id}` 单任务 projection API
3. `POST /tasks/{id}/fork`
4. `POST /tasks/{id}/resume`
5. `POST /tasks/{id}/cancel`
6. `GET /tasks/{id}/lineage`
7. `GET /tasks/{id}/trace` 聚合 trace API
8. `GET /tasks/{id}/llm-calls`
9. `GET /tasks/{id}/tool-calls`
10. `GET /workspaces/{id}/snapshots`
11. `POST /workspaces/{id}/snapshots`
12. `POST /workspaces/{id}/restore`
13. `GET /workers`
14. `GET /config`
15. LLM call payload 增强：记录 request message count、context token estimate、model、provider、latency
16. Tool call payload 增强：记录 input/output file paths
17. Workspace 自动 checkpoint：write_file/shell 前后自动 snapshot，可配置

## 19. 实施阶段

### Phase 1：只读 Console

目标：先让用户看见系统。

内容：

- Vite React 项目初始化
- Layout / navigation
- Tasks list
- Task detail
- Events view
- Trace timeline
- Token panel
- SSE live update

依赖后端：

- 当前 `/tasks`
- 当前 `/events`
- 当前 projection

### Phase 2：任务控制

目标：用户能操作任务。

内容：

- New Task
- Resume
- Cancel
- Fork dialog
- Workflow selector
- Worker/model selector

依赖后端新增：

- `POST /tasks`
- `POST /tasks/{id}/resume`
- `POST /tasks/{id}/cancel`
- `POST /tasks/{id}/fork`

### Phase 3：Workspace 与回溯

目标：用户能理解和恢复文件状态。

内容：

- Snapshot list
- Restore
- Fork from snapshot
- File browser 只读版
- Snapshot diff，第二阶段

依赖后端新增：

- workspace snapshots API
- file browser API

### Phase 4：Trace 深化

目标：接近 LangSmith/Temporal 式调试体验。

内容：

- LLM calls 独立视图
- Prompt/context viewer
- Tool call graph
- DAG graph
- Cost/time charts
- Replay/fork comparison

依赖后端增强：

- 结构化 LLM call events
- context assembler trace
- tool read/write trace

## 20. 前端完成标准

第一版前端可以认为完成，当满足：

- 用户可以创建 task。
- 用户可以实时看到 task event。
- 用户可以查看 task status/progress/final answer。
- 用户可以看到 LLM 调用次数和 token usage。
- 用户可以看到 tool calls。
- 用户可以 fork 一个历史 seq。
- 用户可以看到 lineage。
- 用户可以执行 resume/cancel。
- 用户可以看到 workspace snapshot 信息。
- 用户可以判断 worker/API 是否健康。
- 所有关键动作失败时有明确错误提示。

## 21. 推荐首屏设计

首屏不是欢迎页，而是 Tasks Console：

```text
┌─────────────────────────────────────────────────────────┐
│ Tenet      Tasks   Workspace   Workers   Settings       │
├─────────────────────────────────────────────────────────┤
│ New Task   Search...       Status Filter   Refresh      │
├─────────────────────────────────────────────────────────┤
│ Task ID     Status   Workflow   Query   Progress Tokens │
│ task:123    RUNNING  coding     ...     3/7      12k    │
│ task:456    FAILED   react      ...     2/5      4k     │
└─────────────────────────────────────────────────────────┘
```

点击 task 后进入 detail：

```text
┌─────────────────────────────────────────────────────────┐
│ task:123  RUNNING  coding  phase: unit_test             │
│ Resume Cancel Fork Snapshot                             │
├──────────────┬──────────────────────────────────────────┤
│ Overview     │ Timeline                                 │
│ Trace        │ 1 TaskStarted                            │
│ LLM Calls    │ 2 GenerateThought                        │
│ Tools        │ 3 ToolExecuted read_file                 │
│ Tokens       │ 4 CodingPhaseStarted                     │
│ Events       │ 5 ToolExecuted shell go test             │
└──────────────┴──────────────────────────────────────────┘
```

## 22. 总结

Tenet 前端要解决的不是“展示任务列表”，而是把 Agent 的黑盒执行变成可观察、可审计、可分叉、可恢复的工程工作台。

第一阶段优先做：

- Tasks
- Task Detail
- Trace Timeline
- Events/SSE
- Tools
- Tokens

第二阶段再做：

- New Task
- Fork/Resume/Cancel
- Workspace snapshots
- Workers/settings

第三阶段做深：

- LLM context trace
- Prompt/message 回溯
- 文件 diff
- DAG graph
- 任意历史点恢复体验
