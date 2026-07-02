# Tenet Usable Agent Implementation Plan

> Goal: evolve the current local MVP into a practically usable agent system.
>
> Core framing:
> - Agent perspective: Agent = planning + memory + tool use.
> - Engineering perspective: the system must be replayable, observable, safe enough, and easy to run.

## Current State

The project currently has a runnable local MVP:

- Go event store, workflow context, basic scheduler, local lock/budget guards, and CLI.
- Simple/ReAct workflow skeletons with deterministic `Record` / `Decide`.
- Python stateless worker skeleton with echo adapter and native tools.
- Proto contract exists, but generated gRPC clients/servers are not wired into runtime.
- `make test` passes for Go and Python tests.

It is not yet a usable agent because it does not call a real LLM, does not run cross-process Go/Python gRPC, and does not yet execute a real model-driven tool loop.

## Product Definition

A first usable version should let a user run:

```bash
tenet task run "read this project and fix the failing tests" --workspace .
```

Expected behavior:

- The agent plans the work.
- The agent reads files and runs shell commands.
- The agent edits files when needed.
- The agent observes command output and continues.
- The task ends with a final answer and a persisted event history.
- The user can inspect and replay the task history.

## Track A: Agent Capability

This track answers: can the agent actually do useful work?

### A1. Real LLM Adapter

Implement Python LLM adapters behind the existing stateless worker contract.

- Add OpenAI-compatible adapter.
- Keep `EchoAdapter` for deterministic tests.
- Support model, temperature, messages, tool definitions, and token usage.
- Parse assistant text and tool calls into the existing response shape.
- Validate malformed model responses before returning them to Go.

Acceptance:

- Unit tests cover valid text response, valid tool call response, malformed tool call response.
- A local worker can return real LLM output when `OPENAI_API_KEY` or compatible config is present.

### A2. Go to Python gRPC Runtime

Turn the proto contract into actual cross-process communication.

- Generate Go stubs under `go/internal/gateway/gen/tenet/v1`.
- Generate Python stubs under `python/tenet/proto`.
- Implement Python `TenetWorker` gRPC server.
- Implement Go `worker.Client` backed by gRPC.
- Preserve the current local `EchoClient` for tests.
- Add health check and deadline handling.

Acceptance:

- Go integration test calls Python worker `HealthCheck`.
- Go integration test calls Python worker `GenerateThought`.
- CLI can choose `--worker local` or `--worker grpc`.

### A3. ReAct Tool Loop

Make the default useful workflow: think, call tools, observe, continue.

- Use Go `ReactWorkflow` as the primary workflow for practical tasks.
- Send built-in tool schemas to the model.
- Execute tool calls via Python `ExecuteTool`.
- Append tool results to messages.
- Stop on `is_final`, no tool calls, or max steps.
- Record every thought, tool execution, token use, and completion.

Acceptance:

- A fixture model/client can request `read_file`, then `shell`, then return final answer.
- Event stream contains `GenerateThought`, `ToolExecuted`, `TokenUsed`, `TaskCompleted`.
- No external calls happen during replay.

### A4. Planning

Add explicit planning as a first-class agent capability.

- Add `PlanCreated` event.
- For non-trivial tasks, first LLM call produces a concise step plan.
- Plan is injected into later ReAct messages.
- Support simple plan updates through `PlanUpdated` events.

Acceptance:

- `task inspect` shows plan events.
- ReAct can proceed with or without planning based on workflow selection.

### A5. Memory

Start with event-sourced short-term memory before building complex long-term memory.

- Treat task event history as the source of truth.
- Reconstruct message history from events.
- Add a compact task summary event after completion.
- Later add workspace/project memory indexed by repository path.

Acceptance:

- Replay reconstructs the same messages from event history.
- Completed task has a concise summary payload.

## Track B: Engineering Reliability

This track answers: can users trust and operate the system?

### B1. True Replay

Upgrade `task replay` from printing events to executing the workflow in replay mode.

- Run the same workflow code against historical events.
- Ensure `Decide` does not call external workers in replay.
- Add Three-Zero assertions:
  - zero new events,
  - zero external calls,
  - zero unconsumed history.

Acceptance:

- Replay regression tests run against JSONL fixtures.
- Replay fails loudly on non-determinism.

### B2. Observability

Make task state easy to inspect.

- Improve `task inspect`.
- Add JSON output for all CLI commands.
- Show status, workflow, token usage, duration, final answer, and event count.
- Add task list command backed by event projections or simple event queries.

Acceptance:

- User can find recent tasks.
- User can see why a task failed without reading raw SQLite.

### B3. Workspace Safety

Make tool use powerful but bounded.

- Enforce workspace path containment in Python tools.
- Keep shell dangerous-pattern blocklist.
- Add configurable tool approvals for risky tools.
- Add per-tool timeouts.
- Add write-file audit events.

Acceptance:

- Path traversal tests pass.
- Shell timeout tests pass.
- Risky tools can be disabled or approval-gated.

### B4. Configuration and Developer Experience

Make the project easy to run locally.

- Add `config/tenet.yaml.example`.
- Add `make dev`, `make worker`, `make smoke`.
- Document environment variables.
- Make CLI errors actionable.

Acceptance:

- Fresh clone can run tests and local echo task.
- With an API key, fresh clone can run a real ReAct task.

### B5. Locking and Budget

Move from local guards to production-capable guards.

- Add Redis-backed session lock.
- Add fencing token validation to Python `ExecuteTool`.
- Add token budget projection from event history.
- Abort task when budget is exceeded.

Acceptance:

- Two agents cannot run the same session concurrently under Redis.
- Stale fencing token blocks tool execution.
- Budget exceeded produces a persisted `TaskFailed` event.

### B6. Suspend and Resume

Support long-running or approval-gated workflows.

- Implement `ctx.Sleep`.
- Implement TimerService.
- Record `TimerStarted` and `TimerFired`.
- Resume workflow in replay mode after timer fires.

Acceptance:

- Sleep test uses virtual or short clock.
- Worker is released while task is suspended.

## Track C: Workflow Expansion

This track adds stronger task strategies once the basic agent works.

### C1. Strategy Router

- Replace keyword routing with LLM-assisted complexity analysis.
- Persist `ComplexityAnalyzed`.
- Keep explicit workflow override as highest priority.
- Always fall back to `simple` or `react`.

### C2. DAG Workflow

- First implement sequential DAG execution.
- Add LLM decomposition.
- Persist `TaskDecomposed`, `SubTaskStarted`, `SubTaskCompleted`.
- Add deterministic async only after sequential DAG is stable.

### C3. Coding Workflow

- Implement Design -> Edit -> Test -> Fix -> Finalize.
- Use `read_file`, `write_file`, and `shell`.
- Feed test failures back into the next model step.
- Add retry limits and final summary.

## Recommended Execution Order

1. Real LLM adapter.
2. Generated gRPC and Go/Python worker wiring.
3. Real ReAct tool loop with read/write/shell.
4. Default config, dev commands, and CLI UX.
5. True replay with Three-Zero assertions.
6. Workspace safety hardening and tool approvals.
7. Redis lock, fencing token, and token budget projection.
8. Suspend/resume timer support.
9. Strategy router, DAG, and coding workflow.

## First Milestone: Usable Single-Machine Agent

Scope:

- One Go orchestrator process.
- One Python worker process.
- OpenAI-compatible LLM.
- ReAct workflow.
- Built-in tools: read_file, write_file, shell.
- SQLite event log.
- CLI task run / inspect / replay.

Definition of done:

```bash
tenet task run "inspect this repo and summarize how to run tests" --workspace .
```

The agent should read project files, optionally run tests, produce a useful answer, and leave a replayable event stream.

## Non-Goals Until After First Milestone

- Full multi-agent DAG concurrency.
- MCP integration.
- Distributed deployment.
- Long-term vector memory.
- Sophisticated UI.
- Full Git workspace snapshot manager.

These are valuable, but they should wait until the single-machine agent is genuinely useful.
