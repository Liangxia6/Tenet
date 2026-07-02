# Tenet Implementation Roadmap

## Phase 0 — Repository Bootstrap
- Initialize Go module `github.com/tenet/orchestrator` under `go/`.
- Initialize Python package `tenet` under `python/` with Poetry/virtualenv metadata.
- Add shared `proto/` directory containing `tenet/v1/tenet.proto` per spec.
- Provide Makefile + tooling scripts for generating gRPC stubs.

## Phase 1 — Configuration & Storage Foundations
- Implement YAML configuration loader in Go with static/dynamic classifications.
- Implement SQLite event store with WriteDaemon, migrations, and telemetry tables.
- Provide minimal projection engine (task & timeline) plus snapshot hooks (stubs initially returning TODO errors).
- Deliver config validation CLI command `tenet config validate`.

## Phase 2 — Workflow Runtime Core
- Implement `WorkflowContext`, `Decide`, `Record`, `GetVersion`, `Async` scaffolding.
- Build Scheduler, WorkerPool, TimerService (basic sleep support).
- Implement LockManager (Redis + in-memory fallback) and TokenBudget guard.
- Provide SimpleWorkflow end-to-end execution path (GenerateThought + Record events).

## Phase 3 — gRPC Integration
- Implement Go gRPC server (`TenetOrchestrator`) and client middleware stack for calling Python worker.
- Implement Python worker with `GenerateThought`, `ExecuteTool`, `HealthCheck` skeletons and adapter registry.
- Support built-in tools: read_file, write_file, shell (with blacklist), web_search (stub returns TODO).
- Provide CLI commands: `tenet serve`, `tenet task run` (Simple workflow), `tenet task replay` (partial).

## Phase 4 — Extended Workflows & Patterns
- Implement ReactWorkflow with tool execution loop and token tracking.
- Add StrategyRouter with complexity analysis stub (fixed thresholds) and workflow registry.
- Add DAGWorkflow skeleton (sequential execution fallback until async logic complete).

## Phase 5 — Testing & QA Harness
- Add Go unit tests for event store, workflow context, scheduler.
- Add Python pytest suite for tool executor + adapters.
- Provide replay regression harness using fixture JSONL.

## Phase 6 — Stretch Goals (post-initial delivery)
- Scientific, Interactive, Coding workflows full implementation.
- Projection snapshots, workspace Git/archive hybrid, backup/restore.
- Redis-based rate limiting & MCP integration.

> NOTE: Each phase builds upon prior work; we will aim to land Phase 0-3 in the first implementation cycle to achieve an executable MVP, then iterate.
