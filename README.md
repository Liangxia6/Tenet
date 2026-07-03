# Tenet

Tenet is an MVP agent runtime built around three agent capabilities and two engineering guarantees:

- Agent loop: planning, memory, tool calling.
- Engineering loop: event traceability, replay/fork safety, observable state.

The current MVP includes a Go orchestrator, SQLite event store, projection engine, scheduler, workspace snapshots, fork/restore, Redis-compatible locking with local fallback, gRPC gateway, and a Python worker with OpenAI-compatible/DeepSeek adapters.

## Quick Start

```bash
cp config/tenet.example.yaml config/tenet.yaml
make test
make smoke
```

Run a local echo task:

```bash
cd go
go run ./cmd/tenet task run --config ../config/tenet.yaml --workspace .. --worker echo --workflow simple "hello Tenet"
```

Inspect it:

```bash
go run ./cmd/tenet task list --config ../config/tenet.yaml
go run ./cmd/tenet task status --config ../config/tenet.yaml --stream <task_id>
go run ./cmd/tenet task logs --config ../config/tenet.yaml --stream <task_id>
```

## DeepSeek

Set the key and use the DeepSeek worker mode:

```bash
export DEEPSEEK_API_KEY=sk-...
cd go
go run ./cmd/tenet task run --config ../config/tenet.yaml --workspace .. --worker deepseek --workflow react "inspect this repo"
```

## Python gRPC Worker

Start the Python worker:

```bash
cd python
PYTHONPATH=. python -m tenet.grpc_worker --address 127.0.0.1:50052 --provider echo
```

Run Go against it:

```bash
cd go
go run ./cmd/tenet task run --config ../config/tenet.yaml --workspace .. --worker grpc --worker-address 127.0.0.1:50052 --workflow simple "grpc smoke"
```

## Trace, Snapshot, Fork

Create a workspace snapshot attached to a task stream:

```bash
go run ./cmd/tenet workspace snapshot --config ../config/tenet.yaml --path .. --session <task_id> --stream <task_id> --output json
```

Fork from a stream sequence and restore the latest workspace snapshot before that sequence:

```bash
go run ./cmd/tenet task fork --config ../config/tenet.yaml --stream <task_id> --seq <stream_seq> --query "try another path"
```

## HTTP API

```bash
cd go
go run ./cmd/tenet serve --config ../config/tenet.yaml --http-port 8080
```

Endpoints:

- `GET /healthz`
- `GET /tasks`
- `GET /events?stream_id=<task_id>&from=1`
