#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMPDIR="$(mktemp -d)"
CONFIG="$TMPDIR/tenet.yaml"
WORKSPACE="$TMPDIR/workspace"

mkdir -p "$WORKSPACE"
cat >"$CONFIG" <<YAML
database:
  path: $TMPDIR/tenet.db
  max_open_conns: 1
  write_queue_size: 16
redis:
  session_lock_ttl_seconds: 30
  session_heartbeat_seconds: 10
grpc:
  control_timeout_seconds: 1
  execute_timeout_seconds: 30
workflow:
  max_concurrent_tasks: 2
  record_batch_size: 5
  snapshot_event_interval: 2
workspace:
  base_path: $TMPDIR/workspaces
  snapshot_driver: archive
agent:
  default_max_steps: 5
  default_token_budget: 10000
interactive:
  human_timeout_seconds: 0
YAML

printf "before fork" >"$WORKSPACE/plan.md"

pushd "$ROOT/go" >/dev/null
go run ./cmd/tenet task run --config "$CONFIG" --workspace "$WORKSPACE" --worker echo --workflow simple --output json "smoke task" >"$TMPDIR/task.json"
TASK_ID="$(python3 - "$TMPDIR/task.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))["task_id"])
PY
)"

go run ./cmd/tenet task status --config "$CONFIG" --stream "$TASK_ID" --output json >"$TMPDIR/status.json"
go run ./cmd/tenet workspace snapshot --config "$CONFIG" --path "$WORKSPACE" --session "$TASK_ID" --stream "$TASK_ID" --seq 1 --output json >"$TMPDIR/snapshot.json"
SNAPSHOT_SEQ="$(python3 - "$TMPDIR/snapshot.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))["snapshot"]["StreamSeq"])
PY
)"

printf "after fork" >"$WORKSPACE/plan.md"
go run ./cmd/tenet task fork --config "$CONFIG" --stream "$TASK_ID" --seq "$SNAPSHOT_SEQ" --query "branch from snapshot" --output json >"$TMPDIR/fork.json"
go run ./cmd/tenet task resume --config "$CONFIG" --stream "$TASK_ID" --after 10ms --note "smoke resume" --output json >"$TMPDIR/resume.json"
popd >/dev/null

python3 - "$TMPDIR/status.json" "$TMPDIR/fork.json" "$TMPDIR/resume.json" <<'PY'
import json, pathlib, sys
status = json.load(open(sys.argv[1]))
fork = json.load(open(sys.argv[2]))
resume = json.load(open(sys.argv[3]))
fork_plan = pathlib.Path(fork["workspace"]) / "plan.md"
assert status["status"] == "COMPLETED", status
assert fork["restored"] is True, fork
assert fork_plan.read_text() == "before fork", fork_plan.read_text()
assert resume["fired"]["event_type"] == "TimerFired", resume
assert resume["resumed"]["event_type"] == "TaskResumed", resume
print("smoke ok")
print("task_id=", status["stream_id"])
print("fork_id=", fork["stream_id"])
PY
