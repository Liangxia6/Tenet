#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if ! python3 - <<'PY' >/dev/null 2>&1
import grpc
PY
then
  echo "Go -> Python gRPC smoke: SKIP (python3 cannot import grpc; install python package grpcio)"
  exit 77
fi

TMP_DIR="$(mktemp -d)"
PORT="$((20000 + ($$ % 20000)))"

cleanup() {
  if [[ -f "$TMP_DIR/python_worker.pid" ]]; then
    pid="$(cat "$TMP_DIR/python_worker.pid")"
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$TMP_DIR/workspace"
PYTHONPATH="$ROOT/python" python3 -m tenet.grpc_worker \
  --port "$PORT" \
  --workspace "$TMP_DIR/workspace" \
  --provider echo \
  > "$TMP_DIR/python_worker.log" 2>&1 &
echo $! > "$TMP_DIR/python_worker.pid"

python3 - "$PORT" "$TMP_DIR/workspace" <<'PY'
import json
import sys
import time

import grpc

from tenet.v1 import tenet_pb2, tenet_pb2_grpc

port = sys.argv[1]
workspace = sys.argv[2]
deadline = time.time() + 10
last_error = None
while time.time() < deadline:
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = tenet_pb2_grpc.TenetWorkerStub(channel)
            health = stub.HealthCheck(tenet_pb2.HealthCheckRequest(), timeout=1)
            if health.status == "SERVING":
                break
    except Exception as exc:  # noqa: BLE001 - smoke retry loop.
        last_error = exc
        time.sleep(0.1)
else:
    raise SystemExit(f"python worker did not become healthy: {last_error}")

with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
    stub = tenet_pb2_grpc.TenetWorkerStub(channel)
    write = stub.ExecuteTool(
        tenet_pb2.ExecuteToolRequest(
            session_id="grpc-smoke",
            workspace=workspace,
            tool_name="write_file",
            arguments=json.dumps({"path": "grpc.txt", "content": "hello from grpc"}),
        ),
        timeout=5,
    )
    if write.is_error:
        raise SystemExit(f"write_file failed: {write.stderr}")
    read = stub.ExecuteTool(
        tenet_pb2.ExecuteToolRequest(
            session_id="grpc-smoke",
            workspace=workspace,
            tool_name="read_file",
            arguments=json.dumps({"path": "grpc.txt"}),
        ),
        timeout=5,
    )
    if read.is_error or "hello from grpc" not in read.stdout:
        raise SystemExit(f"read_file failed: stdout={read.stdout!r} stderr={read.stderr!r}")
PY

cat > "$TMP_DIR/tenet.yaml" <<YAML
database:
  path: "$TMP_DIR/tenet.db"
workspace:
  base_path: "$TMP_DIR/workspaces"
  snapshot_driver: archive
grpc:
  control_timeout_seconds: 5
  execute_timeout_seconds: 30
  worker_port: "$PORT"
workflow:
  record_batch_size: 20
agent:
  default_max_steps: 3
  convergence_no_tool_calls: 1
  default_token_budget: 100000
YAML

(cd "$ROOT/go" && go run ./cmd/tenet task run \
  --config "$TMP_DIR/tenet.yaml" \
  --worker grpc \
  --worker-address "127.0.0.1:$PORT" \
  --workflow simple \
  --workspace "$TMP_DIR/workspace" \
  --output json \
  "go python grpc smoke" > "$TMP_DIR/go_grpc_task.json")

grep -q '"status":"COMPLETED"' "$TMP_DIR/go_grpc_task.json"
grep -q 'Echo response' "$TMP_DIR/go_grpc_task.json"

echo "Go -> Python gRPC smoke: OK"
