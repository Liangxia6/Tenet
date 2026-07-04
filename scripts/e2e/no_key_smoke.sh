#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(mktemp -d)"
cleanup() {
  if [[ -f "$TMP_DIR/server.pid" ]]; then
    pid="$(cat "$TMP_DIR/server.pid")"
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" 2>/dev/null || true
  fi
  if [[ -f "$TMP_DIR/mock_openai.pid" ]]; then
    pid="$(cat "$TMP_DIR/mock_openai.pid")"
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

echo "[1/7] Go unit/integration tests"
(cd "$ROOT/go" && go test ./...)

echo "[2/7] Python unit tests"
(cd "$ROOT" && PYTHONPATH=python python3 -m unittest discover -s python/tests)

echo "[3/7] CLI echo task"
cat > "$TMP_DIR/tenet.yaml" <<YAML
database:
  path: "$TMP_DIR/tenet.db"
workspace:
  base_path: "$TMP_DIR/workspaces"
  snapshot_driver: archive
grpc:
  control_timeout_seconds: 5
  execute_timeout_seconds: 30
workflow:
  record_batch_size: 20
agent:
  default_max_steps: 3
  convergence_no_tool_calls: 1
  default_token_budget: 100000
YAML
(cd "$ROOT/go" && go run ./cmd/tenet task run --config "$TMP_DIR/tenet.yaml" --worker echo --workflow simple --workspace "$TMP_DIR/workspace" --output json "hello smoke" > "$TMP_DIR/cli_task.json")
grep -q '"status":"COMPLETED"' "$TMP_DIR/cli_task.json"

echo "[4/7] HTTP API echo task"
(cd "$ROOT/go" && go run ./cmd/tenet serve --config "$TMP_DIR/tenet.yaml" --http-port 18081 --port 19091 --worker-port 19092 > "$TMP_DIR/server.log" 2>&1 & echo $! > "$TMP_DIR/server.pid")
for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:18081/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
curl -fsS -X POST "http://127.0.0.1:18081/api/v1/tasks" \
  -H 'Content-Type: application/json' \
  -d "{\"query\":\"http smoke\",\"workspace\":\"$TMP_DIR/workspace\",\"worker\":\"echo\",\"workflow\":\"simple\"}" \
  > "$TMP_DIR/http_task.json"
grep -q '"status":"COMPLETED"' "$TMP_DIR/http_task.json"
curl -fsS "http://127.0.0.1:18081/api/v1/openapi.json" > "$TMP_DIR/openapi.json"
grep -q '"openapi":"3.0.3"' "$TMP_DIR/openapi.json"
grep -q '"/tasks"' "$TMP_DIR/openapi.json"
kill "$(cat "$TMP_DIR/server.pid")" >/dev/null 2>&1 || true

echo "[5/7] Go -> Python gRPC smoke"
if scripts/e2e/go_python_grpc_smoke.sh; then
  :
else
  status=$?
  if [[ "$status" -eq 77 ]]; then
    echo "Go -> Python gRPC smoke skipped because grpcio is not installed."
  else
    exit "$status"
  fi
fi

echo "[6/7] OpenAI-compatible mock task"
python3 "$ROOT/scripts/e2e/mock_openai_server.py" > "$TMP_DIR/mock_openai.log" 2>&1 & echo $! > "$TMP_DIR/mock_openai.pid"
for _ in $(seq 1 50); do
  if curl -fsS -X POST "http://127.0.0.1:18082/chat/completions" \
    -H 'Content-Type: application/json' \
    -d '{"messages":[{"role":"user","content":"ping"}]}' >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
export MOCK_OPENAI_API_KEY="mock-key"
(cd "$ROOT/go" && go run ./cmd/tenet task run --config "$TMP_DIR/tenet.yaml" --worker openai --base-url http://127.0.0.1:18082 --api-key-env MOCK_OPENAI_API_KEY --model mock-model --workflow simple --workspace "$TMP_DIR/workspace" --output json "mock adapter smoke" > "$TMP_DIR/openai_task.json")
grep -q '"status":"COMPLETED"' "$TMP_DIR/openai_task.json"
grep -q 'mock response' "$TMP_DIR/openai_task.json"

echo "[7/7] DeepSeek-compatible mock task"
export MOCK_DEEPSEEK_API_KEY="mock-key"
(cd "$ROOT/go" && go run ./cmd/tenet task run --config "$TMP_DIR/tenet.yaml" --worker deepseek --base-url http://127.0.0.1:18082 --api-key-env MOCK_DEEPSEEK_API_KEY --model deepseek-mock --workflow simple --workspace "$TMP_DIR/workspace" --output json "deepseek mock smoke" > "$TMP_DIR/deepseek_task.json")
grep -q '"status":"COMPLETED"' "$TMP_DIR/deepseek_task.json"
grep -q 'mock response' "$TMP_DIR/deepseek_task.json"

echo "no-key smoke: OK"
