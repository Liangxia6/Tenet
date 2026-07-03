# Tenet 全功能手动测试指南

本文档用于手动验证 Tenet MVP 的完整功能链路。建议按顺序执行；每一步都给出命令、预期结果和排查提示。

## 0. 测试范围

本手册覆盖：

- Go CLI / SQLite event store
- Python Worker / Go gRPC Gateway
- Echo、本地 OpenAI-compatible、DeepSeek provider
- Workflow：simple、react、dag、scientific、coding、interactive
- Event log、task status、task logs、task watch
- Projection 快照
- Workspace snapshot / restore
- Task fork / lineage
- Timer / delayed resume
- Scheduler
- HTTP API / SSE events

## 1. 环境准备

进入项目根目录：

```bash
cd /Users/hcy/Desktop/Tenet
```

确认 Go、Python 可用：

```bash
go version
python3 --version
```

复制配置：

```bash
cp config/tenet.example.yaml config/tenet.local.yaml
```

为了避免污染项目目录，建议本轮测试使用临时数据库和临时 workspace：

```bash
export TENET_TEST_ROOT="$(mktemp -d)"
cat > "$TENET_TEST_ROOT/tenet.yaml" <<YAML
database:
  path: $TENET_TEST_ROOT/tenet.db
  max_open_conns: 1
  write_queue_size: 64
redis:
  session_lock_ttl_seconds: 30
  session_heartbeat_seconds: 10
grpc:
  orchestrator_port: 50051
  worker_port: 50052
  control_timeout_seconds: 2
  execute_timeout_seconds: 60
workflow:
  max_concurrent_tasks: 4
  record_batch_size: 5
  snapshot_event_interval: 2
workspace:
  base_path: $TENET_TEST_ROOT/workspaces
  snapshot_driver: archive
agent:
  default_max_steps: 5
  default_token_budget: 10000
interactive:
  human_timeout_seconds: 1
YAML
```

## 2. 自动基线测试

先跑项目内置测试：

```bash
make test
make smoke
```

预期：

- `make test`：Go 全部通过；如果默认 `python3` 未安装 `grpcio`，Python gRPC 相关测试可能显示 skipped。
- `make smoke`：输出 `smoke ok`。

如果你希望 Python gRPC 测试不跳过：

```bash
cd /Users/hcy/Desktop/Tenet/python
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
python -m unittest discover -s tests -p 'test_*.py'
deactivate
```

## 3. CLI 基础任务

创建一个测试 workspace：

```bash
mkdir -p "$TENET_TEST_ROOT/workspace"
echo "hello tenet" > "$TENET_TEST_ROOT/workspace/README.md"
```

运行 simple workflow：

```bash
cd /Users/hcy/Desktop/Tenet/go
go run ./cmd/tenet task run \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --workspace "$TENET_TEST_ROOT/workspace" \
  --worker echo \
  --workflow simple \
  --output json \
  "summarize this workspace" | tee "$TENET_TEST_ROOT/simple.json"
```

保存 task id：

```bash
export TENET_TASK_ID="$(python3 - <<'PY'
import json, os
print(json.load(open(os.environ["TENET_TEST_ROOT"] + "/simple.json"))["task_id"])
PY
)"
echo "$TENET_TASK_ID"
```

预期：

- JSON 中 `status` 为 `COMPLETED`
- `task_id` 类似 `task:178...`

查看任务列表：

```bash
go run ./cmd/tenet task list --config "$TENET_TEST_ROOT/tenet.yaml"
```

查看任务状态：

```bash
go run ./cmd/tenet task status \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --stream "$TENET_TASK_ID"
```

预期：

- `status: COMPLETED`
- 有 `final_answer`
- `tokens` 和 `timeline` 可见

查看事件日志：

```bash
go run ./cmd/tenet task logs \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --stream "$TENET_TASK_ID" \
  --output jsonl
```

预期能看到：

- `TaskStarted`
- `GenerateThought`
- `TaskCompleted`

## 4. 多 Workflow 测试

依次测试不同 workflow：

```bash
for wf in react dag scientific coding interactive; do
  echo "=== workflow: $wf ==="
  go run ./cmd/tenet task run \
    --config "$TENET_TEST_ROOT/tenet.yaml" \
    --workspace "$TENET_TEST_ROOT/workspace" \
    --worker echo \
    --workflow "$wf" \
    --output json \
    "test $wf workflow"
done
```

预期：

- 每个 workflow 都返回 `COMPLETED`
- `interactive` 因配置了 `human_timeout_seconds: 1`，会产生等待/计时器事件

重点检查 interactive：

```bash
go run ./cmd/tenet task run \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --workspace "$TENET_TEST_ROOT/workspace" \
  --worker echo \
  --workflow interactive \
  --output json \
  "draft something that needs human review" | tee "$TENET_TEST_ROOT/interactive.json"

export TENET_INTERACTIVE_ID="$(python3 - <<'PY'
import json, os
print(json.load(open(os.environ["TENET_TEST_ROOT"] + "/interactive.json"))["task_id"])
PY
)"

go run ./cmd/tenet task logs \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --stream "$TENET_INTERACTIVE_ID"
```

预期可能出现：

- `WaitingForHumanInput`
- `TimerScheduled`
- `TimerFired`
- `TaskCompleted`

## 5. Scheduler 测试

通过 scheduler worker pool 执行任务：

```bash
go run ./cmd/tenet task run \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --workspace "$TENET_TEST_ROOT/workspace" \
  --worker echo \
  --workflow simple \
  --scheduled \
  --output json \
  "scheduled task"
```

预期：

- 返回 `COMPLETED`
- 没有阻塞或超时

## 6. Workspace Snapshot / Restore

创建快照：

```bash
echo "before snapshot" > "$TENET_TEST_ROOT/workspace/state.txt"

go run ./cmd/tenet workspace snapshot \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --path "$TENET_TEST_ROOT/workspace" \
  --session "$TENET_TASK_ID" \
  --stream "$TENET_TASK_ID" \
  --output json | tee "$TENET_TEST_ROOT/snapshot.json"
```

预期：

- 输出 `snapshot.Type` 为 `archive`
- 输出 `snapshot.Ref` 指向 `.backup/*.tar.gz`
- 输出 `snapshot.StreamSeq`

单独 restore 到新目录：

```bash
export TENET_SNAPSHOT_REF="$(python3 - <<'PY'
import json, os
print(json.load(open(os.environ["TENET_TEST_ROOT"] + "/snapshot.json"))["snapshot"]["Ref"])
PY
)"

mkdir -p "$TENET_TEST_ROOT/restored"
go run ./cmd/tenet workspace restore \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --archive "$TENET_SNAPSHOT_REF" \
  --dest "$TENET_TEST_ROOT/restored"

cat "$TENET_TEST_ROOT/restored/state.txt"
```

预期输出：

```text
before snapshot
```

## 7. Fork / Lineage / Workspace Restore

修改父 workspace：

```bash
echo "after snapshot" > "$TENET_TEST_ROOT/workspace/state.txt"
```

从 snapshot 所在事件序号 fork：

```bash
export TENET_SNAPSHOT_SEQ="$(python3 - <<'PY'
import json, os
print(json.load(open(os.environ["TENET_TEST_ROOT"] + "/snapshot.json"))["snapshot"]["StreamSeq"])
PY
)"

go run ./cmd/tenet task fork \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --stream "$TENET_TASK_ID" \
  --seq "$TENET_SNAPSHOT_SEQ" \
  --query "try another branch" \
  --output json | tee "$TENET_TEST_ROOT/fork.json"
```

保存 fork id 和 workspace：

```bash
export TENET_FORK_ID="$(python3 - <<'PY'
import json, os
print(json.load(open(os.environ["TENET_TEST_ROOT"] + "/fork.json"))["stream_id"])
PY
)"

export TENET_FORK_WORKSPACE="$(python3 - <<'PY'
import json, os
print(json.load(open(os.environ["TENET_TEST_ROOT"] + "/fork.json"))["workspace"])
PY
)"

cat "$TENET_FORK_WORKSPACE/state.txt"
```

预期：

```text
before snapshot
```

查看 lineage：

```bash
go run ./cmd/tenet task lineage \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --stream "$TENET_FORK_ID"
```

预期：

- 输出中包含父任务 id
- 输出中包含 fork 任务 id

## 8. Timer / Resume 测试

即时 resume：

```bash
go run ./cmd/tenet task resume \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --stream "$TENET_TASK_ID" \
  --note "manual resume"
```

延迟 resume：

```bash
go run ./cmd/tenet task resume \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --stream "$TENET_TASK_ID" \
  --after 500ms \
  --note "delayed manual resume" \
  --output json | tee "$TENET_TEST_ROOT/resume.json"
```

预期：

- JSON 中有 `scheduled.event_type = TaskResumeScheduled`
- JSON 中有 `fired.event_type = TimerFired`
- JSON 中有 `resumed.event_type = TaskResumed`

查看状态：

```bash
go run ./cmd/tenet task status \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --stream "$TENET_TASK_ID"
```

预期：

- `status: RUNNING` 或任务之前若已完成则状态可能被 resume 事件改为 `RUNNING`
- timeline 中包含 resumed / timer 信息

## 9. Projection Snapshot 测试

`task status` 会触发 Projection Engine 从 event log 生成视图，并在终态/间隔点持久化 projection snapshot。

检查 SQLite：

```bash
python3 - <<'PY'
import os, sqlite3
root = os.environ["TENET_TEST_ROOT"]
task_id = os.environ["TENET_TASK_ID"]
db = os.path.join(root, "tenet.db")
con = sqlite3.connect(db)
row = con.execute(
    "select stream_id, stream_seq, length(state_blob) from projection_snapshots where stream_id=?",
    (task_id,),
).fetchone()
print(row)
assert row is not None
assert row[2] > 20
PY
```

预期：

- 输出一行 projection snapshot 记录
- `state_blob` 长度大于 20

## 10. Python gRPC Worker 测试

如果还没有 Python venv：

```bash
cd /Users/hcy/Desktop/Tenet/python
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
```

启动 Python Worker：

```bash
cd /Users/hcy/Desktop/Tenet/python
. .venv/bin/activate
PYTHONPATH=. python -m tenet.grpc_worker \
  --port 50123 \
  --provider echo
```

另开一个终端，执行 Go gRPC 任务：

```bash
cd /Users/hcy/Desktop/Tenet/go
go run ./cmd/tenet task run \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --workspace "$TENET_TEST_ROOT/workspace" \
  --worker grpc \
  --worker-address 127.0.0.1:50123 \
  --workflow simple \
  --output json \
  "grpc manual smoke"
```

预期：

- 返回 `COMPLETED`
- result 类似 `Echo response for task ...`

结束 worker：

```bash
Ctrl-C
```

## 11. DeepSeek 测试

配置 API Key：

```bash
export DEEPSEEK_API_KEY="你的 key"
```

用 DeepSeek 直接跑本地 Go worker adapter：

```bash
cd /Users/hcy/Desktop/Tenet/go
go run ./cmd/tenet task run \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --workspace "$TENET_TEST_ROOT/workspace" \
  --worker deepseek \
  --workflow simple \
  --output json \
  "用一句话说明这个项目是什么"
```

预期：

- 网络和 key 正常时，返回 `COMPLETED`
- 如果 key 缺失，错误信息应提示 `DEEPSEEK_API_KEY`

也可以测试 Python Worker DeepSeek：

```bash
cd /Users/hcy/Desktop/Tenet/python
. .venv/bin/activate
PYTHONPATH=. DEEPSEEK_API_KEY="$DEEPSEEK_API_KEY" python -m tenet.grpc_worker \
  --port 50124 \
  --provider deepseek \
  --api-key-env DEEPSEEK_API_KEY
```

另开终端：

```bash
cd /Users/hcy/Desktop/Tenet/go
go run ./cmd/tenet task run \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --workspace "$TENET_TEST_ROOT/workspace" \
  --worker grpc \
  --worker-address 127.0.0.1:50124 \
  --workflow simple \
  --output json \
  "DeepSeek via Python gRPC"
```

## 12. HTTP API / SSE 测试

启动服务：

```bash
cd /Users/hcy/Desktop/Tenet/go
go run ./cmd/tenet serve \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --http-port 18080
```

另开终端测试 health：

```bash
curl -s http://127.0.0.1:18080/healthz
```

预期：

```json
{"status":"ok"}
```

查看 tasks：

```bash
curl -s http://127.0.0.1:18080/tasks
```

查看 SSE events：

```bash
curl -N "http://127.0.0.1:18080/events?stream_id=$TENET_TASK_ID&from=1"
```

预期：

- 能看到 `event:` / `data:` 形式的 SSE 输出
- 有历史事件或后续新事件

结束 serve：

```bash
Ctrl-C
```

## 13. Watch 实时事件测试

终端 A：

```bash
cd /Users/hcy/Desktop/Tenet/go
go run ./cmd/tenet task watch \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --stream "$TENET_TASK_ID" \
  --from 1
```

终端 B：

```bash
cd /Users/hcy/Desktop/Tenet/go
go run ./cmd/tenet task resume \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --stream "$TENET_TASK_ID" \
  --note "watch event"
```

预期：

- 终端 A 能看到 `TaskResumed`

## 14. 取消任务测试

```bash
go run ./cmd/tenet task cancel \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --stream "$TENET_TASK_ID" \
  --reason "manual cancel test"

go run ./cmd/tenet task status \
  --config "$TENET_TEST_ROOT/tenet.yaml" \
  --stream "$TENET_TASK_ID"
```

预期：

- status 变为 `FAILED`
- error/reason 包含 `manual cancel test`

## 15. 数据库人工检查

```bash
python3 - <<'PY'
import os, sqlite3
db = os.path.join(os.environ["TENET_TEST_ROOT"], "tenet.db")
con = sqlite3.connect(db)
for table in ["event_log", "snapshots", "projection_snapshots"]:
    count = con.execute(f"select count(*) from {table}").fetchone()[0]
    print(table, count)
PY
```

预期：

- `event_log` 大于 0
- `snapshots` 大于 0
- `projection_snapshots` 大于 0

## 16. 清理

```bash
echo "$TENET_TEST_ROOT"
rm -rf "$TENET_TEST_ROOT"
```

如果创建了 Python venv，可按需保留或删除：

```bash
rm -rf /Users/hcy/Desktop/Tenet/python/.venv
```

## 17. 常见问题

### Python 测试 skipped=1

默认 `python3` 没有安装 `grpcio`。进入 `python/.venv` 并执行：

```bash
pip install -e .
python -m unittest discover -s tests -p 'test_*.py'
```

### DeepSeek 报缺少 key

确认：

```bash
echo "$DEEPSEEK_API_KEY"
```

### gRPC worker 连不上

确认 worker 终端输出：

```text
tenet python worker ready
```

确认端口一致：

```bash
lsof -i :50123
```

### fork 后没有恢复 workspace

确认你传给 `task fork --seq` 的是 `workspace snapshot --output json` 返回的：

```text
snapshot.StreamSeq
```

如果 fork 的 seq 小于 snapshot seq，系统会认为 fork 点之前没有可用快照。

### `task status` 看不到 projection snapshot

先执行一次：

```bash
go run ./cmd/tenet task status --config "$TENET_TEST_ROOT/tenet.yaml" --stream "$TENET_TASK_ID"
```

然后再查 `projection_snapshots` 表。

## 18. 完整通过标准

你可以认为本轮全功能手测通过，当以下项目都满足：

- `make test` 通过
- `make smoke` 输出 `smoke ok`
- simple/react/dag/scientific/coding/interactive workflow 都能完成
- `task list/status/logs/watch` 可用
- workspace snapshot 可以 restore
- `task fork` 后子 workspace 文件内容是 fork 点快照内容
- `task lineage` 能显示父子链路
- `task resume --after` 产生 scheduled/fired/resumed 事件
- Projection snapshot 表有记录
- Python gRPC worker 能被 Go CLI 调用
- HTTP `/healthz`、`/tasks`、`/events` 可用
- DeepSeek 在有 key 时可返回正常结果
