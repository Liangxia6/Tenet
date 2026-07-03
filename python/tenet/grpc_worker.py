from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import time
from concurrent import futures
from dataclasses import asdict, is_dataclass
from typing import Any

try:
    import grpc
except ImportError as exc:  # pragma: no cover - exercised by CLI users without grpcio installed.
    raise SystemExit("tenet.grpc_worker requires grpcio. Install with: python -m pip install grpcio") from exc

from .worker import StatelessWorker
from .v1 import tenet_pb2, tenet_pb2_grpc


WORKER_SERVICE = "tenet.v1.TenetWorker"
ORCHESTRATOR_SERVICE = "tenet.v1.TenetOrchestrator"


def _loads(data: bytes) -> dict[str, Any]:
    if not data:
        return {}
    return json.loads(data.decode("utf-8"))


def _dumps(value: Any) -> bytes:
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


class TenetWorkerRPC:
    def __init__(self, worker: StatelessWorker) -> None:
        self.worker = worker

    def generate_thought(self, request: dict[str, Any], context: grpc.ServicerContext) -> dict[str, Any]:
        del context
        return asyncio.run(self.worker.generate_thought(request))

    def execute_tool(self, request: dict[str, Any], context: grpc.ServicerContext) -> dict[str, Any]:
        del context
        result = asyncio.run(self.worker.execute_tool(request))
        return asdict(result)

    def health_check(self, request: dict[str, Any], context: grpc.ServicerContext) -> dict[str, Any]:
        del request, context
        return asyncio.run(self.worker.health_check())


class TenetWorkerProtoServicer(tenet_pb2_grpc.TenetWorkerServicer):
    def __init__(self, worker: StatelessWorker) -> None:
        self.worker = worker

    def GenerateThought(self, request: tenet_pb2.GenerateThoughtRequest, context: grpc.ServicerContext) -> tenet_pb2.GenerateThoughtResponse:
        del context
        response = asyncio.run(self.worker.generate_thought({
            "session_id": request.session_id,
            "task_id": request.task_id,
            "model": request.model,
            "temperature": request.temperature,
            "system_prompt": request.system_prompt,
            "messages": [_message_to_dict(item) for item in request.messages],
            "tools": [_tool_definition_to_dict(item) for item in request.tools],
        }))
        return _dict_to_generate_thought_response(response)

    def ExecuteTool(self, request: tenet_pb2.ExecuteToolRequest, context: grpc.ServicerContext) -> tenet_pb2.ExecuteToolResponse:
        del context
        result = asyncio.run(self.worker.execute_tool({
            "session_id": request.session_id,
            "fencing_token": request.fencing_token,
            "workspace": request.workspace,
            "tool_name": request.tool_name,
            "arguments": request.arguments,
        }))
        return tenet_pb2.ExecuteToolResponse(
            stdout=result.stdout,
            stderr=result.stderr,
            exit_code=result.exit_code,
            is_error=result.is_error,
            duration_ms=result.duration_ms,
        )

    def HealthCheck(self, request: tenet_pb2.HealthCheckRequest, context: grpc.ServicerContext) -> tenet_pb2.HealthCheckResponse:
        del request, context
        response = asyncio.run(self.worker.health_check())
        return tenet_pb2.HealthCheckResponse(
            status=str(response.get("status", "")),
            worker_count=int(response.get("worker_count", 0)),
            uptime_seconds=int(response.get("uptime_seconds", 0)),
        )


def create_server(worker: StatelessWorker, max_workers: int = 10) -> grpc.Server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    tenet_pb2_grpc.add_TenetWorkerServicer_to_server(TenetWorkerProtoServicer(worker), server)
    return server


def register_agent(orchestrator_address: str, agent_id: str, listen_port: int, max_concurrency: int) -> dict[str, Any]:
    with grpc.insecure_channel(orchestrator_address) as channel:
        stub = tenet_pb2_grpc.TenetOrchestratorStub(channel)
        response = stub.RegisterAgent(
            tenet_pb2.RegisterAgentRequest(agent_id=agent_id, listen_port=listen_port, max_concurrency=max_concurrency),
            timeout=60,
        )
        return {"accepted": response.accepted, "orchestrator_id": response.orchestrator_id, "message": response.message}


def serve(
    *,
    port: int,
    workspace: str,
    orchestrator_address: str = "",
    agent_id: str = "",
    max_concurrency: int = 10,
    provider: str = "",
    model: str = "",
    base_url: str = "",
    api_key_env: str = "",
) -> None:
    configure_provider_env(provider=provider, model=model, base_url=base_url, api_key_env=api_key_env)
    worker = StatelessWorker(workspace=workspace)
    server = create_server(worker, max_workers=max_concurrency)
    bound = server.add_insecure_port(f"[::]:{port}")
    if bound == 0:
        raise RuntimeError(f"failed to bind worker port {port}")
    server.start()
    agent_id = agent_id or f"python-worker-{os.getpid()}"
    print(f"tenet python worker ready: agent_id={agent_id} port={port} workspace={workspace}", flush=True)
    if orchestrator_address:
        response = register_agent(orchestrator_address, agent_id, port, max_concurrency)
        print(f"registered with orchestrator: {response}", flush=True)

    stopped = False

    def stop(signum: int, frame: Any) -> None:
        nonlocal stopped
        del signum, frame
        if stopped:
            return
        stopped = True
        server.stop(grace=3)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        while not stopped:
            time.sleep(0.2)
    finally:
        server.stop(grace=0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a stateless Tenet Python gRPC worker.")
    parser.add_argument("--port", type=int, default=50052)
    parser.add_argument("--workspace", default=".")
    parser.add_argument("--orchestrator-address", default="")
    parser.add_argument("--agent-id", default="")
    parser.add_argument("--max-concurrency", type=int, default=10)
    parser.add_argument("--provider", default="", choices=["", "echo", "openai", "deepseek"])
    parser.add_argument("--model", default="")
    parser.add_argument("--base-url", default="")
    parser.add_argument("--api-key-env", default="")
    args = parser.parse_args(argv)
    serve(
        port=args.port,
        workspace=args.workspace,
        orchestrator_address=args.orchestrator_address,
        agent_id=args.agent_id,
        max_concurrency=args.max_concurrency,
        provider=args.provider,
        model=args.model,
        base_url=args.base_url,
        api_key_env=args.api_key_env,
    )
    return 0


def configure_provider_env(*, provider: str = "", model: str = "", base_url: str = "", api_key_env: str = "") -> None:
    provider = provider.strip().lower()
    if provider:
        os.environ["TENET_LLM_PROVIDER"] = provider
    if not provider:
        return
    if provider == "openai":
        if model:
            os.environ["TENET_OPENAI_MODEL"] = model
        if base_url:
            os.environ["TENET_OPENAI_BASE_URL"] = base_url
        if api_key_env:
            os.environ["TENET_OPENAI_API_KEY_ENV"] = api_key_env
    if provider == "deepseek":
        if model:
            os.environ["TENET_DEEPSEEK_MODEL"] = model
        if base_url:
            os.environ["TENET_DEEPSEEK_BASE_URL"] = base_url
        if api_key_env:
            os.environ["TENET_DEEPSEEK_API_KEY_ENV"] = api_key_env


def _message_to_dict(message: tenet_pb2.Message) -> dict[str, Any]:
    return {
        "role": message.role,
        "content": message.content,
        "tool_call_id": message.tool_call_id,
        "tool_calls": [_tool_call_to_dict(call) for call in message.tool_calls],
    }


def _tool_call_to_dict(call: tenet_pb2.ToolCall) -> dict[str, Any]:
    return {"call_id": call.call_id, "tool_name": call.tool_name, "arguments": call.arguments}


def _tool_definition_to_dict(tool: tenet_pb2.ToolDefinition) -> dict[str, Any]:
    return {"name": tool.name, "description": tool.description, "parameters_schema": tool.parameters_schema}


def _dict_to_generate_thought_response(response: dict[str, Any]) -> tenet_pb2.GenerateThoughtResponse:
    usage = response.get("usage") or {}
    return tenet_pb2.GenerateThoughtResponse(
        thought=str(response.get("thought", "")),
        tool_calls=[
            tenet_pb2.ToolCall(
                call_id=str(call.get("call_id", "")),
                tool_name=str(call.get("tool_name", "")),
                arguments=str(call.get("arguments", "")),
            )
            for call in response.get("tool_calls", [])
        ],
        is_final=bool(response.get("is_final", True)),
        finish_reason=str(response.get("finish_reason", "stop")),
        usage=tenet_pb2.TokenUsage(
            prompt_tokens=int(usage.get("prompt_tokens", 0)),
            completion_tokens=int(usage.get("completion_tokens", 0)),
            total_tokens=int(usage.get("total_tokens", 0)),
            cost_usd=float(usage.get("cost_usd", 0.0)),
        ),
        discovered_tools=[
            tenet_pb2.ToolDefinition(
                name=str(tool.get("name", "")),
                description=str(tool.get("description", "")),
                parameters_schema=str(tool.get("parameters_schema", "")),
            )
            for tool in response.get("discovered_tools", [])
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
