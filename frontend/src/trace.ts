import type { StreamEvent, TraceKind, TraceStep } from "./types";

export function parsePayload(event: StreamEvent): Record<string, unknown> {
  try {
    return JSON.parse(event.payload || "{}") as Record<string, unknown>;
  } catch {
    return { raw: event.payload };
  }
}

export function eventSummary(event: StreamEvent) {
  const payload = parsePayload(event);
  const pick = (...keys: string[]) => {
    for (const key of keys) {
      const value = payload[key];
      if (typeof value === "string" && value.trim()) return value;
      if (typeof value === "number") return String(value);
      if (typeof value === "boolean") return String(value);
    }
    return "";
  };
  switch (event.event_type) {
    case "TaskStarted":
    case "TaskCreated":
      return pick("query", "workflow_type", "workspace");
    case "GenerateThought":
      return summarizeThought(payload);
    case "ToolExecuted":
      return pick("tool_name") || summarizeNested(payload, "result", ["stdout", "Stdout", "stderr", "Stderr"]);
    case "TokenUsed":
      return `${pick("total_tokens")} tokens`;
    case "SubTaskDispatched":
    case "SubTaskCompleted":
      return pick("subtask_id", "agent_role", "result");
    case "TaskCompleted":
      return pick("final_answer", "result");
    case "TaskFailed":
    case "TaskCancelled":
      return pick("error", "reason");
    case "WorkspaceSnapshot":
      return pick("snapshot_type", "snapshot_ref", "workspace");
    case "ForkWorkspaceRestored":
    case "ForkWorkspaceInitialized":
      return pick("workspace", "snapshot_ref", "reason");
    case "TimerScheduled":
    case "TimerFired":
      return pick("timer_id", "delay_ms");
    case "TaskResumed":
      return pick("note");
    default:
      return pick("phase", "reason", "selected_workflow", "query");
  }
}

export function buildTrace(events: StreamEvent[]): TraceStep[] {
  return events.map((event) => ({
    seq: event.stream_seq,
    kind: kindForEvent(event.event_type),
    title: titleForEvent(event.event_type),
    summary: eventSummary(event),
    status: statusForEvent(event.event_type),
    event
  }));
}

export function kindForEvent(eventType: string): TraceKind {
  if (eventType.includes("Tool")) return "tool";
  if (eventType === "GenerateThought" || eventType === "TokenUsed") return "llm";
  if (eventType.includes("Timer") || eventType.includes("Resume")) return "timer";
  if (eventType.includes("Workspace") || eventType.includes("Snapshot")) return "workspace";
  if (eventType.includes("Fork")) return "fork";
  if (eventType.includes("TaskCompleted") || eventType.includes("TaskFailed") || eventType.includes("Cancelled")) return "status";
  return "workflow";
}

function titleForEvent(eventType: string) {
  return eventType.replace(/([a-z])([A-Z])/g, "$1 $2");
}

function statusForEvent(eventType: string): TraceStep["status"] {
  if (eventType.includes("Failed") || eventType.includes("Cancelled")) return "error";
  if (eventType.includes("Paused") || eventType.includes("Waiting")) return "paused";
  if (eventType.includes("Started") || eventType.includes("Scheduled")) return "running";
  return "ok";
}

function summarizeThought(payload: Record<string, unknown>) {
  const result = payload.result;
  if (typeof result === "string") return result.slice(0, 160);
  if (result && typeof result === "object") {
    const record = result as Record<string, unknown>;
    const thought = record.thought || record.Thought;
    if (typeof thought === "string") return thought.slice(0, 160);
  }
  return "";
}

function summarizeNested(payload: Record<string, unknown>, key: string, nestedKeys: string[]) {
  const value = payload[key];
  if (!value || typeof value !== "object") return "";
  const record = value as Record<string, unknown>;
  for (const nestedKey of nestedKeys) {
    const nested = record[nestedKey];
    if (typeof nested === "string" && nested.trim()) return nested.slice(0, 160);
  }
  return "";
}
