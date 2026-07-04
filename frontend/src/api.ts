import type {
  CreateTaskInput,
  CreateTaskResponse,
  ForkResponse,
  LineageResponse,
  StreamEvent,
  TaskListItem,
  TaskView,
  WorkerInfo
} from "./types";

const configuredBase = import.meta.env.VITE_TENET_API_URL as string | undefined;
export const API_BASE = configuredBase?.replace(/\/$/, "") || "/api";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {})
    }
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

export const api = {
  health: () => request<{ status: string; version: string }>("/healthz"),
  config: () => request<Record<string, unknown>>("/config"),
  workers: () => request<WorkerInfo[]>("/workers"),
  tasks: (limit = 50) => request<TaskListItem[]>(`/tasks?limit=${limit}`),
  createTask: (input: CreateTaskInput) =>
    request<CreateTaskResponse>("/tasks", {
      method: "POST",
      body: JSON.stringify(input)
    }),
  sendMessage: (streamId: string, input: CreateTaskInput) =>
    request<CreateTaskResponse>(`/tasks/${encodeURIComponent(streamId)}/messages`, {
      method: "POST",
      body: JSON.stringify(input)
    }),
  task: (streamId: string) => request<TaskView>(`/tasks/${encodeURIComponent(streamId)}`),
  events: (streamId: string, from = 1) =>
    request<StreamEvent[]>(`/tasks/${encodeURIComponent(streamId)}/events?from=${from}`),
  cancel: (streamId: string, reason: string) =>
    request<StreamEvent>(`/tasks/${encodeURIComponent(streamId)}/cancel`, {
      method: "POST",
      body: JSON.stringify({ reason })
    }),
  resume: (streamId: string, note: string, after?: string) =>
    request<unknown>(`/tasks/${encodeURIComponent(streamId)}/resume`, {
      method: "POST",
      body: JSON.stringify({ note, after })
    }),
  fork: (streamId: string, seq: number, query: string, restoreWorkspace = true) =>
    request<ForkResponse>(`/tasks/${encodeURIComponent(streamId)}/fork`, {
      method: "POST",
      body: JSON.stringify({ seq, query, restore_workspace: restoreWorkspace })
    }),
  lineage: (streamId: string) => request<LineageResponse>(`/tasks/${encodeURIComponent(streamId)}/lineage`)
};

export function eventSourceURL(streamId: string, from: number) {
  if (API_BASE.startsWith("http")) {
    return `${API_BASE}/events?stream_id=${encodeURIComponent(streamId)}&from=${from}`;
  }
  return `/events?stream_id=${encodeURIComponent(streamId)}&from=${from}`;
}
