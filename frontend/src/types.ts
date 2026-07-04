export type TaskStatus = "RUNNING" | "PAUSED" | "COMPLETED" | "FAILED";

export type Progress = {
  completed_steps: number;
  total_steps: number;
};

export type TokenState = {
  total_tokens: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_cost_usd: number;
  by_agent: Record<string, number>;
  by_model: Record<string, number>;
  budget_limit: number;
  budget_exceeded: boolean;
};

export type TimelineStep = {
  seq: number;
  type: string;
  content?: string;
  tool_name?: string;
  stdout?: string;
  stderr?: string;
  duration_ms?: number;
  timestamp: string;
};

export type TimelineState = {
  stream_id: string;
  steps: TimelineStep[];
  total_steps: number;
  duration_ms: number;
};

export type SubTaskState = {
  id: string;
  agent_role?: string;
  status: TaskStatus;
  result?: string;
};

export type TaskView = {
  stream_id: string;
  status: TaskStatus;
  query?: string;
  workspace?: string;
  workflow_type?: string;
  current_phase?: string;
  progress: Progress;
  subtasks?: SubTaskState[];
  final_answer?: string;
  error?: string;
  timeline: TimelineState;
  tokens: TokenState;
};

export type TaskListItem = {
  stream_id: string;
  status: TaskStatus;
  workflow?: string;
  latest_seq: number;
  last_event: string;
  query?: string;
  tokens?: number;
  phase?: string;
  updated_at?: string;
};

export type StreamEvent = {
  id: number;
  stream_id: string;
  stream_seq: number;
  event_type: string;
  payload: string;
  parent_id?: string;
  timestamp: string;
};

export type CreateTaskInput = {
  query: string;
  message?: string;
  workspace: string;
  workflow: string;
  worker: string;
  worker_address?: string;
  model?: string;
  base_url?: string;
  api_key_env?: string;
  api_key?: string;
  max_steps?: number;
  scheduled?: boolean;
};

export type CreateTaskResponse = {
  task_id: string;
  status: string;
  workflow: string;
  complexity_score: number;
  result: unknown;
};

export type ForkResponse = {
  stream_id: string;
  parent_id: string;
  fork_at_seq: number;
  workspace?: string;
  restored?: boolean;
  snapshot?: {
    StreamID: string;
    StreamSeq: number;
    Type: string;
    Ref: string;
  } | null;
};

export type LineageResponse = {
  stream_id: string;
  lineage: string[];
  children: string[];
};

export type WorkerInfo = {
  id: string;
  status: string;
  modes: string[];
  grpcPort: number;
};

export type TraceKind = "llm" | "tool" | "workflow" | "timer" | "workspace" | "fork" | "status";

export type TraceStep = {
  seq: number;
  kind: TraceKind;
  title: string;
  summary?: string;
  status?: "ok" | "error" | "running" | "paused";
  event: StreamEvent;
};
