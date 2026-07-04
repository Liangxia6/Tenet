import {
  Activity,
  AlertTriangle,
  Brain,
  CheckCircle2,
  ChevronRight,
  GitFork,
  Hammer,
  Menu,
  MessageSquarePlus,
  RefreshCw,
  Send,
  Settings,
  Square,
  Zap
} from "lucide-react";
import { FormEvent, useEffect, useMemo, useState } from "react";
import type React from "react";
import { API_BASE, api, eventSourceURL } from "./api";
import { buildTrace, parsePayload } from "./trace";
import type { CreateTaskInput, StreamEvent, TaskListItem, TaskStatus, TaskView, TraceStep } from "./types";

const workflows = ["auto", "simple", "react", "dag", "scientific", "coding", "interactive"];
const workers = ["echo", "openai", "deepseek", "grpc"];

type ChatMessage = {
  id: string;
  role: "user" | "assistant" | "tool" | "system";
  content: string;
  seq: number;
  eventType: string;
};

type InspectorTab = "trace" | "tools" | "events" | "tokens";

export function App() {
  const [tasks, setTasks] = useState<TaskListItem[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [task, setTask] = useState<TaskView | null>(null);
  const [events, setEvents] = useState<StreamEvent[]>([]);
  const [prompt, setPrompt] = useState("");
  const [worker, setWorker] = useState("echo");
  const [workflow, setWorkflow] = useState("auto");
  const [workspace, setWorkspace] = useState("/Users/hcy/Desktop/Tenet");
  const [model, setModel] = useState("");
  const [apiKey, setAPIKey] = useState(() => localStorage.getItem("tenet.deepseek_api_key") || "");
  const [workerAddress, setWorkerAddress] = useState("");
  const [inspectorTab, setInspectorTab] = useState<InspectorTab>("trace");
  const [sidebarWidth, setSidebarWidth] = useState(292);
  const [inspectorWidth, setInspectorWidth] = useState(420);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [sseState, setSseState] = useState("idle");

  async function refreshTasks() {
    const next = await api.tasks(100);
    setTasks(next);
    if (!selectedId && next.length > 0) setSelectedId(next[0].stream_id);
  }

  async function refreshSession(streamId = selectedId) {
    if (!streamId) {
      setTask(null);
      setEvents([]);
      return;
    }
    const [view, nextEvents] = await Promise.all([api.task(streamId), api.events(streamId, 1)]);
    setTask(view);
    setEvents(nextEvents);
    if (view.workspace) setWorkspace(view.workspace);
  }

  useEffect(() => {
    refreshTasks().catch((err) => setError(err.message));
  }, []);

  useEffect(() => {
    refreshSession(selectedId).catch((err) => setError(err.message));
  }, [selectedId]);

  useEffect(() => {
    if (!selectedId) return;
    const from = events.length > 0 ? Math.max(...events.map((event) => event.stream_seq)) + 1 : 1;
    const source = new EventSource(eventSourceURL(selectedId, from));
    const handle = (message: MessageEvent) => {
      try {
        const event = JSON.parse(message.data) as StreamEvent;
        setEvents((current) => mergeEvent(current, event));
        api.task(selectedId).then(setTask).catch(() => undefined);
        refreshTasks().catch(() => undefined);
        setSseState("connected");
      } catch {
        setSseState("error");
      }
    };
    setSseState("connected");
    source.onmessage = handle;
    for (const eventType of tenetEventTypes) source.addEventListener(eventType, handle);
    source.onerror = () => setSseState("reconnecting");
    return () => source.close();
  }, [selectedId, events.length]);

  const messages = useMemo(() => buildChatMessages(events), [events]);
  const trace = useMemo(() => buildTrace(events), [events]);
  const selectedTask = tasks.find((item) => item.stream_id === selectedId);

  function newChat() {
    setSelectedId("");
    setTask(null);
    setEvents([]);
    setPrompt("");
    setWorkflow("auto");
  }

  async function submitPrompt(event?: FormEvent) {
    event?.preventDefault();
    const content = prompt.trim();
    if (!content) return;
    setLoading(true);
    setError("");
    try {
      const input: CreateTaskInput = {
        query: content,
        message: content,
        workspace,
        workflow,
        worker,
        worker_address: workerAddress,
        model,
        api_key: apiKey,
        max_steps: 8
      };
      if ((worker === "deepseek" || worker === "openai") && apiKey) {
        localStorage.setItem("tenet.deepseek_api_key", apiKey);
      }
      const result = selectedId ? await api.sendMessage(selectedId, input) : await api.createTask(input);
      setPrompt("");
      await refreshTasks();
      setSelectedId(result.task_id);
      await refreshSession(result.task_id);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  async function cancelTask() {
    if (!selectedId) return;
    setLoading(true);
    try {
      await api.cancel(selectedId, "cancelled from console");
      await refreshSession();
      await refreshTasks();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  async function forkLatest() {
    if (!selectedId || events.length === 0) return;
    const seq = Math.max(...events.map((event) => event.stream_seq));
    setLoading(true);
    try {
      const fork = await api.fork(selectedId, seq, prompt || "forked session", true);
      await refreshTasks();
      setSelectedId(fork.stream_id);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  function beginResize(kind: "sidebar" | "inspector") {
    return (event: React.MouseEvent) => {
      event.preventDefault();
      const startX = event.clientX;
      const startSidebar = sidebarWidth;
      const startInspector = inspectorWidth;
      const move = (moveEvent: MouseEvent) => {
        const delta = moveEvent.clientX - startX;
        if (kind === "sidebar") {
          setSidebarWidth(Math.max(220, Math.min(420, startSidebar + delta)));
        } else {
          setInspectorWidth(Math.max(320, Math.min(680, startInspector - delta)));
        }
      };
      const up = () => {
        window.removeEventListener("mousemove", move);
        window.removeEventListener("mouseup", up);
      };
      window.addEventListener("mousemove", move);
      window.addEventListener("mouseup", up);
    };
  }

  return (
    <div className="chatShell" style={{ gridTemplateColumns: `${sidebarWidth}px 6px minmax(0, 1fr)` }}>
      <aside className="sessionSidebar">
        <div className="sideHeader">
          <div className="brandCompact"><span>T</span><strong>Tenet</strong></div>
          <button className="iconButton" title="Refresh sessions" onClick={() => refreshTasks().catch((err) => setError(err.message))}><RefreshCw size={17} /></button>
        </div>
        <button className="newChatButton" onClick={newChat}><MessageSquarePlus size={17} /> New session</button>
        <div className="sessionList">
          {tasks.map((item) => (
            <button key={item.stream_id} className={`sessionItem ${item.stream_id === selectedId ? "active" : ""}`} onClick={() => setSelectedId(item.stream_id)}>
              <span className="sessionTitle">{item.query || item.stream_id}</span>
              <span className="sessionMeta"><StatusBadge status={item.status} /> {item.workflow || "workflow"} · seq {item.latest_seq}</span>
            </button>
          ))}
          {tasks.length === 0 && <div className="empty">No sessions yet.</div>}
        </div>
        <div className="sideFooter">
          <div className={`connection ${sseState}`}>{sseState}</div>
          <span className="mono">{API_BASE}</span>
        </div>
      </aside>
      <div className="resizeHandle vertical" title="Resize sessions" onMouseDown={beginResize("sidebar")} />

      <main className="chatMain">
        <header className="chatTopbar">
          <div>
            <div className="eyebrow">{selectedId ? "Session" : "New Session"}</div>
            <h1>{selectedTask?.query || task?.query || "Ask Tenet to work on a task"}</h1>
            {selectedId && <div className="mono idLine">{selectedId}</div>}
          </div>
          <div className="topActions">
            {task && <StatusBadge status={task.status} />}
            <button className="secondaryButton" disabled={!selectedId || loading} onClick={forkLatest}><GitFork size={16} />Fork</button>
            <button className="dangerButton" disabled={!selectedId || loading} onClick={cancelTask}><Square size={16} />Cancel</button>
          </div>
        </header>

        {error && <div className="errorBanner"><AlertTriangle size={16} />{error}</div>}

        <section className="chatBody" style={{ gridTemplateColumns: `minmax(0, 1fr) 6px ${inspectorWidth}px` }}>
          <div className="messageStream">
            {messages.length === 0 ? (
              <div className="welcome">
                <Brain size={42} />
                <h2>Start a Tenet session</h2>
                <p>输入提示词，选择 worker 和 workflow。一个 Task 会作为一个可持续对话的 session，后续消息会继续追加到同一个事件流。</p>
              </div>
            ) : (
              messages.map((message) => <ChatBubble key={message.id} message={message} />)
            )}
          </div>
          <div className="resizeHandle inspectorHandle" title="Resize inspector" onMouseDown={beginResize("inspector")} />
          <Inspector tab={inspectorTab} setTab={setInspectorTab} trace={trace} events={events} task={task} />
        </section>

        <form className="chatComposer" onSubmit={submitPrompt}>
          <textarea
            value={prompt}
            onChange={(event) => setPrompt(event.target.value)}
            placeholder={selectedId ? "继续这个 session，输入下一条提示词..." : "输入提示词开始一个新 Agent session..."}
            onKeyDown={(event) => {
              if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) submitPrompt();
            }}
          />
          <div className="composerControls">
            <button className="primaryButton sendButton" disabled={loading || !prompt.trim()}><Send size={17} />{loading ? "Working..." : selectedId ? "Send" : "Start"}</button>
            <label>Worker<select value={worker} onChange={(event) => setWorker(event.target.value)}>{workers.map((item) => <option key={item}>{item}</option>)}</select></label>
            <label>Workflow<select value={workflow} onChange={(event) => setWorkflow(event.target.value)}>{workflows.map((item) => <option key={item}>{item}</option>)}</select></label>
            <label>Workspace<input value={workspace} onChange={(event) => setWorkspace(event.target.value)} /></label>
            <label>Model<input value={model} onChange={(event) => setModel(event.target.value)} placeholder={worker === "deepseek" ? "deepseek-chat" : "optional"} /></label>
            {worker === "grpc" && <label>gRPC<input value={workerAddress} onChange={(event) => setWorkerAddress(event.target.value)} placeholder="127.0.0.1:50052" /></label>}
            {(worker === "deepseek" || worker === "openai") && <label className="apiKeyField">API Key<input type="password" value={apiKey} onChange={(event) => setAPIKey(event.target.value)} placeholder={`${worker} API key`} /></label>}
          </div>
          <div className="composerHint">Press Cmd/Ctrl + Enter to send. 当前 Task 会像 session 一样保留事件、Trace、工具和上下文。</div>
        </form>
      </main>
    </div>
  );
}

function ChatBubble({ message }: { message: ChatMessage }) {
  return (
    <article className={`chatBubble ${message.role}`}>
      <div className="avatar">{message.role === "user" ? "U" : message.role === "tool" ? <Hammer size={16} /> : "T"}</div>
      <div className="bubbleBody">
        <div className="bubbleMeta"><strong>{message.role === "user" ? "You" : message.role === "tool" ? "Tool" : "Tenet"}</strong><span className="mono">#{message.seq} {message.eventType}</span></div>
        <div className="bubbleText">{message.content}</div>
      </div>
    </article>
  );
}

function Inspector({
  tab,
  setTab,
  trace,
  events,
  task
}: {
  tab: InspectorTab;
  setTab: (tab: InspectorTab) => void;
  trace: TraceStep[];
  events: StreamEvent[];
  task: TaskView | null;
}) {
  const tools = events.filter((event) => event.event_type === "ToolExecuted");
  return (
    <aside className="agentInspector">
      <div className="inspectorTabs">
        {(["trace", "tools", "events", "tokens"] as InspectorTab[]).map((item) => (
          <button key={item} className={tab === item ? "active" : ""} onClick={() => setTab(item)}>{item}</button>
        ))}
      </div>
      {tab === "trace" && <TracePanel trace={trace} />}
      {tab === "tools" && <ToolPanel events={tools} />}
      {tab === "events" && <EventPanel events={events} />}
      {tab === "tokens" && <TokenPanel task={task} events={events} />}
    </aside>
  );
}

function TracePanel({ trace }: { trace: TraceStep[] }) {
  return (
    <div className="inspectorScroll">
      {trace.map((step) => (
        <details key={`${step.event.stream_id}:${step.seq}`} className="traceLine">
          <summary><span className={`dot ${step.kind}`} /><span className="mono">#{step.seq}</span><strong>{step.title}</strong></summary>
          <p>{step.summary || "No summary"}</p>
          <pre>{JSON.stringify(parsePayload(step.event), null, 2)}</pre>
        </details>
      ))}
    </div>
  );
}

function ToolPanel({ events }: { events: StreamEvent[] }) {
  return (
    <div className="inspectorScroll">
      {events.length === 0 && <div className="empty">No tool calls yet.</div>}
      {events.map((event) => <EventBlock key={`${event.stream_id}:${event.stream_seq}`} event={event} />)}
    </div>
  );
}

function EventPanel({ events }: { events: StreamEvent[] }) {
  return (
    <div className="inspectorScroll">
      {events.map((event) => <EventBlock key={`${event.stream_id}:${event.stream_seq}`} event={event} />)}
    </div>
  );
}

function TokenPanel({ task, events }: { task: TaskView | null; events: StreamEvent[] }) {
  const llmCount = events.filter((event) => event.event_type === "GenerateThought").length;
  return (
    <div className="inspectorScroll">
      <div className="metricGrid">
        <Metric icon={<Brain size={16} />} label="LLM calls" value={String(llmCount)} />
        <Metric icon={<Zap size={16} />} label="Tokens" value={String(task?.tokens.total_tokens || 0)} />
        <Metric icon={<CheckCircle2 size={16} />} label="Budget" value={String(task?.tokens.budget_limit || 0)} />
        <Metric icon={<Activity size={16} />} label="Events" value={String(events.length)} />
      </div>
    </div>
  );
}

function EventBlock({ event }: { event: StreamEvent }) {
  return (
    <details className="eventBlock">
      <summary><span className="mono">#{event.stream_seq}</span><strong>{event.event_type}</strong><ChevronRight size={14} /></summary>
      <pre>{JSON.stringify(parsePayload(event), null, 2)}</pre>
    </details>
  );
}

function Metric({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return <div className="metricCard">{icon}<span>{label}</span><strong>{value}</strong></div>;
}

function StatusBadge({ status }: { status: TaskStatus }) {
  return <span className={`status ${status.toLowerCase()}`}>{status}</span>;
}

function buildChatMessages(events: StreamEvent[]): ChatMessage[] {
  const messages: ChatMessage[] = [];
  for (const event of events) {
    const payload = parsePayload(event);
    if (event.event_type === "TaskCreated") {
      const content = stringValue(payload.query);
      if (content) messages.push({ id: `${event.stream_seq}:user-created`, role: "user", content, seq: event.stream_seq, eventType: event.event_type });
    }
    if (event.event_type === "UserMessage") {
      const content = stringValue(payload.content);
      if (content) messages.push({ id: `${event.stream_seq}:user`, role: "user", content, seq: event.stream_seq, eventType: event.event_type });
    }
    if (event.event_type === "TaskCompleted") {
      const content = stringValue(payload.final_answer || payload.result);
      if (content) messages.push({ id: `${event.stream_seq}:assistant`, role: "assistant", content, seq: event.stream_seq, eventType: event.event_type });
    }
    if (event.event_type === "ToolExecuted") {
      const result = payload.result && typeof payload.result === "object" ? payload.result as Record<string, unknown> : {};
      const toolName = stringValue(payload.tool_name || result.tool_name || result.ToolName || "tool");
      const stdout = stringValue(result.stdout || result.Stdout);
      const stderr = stringValue(result.stderr || result.Stderr);
      messages.push({ id: `${event.stream_seq}:tool`, role: "tool", content: `${toolName}${stdout ? `\n\n${stdout}` : ""}${stderr ? `\n\nstderr:\n${stderr}` : ""}`, seq: event.stream_seq, eventType: event.event_type });
    }
  }
  return messages;
}

function stringValue(value: unknown) {
  if (typeof value === "string") return value;
  if (value == null) return "";
  return JSON.stringify(value);
}

function mergeEvent(events: StreamEvent[], event: StreamEvent) {
  const key = `${event.stream_id}:${event.stream_seq}`;
  if (events.some((item) => `${item.stream_id}:${item.stream_seq}` === key)) return events;
  return [...events, event].sort((a, b) => a.stream_seq - b.stream_seq);
}

const tenetEventTypes = [
  "TaskCreated",
  "UserMessage",
  "ComplexityAnalyzed",
  "TaskStarted",
  "GenerateThought",
  "ToolsDiscovered",
  "ToolExecuted",
  "TokenUsed",
  "TaskDecomposed",
  "SubTaskDispatched",
  "SubTaskCompleted",
  "CodingPhaseStarted",
  "CodingPhaseCompleted",
  "CodingSnapshotCreated",
  "CodingPhaseSkipped",
  "WaitingForHumanInput",
  "TimerScheduled",
  "TimerFired",
  "TaskResumeScheduled",
  "TaskResumed",
  "WorkspaceSnapshot",
  "ForkCreated",
  "ForkWorkspaceInitialized",
  "ForkWorkspaceRestored",
  "ForkWorkspaceRestoreFailed",
  "TaskCompleted",
  "TaskFailed",
  "TaskCancelled"
];
