import { type ChildProcess, fork } from "node:child_process";
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { StateDatabase } from "../src/state/database.js";
import type { JsonValue } from "../src/state/json.js";
import { isRecord, WORKER_PROTOCOL_SCHEMA, type WorkerEvent } from "../src/worker/protocol.js";
import { makeStateDatabasePath, makeTempDir } from "./helpers.js";

const ORIGIN_SESSION_ID = "worker-protocol-test-session";
const PROTOCOL_TIMEOUT_MS = 20_000;

type Reply = { ok: true; result: unknown } | { ok: false; error: string };

class WorkerClient {
  private nextId = 0;
  private readonly replies = new Map<string, (reply: Reply) => void>();
  private readonly events: WorkerEvent[] = [];
  private readonly waiters: Array<{
    match: (event: WorkerEvent) => boolean;
    resolve: (event: WorkerEvent) => void;
    reject: (error: Error) => void;
    timer: NodeJS.Timeout;
  }> = [];
  private readonly stderr: string[] = [];
  private exited: Error | undefined;

  constructor(readonly child: ChildProcess) {
    child.on("message", (frame: unknown) => this.consume(frame));
    child.stderr?.on("data", (chunk: Buffer) => this.stderr.push(chunk.toString("utf8")));
    child.on("exit", (code) => {
      this.exited = new Error(`worker exited with code ${String(code)}: ${this.stderr.join("")}`);
      for (const settle of this.replies.values()) {
        settle({ ok: false, error: this.exited.message });
      }
      this.replies.clear();
      for (const waiter of [...this.waiters]) {
        clearTimeout(waiter.timer);
        waiter.reject(this.exited);
      }
      this.waiters.length = 0;
    });
  }

  private consume(frame: unknown): void {
    if (!isRecord(frame) || frame.schema !== WORKER_PROTOCOL_SCHEMA) return;
    if (frame.event !== undefined) {
      const event = frame.event as WorkerEvent;
      const index = this.waiters.findIndex((waiter) => waiter.match(event));
      if (index === -1) {
        this.events.push(event);
        return;
      }
      const waiter = this.waiters.splice(index, 1)[0];
      if (waiter === undefined) {
        this.events.push(event);
        return;
      }
      clearTimeout(waiter.timer);
      waiter.resolve(event);
      return;
    }
    const id = frame.id;
    if (typeof id !== "string") return;
    const settle = this.replies.get(id);
    if (settle === undefined) return;
    this.replies.delete(id);
    settle(
      frame.ok === true
        ? { ok: true, result: frame.result }
        : { ok: false, error: typeof frame.error === "string" ? frame.error : "unknown error" },
    );
  }

  request(message: Record<string, unknown>): Promise<unknown> {
    if (this.exited !== undefined) return Promise.reject(this.exited);
    const id = `req-${this.nextId++}`;
    return new Promise((resolve, reject) => {
      this.replies.set(id, (reply) => {
        if (reply.ok) resolve(reply.result);
        else reject(new Error(reply.error));
      });
      this.child.send({ schema: WORKER_PROTOCOL_SCHEMA, id, message });
    });
  }

  waitForEvent<E extends WorkerEvent>(
    match: (event: WorkerEvent) => event is E,
    label: string,
  ): Promise<E> {
    if (this.exited !== undefined) return Promise.reject(this.exited);
    const buffered = this.events.find(match);
    if (buffered !== undefined) {
      this.events.splice(this.events.indexOf(buffered), 1);
      return Promise.resolve(buffered);
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const index = this.waiters.findIndex((waiter) => waiter.timer === timer);
        if (index !== -1) this.waiters.splice(index, 1);
        reject(new Error(`timed out waiting for worker event: ${label}`));
      }, PROTOCOL_TIMEOUT_MS);
      this.waiters.push({
        match,
        resolve: (event) => resolve(event as E),
        reject,
        timer,
      });
    });
  }
}

let client: WorkerClient;
let databasePath: string;
let cwd: string;

const AGENT_FIXTURE = `import { agent, defineWorkflow } from "@osolmaz/pi-workflows";

export default defineWorkflow({
  name: "worker-agent",
  startAt: "ask",
  nodes: {
    ask: agent({
      prompt: ({ input }) => \`Answer the request.\n\nRequest: \${(input as { task: string }).task}\`,
      expectedOutput: '{ "reply": "text" }',
    }),
  },
  edges: [],
});
`;

const CHECKPOINT_FIXTURE = `import { checkpoint, compute, defineWorkflow } from "@osolmaz/pi-workflows";

export default defineWorkflow({
  name: "worker-checkpoint",
  startAt: "hold",
  nodes: {
    hold: checkpoint({ summary: "Needs review" }),
    done: compute({ run: ({ outputs }) => ({ reviewed: outputs.hold }) }),
  },
  edges: [{ from: "hold", to: "done" }],
});
`;

const DECISION_FIXTURE = `import {
  choice,
  compute,
  defineHumanChoices,
  defineWorkflow,
  humanDecision,
  humanDecisionEdge,
} from "@osolmaz/pi-workflows";

const choices = defineHumanChoices({
  continue: choice({ label: "Continue" }),
  stop: choice({ label: "Stop" }),
});

export default defineWorkflow({
  name: "worker-decision",
  startAt: "approve",
  nodes: {
    approve: humanDecision({
      audience: "operator",
      choices,
      request: () => ({
        title: "Continue?",
        subject: { note: "proceed" },
        presentation: { schema: "pi-workflows.decision-presentation.v1", summary: "Review.", blocks: [] },
      }),
    }),
    went: compute({ run: ({ outputs }) => ({ decision: outputs.approve }) }),
    halted: compute({ run: () => "halted" }),
  },
  edges: [
    humanDecisionEdge({ from: "approve", choices, cases: { continue: "went", stop: "halted" } }),
  ],
});
`;

const DECISION_TIMEOUT_FIXTURE = `import {
  choice,
  compute,
  defineHumanChoices,
  defineWorkflow,
  humanDecision,
  humanDecisionEdge,
} from "@osolmaz/pi-workflows";

const choices = defineHumanChoices({
  continue: choice({ label: "Continue" }),
  stop: choice({ label: "Stop" }),
});

export default defineWorkflow({
  name: "worker-decision-timeout",
  startAt: "approve",
  nodes: {
    approve: humanDecision({
      audience: "operator",
      choices,
      onTimeout: { afterMs: 250, response: { choice: "continue" } },
      request: () => ({
        title: "Continue?",
        subject: { note: "timeout" },
        presentation: { schema: "pi-workflows.decision-presentation.v1", summary: "Review.", blocks: [] },
      }),
    }),
    went: compute({ run: ({ outputs }) => ({ decision: outputs.approve }) }),
    halted: compute({ run: () => "halted" }),
  },
  edges: [
    humanDecisionEdge({ from: "approve", choices, cases: { continue: "went", stop: "halted" } }),
  ],
});
`;

const NOTIFY_FIXTURE = `import { compute, defineWorkflow, notify } from "@osolmaz/pi-workflows";

export default defineWorkflow({
  name: "worker-notify",
  startAt: "note",
  nodes: {
    note: notify({ message: () => "Build finished", kind: "progress" }),
    done: compute({ run: () => "notified" }),
  },
  edges: [{ from: "note", to: "done" }],
});
`;

const SETTINGS_FIXTURE = `import {
  agent,
  allowSettingsPath,
  defineWorkflow,
  workflowSettings,
} from "@osolmaz/pi-workflows";

export default defineWorkflow({
  name: "worker-settings",
  settings: workflowSettings({
    initial: { route: "a" },
    parse: (value) => value as { route: string },
    description: "Route choice.",
    paths: [allowSettingsPath("/route", { replace: ["session"] })],
  }),
  startAt: "ask",
  nodes: {
    ask: agent({ prompt: () => "Say hi.", expectedOutput: '{ "reply": "text" }' }),
  },
  edges: [],
});
`;

async function writeFixtures(): Promise<void> {
  const workflowsDir = path.join(cwd, ".pi", "workflows");
  await mkdir(workflowsDir, { recursive: true });
  const fixtures: Array<[string, string]> = [
    ["worker-agent.workflow.ts", AGENT_FIXTURE],
    ["worker-checkpoint.workflow.ts", CHECKPOINT_FIXTURE],
    ["worker-decision.workflow.ts", DECISION_FIXTURE],
    ["worker-decision-timeout.workflow.ts", DECISION_TIMEOUT_FIXTURE],
    ["worker-notify.workflow.ts", NOTIFY_FIXTURE],
    ["worker-settings.workflow.ts", SETTINGS_FIXTURE],
  ];
  await Promise.all(
    fixtures.map(([name, source]) => writeFile(path.join(workflowsDir, name), source, "utf8")),
  );
}

function forkWorker(): WorkerClient {
  const entry = path.resolve("dist/worker/worker-entry.js");
  if (!existsSync(entry)) {
    throw new Error(`worker entry is missing after build: ${entry}`);
  }
  const child = fork(entry, [], {
    env: {
      ...process.env,
      PI_WORKFLOWS_WORKER_CONFIG: JSON.stringify({
        databasePath,
        originSessionId: ORIGIN_SESSION_ID,
        cwd,
      }),
    },
    stdio: ["inherit", "inherit", "pipe", "ipc"],
  });
  return new WorkerClient(child);
}

async function startRun(workflow: string, input?: JsonValue): Promise<string> {
  const reply = (await client.request({
    type: "run.start",
    workflow,
    ...(input === undefined ? {} : { input }),
  })) as { runId: string };
  expect(typeof reply.runId).toBe("string");
  return reply.runId;
}

type RunFinishedEvent = Extract<WorkerEvent, { type: "run.finished" }>;

async function waitForRunFinished(runId: string): Promise<RunFinishedEvent> {
  return client.waitForEvent(
    (event): event is RunFinishedEvent => event.type === "run.finished" && event.runId === runId,
    `run.finished for ${runId}`,
  );
}

describe("worker IPC protocol", () => {
  beforeAll(async () => {
    execFileSync("npm", ["run", "build"], { stdio: "pipe" });
    databasePath = await makeStateDatabasePath("worker-protocol-ipc");
    cwd = await makeTempDir("worker-protocol-cwd");
    await writeFixtures();
    client = forkWorker();
    await client.request({ type: "run.list" });
  }, 240_000);

  afterAll(async () => {
    client.child.kill();
    await new Promise<void>((resolve) => {
      if (client.child.exitCode !== null || client.child.killed) {
        resolve();
        return;
      }
      client.child.on("exit", () => resolve());
    });
    await Promise.allSettled([
      rm(path.dirname(databasePath), { recursive: true, force: true }),
      rm(cwd, { recursive: true, force: true }),
    ]);
  });

  let agentRunId = "";
  let agentRequestId = "";
  let agentRunRevision = 0;

  it("starts a run and parks it at an agent step", async () => {
    agentRunId = await startRun("worker-agent", { task: "hello worker" });
    const started = await client.waitForEvent(
      (event): event is Extract<WorkerEvent, { type: "run.started" }> =>
        event.type === "run.started" && event.runId === agentRunId,
      "run.started",
    );
    expect(started.runId).toBe(agentRunId);
    const request = await client.waitForEvent(
      (event): event is Extract<WorkerEvent, { type: "agent.request" }> =>
        event.type === "agent.request" &&
        event.runId === agentRunId &&
        event.prompt.includes("hello worker"),
      "agent.request",
    );
    agentRequestId = request.requestId;
    expect(request.requestId).toMatch(/^request-[0-9a-f]+$/);
    expect(isRecord(request.contract)).toBe(true);
  });

  it("streams agent updates", async () => {
    const reply = (await client.request({
      type: "agent.update",
      requestId: agentRequestId,
      update: {
        type: "progress",
        key: "main",
        data: {
          schema: "pi-workflows.progress.v1",
          status: "running",
          phase: "thinking",
          completed: 1,
          total: 2,
          unit: "steps",
        },
      },
    })) as { runId: string };
    expect(reply.runId).toBe(agentRunId);
  });

  it("accepts an agent submission and finishes the run", async () => {
    const reply = (await client.request({
      type: "agent.submit",
      requestId: agentRequestId,
      submission: { output: { reply: "hello back" } },
    })) as { submissionId: string; status: string };
    expect(reply.submissionId).toMatch(/^submission-/);
    expect(reply.status).toBe("accepted");
    const finished = await waitForRunFinished(agentRunId);
    expect(finished.status).toBe("completed");
    expect(finished.finalOutput).toMatchObject({ reply: "hello back" });
    const summary = (await client.request({ type: "run.get", runId: agentRunId })) as {
      run: { status: string; revision: number };
    };
    expect(summary.run.status).toBe("completed");
    agentRunRevision = summary.run.revision;
  });

  it("answers a checkpoint over IPC", async () => {
    const runId = await startRun("worker-checkpoint");
    const request = await client.waitForEvent(
      (event): event is Extract<WorkerEvent, { type: "checkpoint.request" }> =>
        event.type === "checkpoint.request" && event.runId === runId,
      "checkpoint.request",
    );
    const reply = (await client.request({
      type: "checkpoint.answer",
      requestId: request.requestId,
      input: { approved: true },
    })) as { outcome: string };
    expect(reply.outcome).toBe("accepted");
    const finished = await waitForRunFinished(runId);
    expect(finished.status).toBe("completed");
  });

  it("answers a protected human decision over IPC", async () => {
    const runId = await startRun("worker-decision");
    const request = await client.waitForEvent(
      (event): event is Extract<WorkerEvent, { type: "decision.request" }> =>
        event.type === "decision.request" && event.runId === runId,
      "decision.request",
    );
    const reply = (await client.request({
      type: "decision.answer",
      requestId: request.requestId,
      response: { choice: "continue" },
    })) as { outcome: string };
    expect(reply.outcome).toBe("accepted");
    const finished = await waitForRunFinished(runId);
    expect(finished.status).toBe("completed");
  });

  it("resolves an expired decision default on resume", async () => {
    const runId = await startRun("worker-decision-timeout");
    await client.waitForEvent(
      (event): event is Extract<WorkerEvent, { type: "decision.request" }> =>
        event.type === "decision.request" && event.runId === runId,
      "decision.request",
    );
    await new Promise((resolve) => setTimeout(resolve, 350));
    const reply = (await client.request({ type: "run.resume", runId })) as {
      resumed: boolean;
    };
    expect(reply.resumed).toBe(true);
    const finished = await waitForRunFinished(runId);
    expect(finished.status).toBe("completed");
    const summary = (await client.request({ type: "run.get", runId })) as {
      run: { finalOutput: { decision: { choice: string } } };
    };
    expect(summary.run.finalOutput.decision.choice).toBe("continue");
  });

  it("delivers notifications and marks them sent", async () => {
    const runId = await startRun("worker-notify");
    const notification = await client.waitForEvent(
      (event): event is Extract<WorkerEvent, { type: "notification.request" }> =>
        event.type === "notification.request" && event.runId === runId,
      "notification.request",
    );
    const reply = (await client.request({
      type: "notification.delivered",
      notificationRequestId: notification.notificationRequestId,
      ok: true,
      piSessionEntryId: "entry-1",
    })) as { ok: boolean };
    expect(reply.ok).toBe(true);
    const finished = await waitForRunFinished(runId);
    expect(finished.status).toBe("completed");
    const state = new StateDatabase({ filePath: databasePath, mode: "read-only" });
    try {
      const row = state.connection
        .prepare("SELECT status, pi_session_entry_id FROM workflow_messages WHERE run_id = ?")
        .get(runId) as { status: string; pi_session_entry_id: string | null };
      expect(row.status).toBe("sent");
      expect(row.pi_session_entry_id).toBe("entry-1");
    } finally {
      state.close();
    }
  });

  let settingsRunId = "";

  it("patches live settings on a parked run", async () => {
    settingsRunId = await startRun("worker-settings");
    await client.waitForEvent(
      (event): event is Extract<WorkerEvent, { type: "agent.request" }> =>
        event.type === "agent.request" && event.runId === settingsRunId,
      "agent.request",
    );
    const reply = (await client.request({
      type: "settings.patch",
      runId: settingsRunId,
      patch: [{ op: "replace", path: "/route", value: "b" }],
    })) as { scopeId: string; changeNumber: number; adopted: boolean };
    expect(reply.changeNumber).toBe(1);
    expect(reply.adopted).toBe(false);
    expect(reply.scopeId).toMatch(/worker-settings/);

    const repeated = (await client.request({
      type: "settings.patch",
      runId: settingsRunId,
      patch: [{ op: "replace", path: "/route", value: "b" }],
    })) as { changeNumber: number; adopted: boolean };
    expect(repeated.adopted).toBe(true);
    expect(repeated.changeNumber).toBe(1);
  });

  it("pauses a parked run, resumes it, and re-emits the pending request", async () => {
    const pausedReply = (await client.request({
      type: "run.pause",
      runId: settingsRunId,
    })) as { paused: boolean };
    expect(pausedReply.paused).toBe(true);
    const summary = (await client.request({ type: "run.get", runId: settingsRunId })) as {
      run: { status: string; paused: boolean };
    };
    expect(summary.run.paused).toBe(true);

    const resumeReply = (await client.request({
      type: "run.resume",
      runId: settingsRunId,
    })) as { resumed: boolean; waitingForInteraction: boolean };
    expect(resumeReply.resumed).toBe(false);
    expect(resumeReply.waitingForInteraction).toBe(true);
    const reemitted = await client.waitForEvent(
      (event): event is Extract<WorkerEvent, { type: "agent.request" }> =>
        event.type === "agent.request" && event.runId === settingsRunId,
      "re-emitted agent request after resume",
    );
    expect(reemitted.requestId).toMatch(/^request-[0-9a-f]+$/);
  });

  it("cancels a parked run", async () => {
    const reply = (await client.request({
      type: "run.cancel",
      runId: settingsRunId,
    })) as { status: string };
    expect(reply.status).toBe("cancelled");
    const finished = await waitForRunFinished(settingsRunId);
    expect(finished.status).toBe("cancelled");
  });

  it("restarts a terminal run and adopts a repeat restart", async () => {
    const restartReply = (await client.request({
      type: "run.restart",
      runId: agentRunId,
      expectedRevision: agentRunRevision,
    })) as { runId: string; parentRunId: string };
    expect(restartReply.parentRunId).toBe(agentRunId);
    expect(restartReply.runId).not.toBe(agentRunId);
    await client.waitForEvent(
      (event): event is Extract<WorkerEvent, { type: "agent.request" }> =>
        event.type === "agent.request" && event.runId === restartReply.runId,
      "restarted run's agent.request",
    );

    const adoptedReply = (await client.request({
      type: "run.restart",
      runId: agentRunId,
      expectedRevision: agentRunRevision,
    })) as { runId: string; parentRunId: string; adopted: boolean };
    expect(adoptedReply.adopted).toBe(true);
    expect(adoptedReply.runId).toBe(restartReply.runId);
  });

  it("lists runs by status filter", async () => {
    const all = (await client.request({ type: "run.list" })) as {
      runs: Array<{ runId: string; originSessionId: string }>;
    };
    expect(all.runs.map((run) => run.runId)).toContain(agentRunId);
    expect(all.runs.every((run) => run.originSessionId === ORIGIN_SESSION_ID)).toBe(true);

    const waiting = (await client.request({ type: "run.list", status: "waiting" })) as {
      runs: Array<{ runId: string }>;
    };
    expect(waiting.runs.length).toBeGreaterThan(0);
    for (const run of waiting.runs) {
      const summary = (await client.request({ type: "run.get", runId: run.runId })) as {
        run: { status: string };
      };
      expect(summary.run.status).toBe("waiting");
    }
  });

  it("replies with an error for an unknown run", async () => {
    await expect(client.request({ type: "run.get", runId: "run-does-not-exist" })).rejects.toThrow(
      /not found/,
    );
  });
});
