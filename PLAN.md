# Plan: Localize pi-workflows — collapse the distributed service into an isolated durable worker

## Goal

Turn `pi-workflows` from a **durable distributed workflow service** (server + child runners over a
socket, with claims/leases/epochs/fencing/viewer projections) into a **local Pi extension backed by
one isolated workflow worker process that owns SQLite directly**.

**No workflow-language behavior changes.** Preserve the semantics of `*.workflow.ts` and the
Pi-native workflow experience listed below. Server-specific operational behavior — multi-process
ownership, external decision channels, remote viewing, follow-up queues — is intentionally removed.

**Preserved (must keep working exactly as today):**

- The DSL: node types (`agent`, `compute`, `action`, `shell`, `checkpoint`, `decision`,
  `humanDecision`), edges/routing, composition (`includeWorkflow`, named exits), control loops,
  `maxSteps`, node timeouts, `$result.outcome` routing.
- Checkpoints (park + resume in the same run), human decisions (in-Pi presentation, `/workflow
answer`, `onTimeout` default policy), live settings via JSON Patch, updates/notifications.
- Run control: pause/resume/cancel/restart (restart = new run from original input).
- The two Pi-experience recovery behaviors: post-workflow model turn, and bounded
  missing-submission reminders for agent steps.
- Effect idempotency semantics (repeated key + same request adopts; uncertain manual effect →
  ambiguous).
- **Durable resume:** kill Pi → restart → resume the same checkpoint/run (now provided by the
  worker's SQLite, not the server).

**Intentionally removed (server-specific operational behavior):**

- Multi-process ownership machinery: claims, leases, epochs, fencing tokens, runner supervision,
  process handoff/recovery, the `pi-workflows.client.v1` socket protocol.
- External decision channels (Telegram) and remote viewing (`piw`, Herdr, WebSocket relay).
- Follow-up queues (`queue-follow-up` / `remove-follow-up` / post-completion prompts).
- The built-in workflow catalog (`src/builtins/`) — a product change; user `*.workflow.ts` files
  still load.

---

## Architecture: invert the runner — let the worker own SQLite directly

### Why upstream got so large

Upstream promises all of these simultaneously: multiple processes, server survives clients, runners
survive/recover, concurrent clients, exactly-one-owner, durable commands, process handoff, external
viewers, Telegram decisions, resource controllers, crash recovery, idempotent effects, session
replay. That genuinely requires claims, leases, fencing tokens, revisions, ownership, and recovery
logic. It is a **durable distributed workflow service** — valid, but far beyond the requirement here.

The tell is the runner protocol: it does **per-store-operation RPC**. `src/server/workflow-runner-protocol.ts`
exposes `store.initializeRun`, `store.prepareRunResume`, `store.readRunState`, `store.commitTransition`,
`store.publishUpdate`, `store.findSettingsScope`, `store.reserveEffect`, `store.settleEffect`, plus
`interaction.*` and `notification.*` — and every message carries server-era coordination fields
(`generation`, `runnerEpoch`, `expectedRevision`, `claimLost`). Persistence is on the **wrong side of
the boundary**: the runner is subordinate to a central server that owns persistence/authority.

### The inversion

Collapse that into: **one Pi session, one isolated workflow worker, one durable SQLite store, one
tiny semantic message protocol.** The worker owns SQLite directly — no `store.*` RPC crosses the
boundary. The protocol describes **workflow interactions**, not the engine's persistence API.

```text
Pi extension (owns the Pi session)
     │
     │  tiny semantic protocol (Node IPC channel via child_process.fork)
     ▼
Workflow worker (isolated process)
     ├── WorkflowEngine
     ├── SqliteWorkflowStore   ← owns SQLite directly
     ├── workflow loader (jiti)
     └── effect / checkpoint / settings state
```

The worker is spawned by the Pi extension and accesses the existing workflow SQLite database
directly. Database scoping and durable workflow persistence semantics remain unchanged from the
existing implementation; only the server/RPC layer between the engine and store is removed.

The engine's three real seams are preserved and are what make this work:

- **`WorkflowEngine`** — execution.
- **`WorkflowExecutionStore`** (`src/workflows/store.ts:184-233`) — persistence/state. The worker
  implements it with SQLite; the engine never knows or cares.
- **`AgentStepExecutor`** (`src/workflows/types.ts:900-910`) — agent execution. In the worker this is
  a **parking executor** (see below).

### The key insight: the worker's executor _parks_; the extension _presents_

An agent step does NOT reach Pi from the worker. The current `InteractiveExecutor`
(`src/server/workflow-runner-entry.ts:207`) proves the pattern: on a fresh agent step it records the
pending interaction, then `throw new RunParkedError()`. The engine parks the run. The **extension**
— which owns the Pi session — presents the step to the model (a custom message of type
`WORKFLOW_AGENT_STEP_MESSAGE_TYPE` + a model turn) and later sends `agent.submit` back over the
protocol. The worker's executor never touches Pi; it records the pending interaction through the
small `InteractionStore` seam (see below) and parks. Checkpoints and human decisions use the same
park-and-resume pattern, so the protocol is uniform.

This is what lets us throw away the distributed machinery **without** throwing away durability or
execution isolation: the worker is a separate process (isolation), owns SQLite (durable resume), and
talks to Pi only through the small interaction protocol.

### The tiny semantic protocol

The protocol is an **envelope with request IDs and explicit replies**, not bare commands + events.
Once two operations can be outstanding, the extension needs to know which reply belongs to which
request. Every host message carries an `id`; the worker answers it with a `reply` (or `error`) that
names that `id`. Unsolicited worker→host messages are `event`s. This keeps `worker-adapter.ts` able
to offer a straightforward `await adapter.request(...)` without recreating `WorkflowClient`.

Host → worker (only things that genuinely cross the Pi/worker boundary):

```ts
type HostMessage =
  | { id: string; type: "run.start"; workflow: string; input: unknown }
  | { id: string; type: "run.list"; status?: "active" | "waiting" | "paused" }
  | { id: string; type: "run.get"; runId: string }
  | { id: string; type: "run.resume"; runId: string }
  | { id: string; type: "run.pause"; runId: string }
  | { id: string; type: "run.cancel"; runId: string }
  | { id: string; type: "run.restart"; runId: string; expectedRevision: number }
  | { id: string; type: "agent.submit"; requestId: string; output: unknown }
  | { id: string; type: "agent.update"; requestId: string; update: WorkflowUpdateInput }
  | {
      id: string;
      type: "notification.delivered";
      notificationRequestId: string;
      ok: boolean;
      error?: string;
    }
  | { id: string; type: "checkpoint.answer"; requestId: string; input: unknown }
  | { id: string; type: "decision.answer"; requestId: string; response: unknown }
  | { id: string; type: "settings.patch"; runId: string; patch: JsonPatch };
```

Worker → host:

```ts
type WorkerEvent =
  | { type: "run.started"; runId: string; state: WorkflowRunState }
  | { type: "run.changed"; runId: string; state: WorkflowRunState }
  | { type: "agent.request"; requestId: string; contract: AgentStepContract; prompt: string }
  | { type: "checkpoint.request"; requestId: string; request: CheckpointRequest }
  | { type: "decision.request"; requestId: string; request: HumanDecisionRequest }
  | {
      type: "notification.request";
      notificationRequestId: string;
      notification: WorkflowNotificationRequest;
    }
  | { type: "run.finished"; runId: string; state: WorkflowRunState };

type WorkerMessage =
  | { type: "reply"; replyTo: string; result: unknown } // correlated to a host message id
  | { type: "error"; replyTo?: string; message: string } // with replyTo → that request failed
  | { type: "event"; event: WorkerEvent }; // unsolicited worker→host
```

Notes:

- `reply.result` is the operation's return value (e.g. `{ runId }` for `run.start`, the settled
  receipt for an answer, the new settings scope for `settings.patch`). `run.list` returns a summary
  array of durable runs (id, workflow, status, updatedAt); `run.get` returns the full
  `WorkflowRunState` for one run.
- An `error` with a `replyTo` rejects that specific outstanding request; an `error` without one is a
  fatal worker-level failure (the adapter tears down the worker).
- **Recovery discovery:** on startup, the worker opens SQLite but does not automatically execute
  nonterminal runs. The extension can query durable runs with `run.list` / `run.get` and explicitly
  resume one via `run.resume`. Resuming a parked run reconstructs and re-emits its pending
  agent/checkpoint/decision interaction as needed (the parking executor re-issues the request from
  the store, so the extension re-presents it). This is what makes "kill Pi + worker → restart →
  resume the same run" work without any server-side recovery sweep.
- No `store.*` operations, no `generation`/`runnerEpoch`/`claimLost` fields. `expectedRevision`
  stays only where it is a real optimistic-concurrency guard on a user action (restart, settings
  patch), not a per-store-op fencing token.
- The adapter's public surface is therefore small and request/reply-shaped:
  `await adapter.request({ type: "run.start", ... })`, plus an `onEvent(...)` subscription for the
  unsolicited `WorkerEvent`s that drive the widget and agent-step presentation.

**Transport: Node's dedicated IPC channel, not stdout.** Workflow code (compute, action, shell,
dependency, or debugging) can call `console.log`, so stdout is an unsafe framing channel — a stray
log line would corrupt the protocol stream. Use `child_process.fork()` with an IPC channel:

```ts
// extension/worker-adapter.ts
const worker = fork(workerEntry, [], {
  stdio: ["inherit", "inherit", "inherit", "ipc"], // stdout/stderr stay ordinary logs
});
worker.send(message); // host → worker (a HostMessage with an id)
worker.on("message", (msg: WorkerMessage) => {
  if (msg.type === "reply") pending.get(msg.replyTo)?.resolve(msg.result);
  else if (msg.type === "error" && msg.replyTo) pending.get(msg.replyTo)?.reject(msg);
  else if (msg.type === "error")
    fatal(msg); // no replyTo → tear down the worker
  else eventListeners.forEach((l) => l(msg.event)); // unsolicited WorkerEvent
});
// request(): mint an id, register a pending promise, send { id, ... }, await the reply.
```

```ts
// worker/worker-entry.ts
process.on("message", async (msg: HostMessage) => {
  try {
    const result = await dispatch(msg); // run.start / agent.submit / ...
    process.send({ type: "reply", replyTo: msg.id, result });
  } catch (error) {
    process.send({ type: "error", replyTo: msg.id, message: errorMessage(error) });
  }
});
// engine onRunStarted/onEvent + parking executor + notification sink emit
// process.send({ type: "event", event }) — unsolicited, no replyTo.
```

stdout/stderr remain ordinary logs (inherited, visible in the terminal); the semantic protocol gets
its own channel. `fork` also gives the worker the same Node runtime and module resolution as the
extension, so it can load `*.workflow.ts` via jiti without a separate install.

### What SQLite does (and doesn't) do

SQLite becomes completely internal to the worker:

```text
WorkflowEngine → WorkflowExecutionStore → SQLite
```

It provides: durable run state, outputs/results, resume after Pi restarts, checkpoints waiting across
restarts, human decisions, settings, effect/idempotency receipts, and trace/update history.

It does **not** contain: server leases, server commands, runner epochs, socket clients, viewer
projections, coordinator ownership, remote session message queues, process registries. This
dramatically reduces both the DB schema (`src/state/schema.ts`, 805 lines) and the surrounding code.

### Optional layering (directories, not necessarily npm packages)

The architectural separation is the useful part; separate packages are optional:

```text
core/     WorkflowEngine, DSL, WorkflowExecutionStore interface, AgentStepExecutor interface
sqlite/   SqliteWorkflowStore
worker/   worker entry + tiny protocol
extension/ worker adapter, Pi agent-step presentation, widget
```

This makes everything optional. You could run `new WorkflowEngine({ store: new MemoryWorkflowStore(),
executor: someExecutor })` with **no worker and no SQLite** (e.g. for tests), or use the full
`Pi → worker → WorkflowEngine → SqliteWorkflowStore` path for isolation + durable resume.

---

## What to keep (preserve semantics)

### `src/workflows/` — the engine — KEEP as a derived minimal set, decouple from server-era state

The keep list is what the worker path actually uses, not the current file inventory (server-era
files are listed under "What to remove"):

- `engine.ts` (1,942), `transitions.ts`, `graph.ts`, `composition.ts`, `control-loop.ts`,
  `loader.ts`, `definition.ts`, `types.ts`, `schema.ts`, `errors.ts`, `diagnostics.ts`
- Node types: `agent`, `compute`, `action`, `shell` (`shell.ts`), `checkpoint`, `decision`
  (`decision.ts`), `human-decision.ts` + `decision-presentation.ts`
- `settings.ts` + `json-patch.ts`, `updates.ts`, `requests.ts`, `progress.ts`, `text.ts`,
  `tool-input.ts`, `workflow-message-content.ts`
- **Strip server-era imports from kept files (Phase 4):** `requests.ts` and
  `workflow-message-content.ts` import `state/workflow-messages.js`; `human-decision.ts` imports
  `state/viewer.js`. Those state modules are removed, so the imports and any code paths that exist
  only for them must go with them or the tree will not compile.
- **`store.ts` (5,192) — becomes the worker's `SqliteWorkflowStore`.** The engine depends only on the
  `WorkflowExecutionStore` interface (`store.ts:184-233`). The concrete `WorkflowRunStore` class
  (lines 496-4219) is kept as the SQLite implementation but **stripped of server-era coordination**:
  no lease/token/generation fencing, no viewer projections, no session message queue. It keeps the
  semantic tables (runs, node attempts, outputs, checkpoints, decisions, settings, effects, updates,
  trace).

### `src/extension/` — Pi-native integration (5,390 lines) — KEEP, rewire to the worker adapter

- `widget.ts` (505), `message-card.ts`, `step-message.ts`, `terminal-message.ts`,
  `session-view.ts` (287) — widget + message rendering. `step-message.ts` already has the
  agent-step presentation (`WORKFLOW_AGENT_STEP_MESSAGE_TYPE`, model-turn trigger) — reused as-is.
- `workflow-tool.ts` / `tool-input.ts` — the model-facing `workflow` tool.
- `shortcuts.ts`, `index.ts` (1,628) — `/workflow` command + session lifecycle.
- New: **`worker-adapter.ts`** (`fork`s the worker with an IPC channel, supervises it, speaks the
  tiny protocol via `worker.send` / `worker.on("message")`), and the agent-step presentation is
  driven by `agent.request` messages instead of `watchSession`.
- `recorder.ts`, `session-events.ts`, `session-delivery.ts`, `session-run-adapter.ts`,
  `workflow-message-coordinator.ts` — trim to what the worker path needs.

### `src/render/` — KEEP only two files

- `render/format.ts` (19) and `render/node-type.ts` (28) — used by `widget.ts:4-5`.
- Remove `graph.ts`, `graph-render.ts` (1,076), `canvas.ts` (248), `ansi.ts` (47) — only used by
  `src/viewer/`.

### New: `src/worker/` — the isolated worker process

- **`worker-entry.ts`** — process entry. Loads workflows via `loader.ts` (jiti), owns a map of active
  `WorkflowEngine` instances. Constructs one `SqliteWorkflowStore(dbPath)` and passes it to both the
  engine (as `WorkflowExecutionStore`) and the parking executor (as `InteractionStore`):
  ```ts
  const store = new SqliteWorkflowStore(dbPath);
  const engine = new WorkflowEngine({
    store, // WorkflowExecutionStore
    executor: new ParkingAgentExecutor(store), // InteractionStore
    notificationSink: workerNotificationSink,
    onRunStarted,
    onEvent,
  });
  ```
  Speaks the tiny protocol over Node's dedicated IPC channel.
- **`parking-executor.ts`** — implements `AgentStepExecutor` (the `InteractiveExecutor` pattern),
  depending only on `InteractionStore`: fresh step → `interactionStore.requestInteraction(...)` +
  `throw new RunParkedError()`; resumed-with-persisted-candidate → read it back, validate +
  `request.accept(...)`, then `acceptInteraction(...)` or `rejectInteraction(...)` + re-park. Sets
  `preservesActiveTimeBudget = true` and `assistantMessageMode = "visible"`.
- **`worker-notification-sink.ts`** — implements `WorkflowNotificationSink`; emits a
  `notification.request` event, awaits the correlated `notification.delivered` ack, and returns the
  receipt (see seam 6).

**Worker startup context:** the worker is spawned by the Pi extension and inherits the Pi process
working directory. It opens the **same** database the server used today, resolved exactly as today:
`options.filePath ?? workflowStatePath(homeDir)` — i.e. `~/.pi/agent/workflows/<db>` unless an
explicit path is passed (`state/database.ts:61-70`). That is one shared state DB for all sessions,
not a per-session or per-project file; the worker merely opens it directly instead of over RPC. No
worker IDs, project IDs, registries, routing tables, leases, ownership records, or project-scoping
protocol are introduced.

---

## What to remove

### `src/server/` (12,611 lines) — REMOVE entirely

`server.ts`, `server-entry.ts`, `rpc-bridge.ts`, `rpc-executor.ts`, `lock.ts`, `processes.ts`,
`recovery.ts`, `state.ts`, `view.ts`, and all runner supervision (`workflow-runner-*`,
`resource-runner-*`, `child-runner-supervisor.ts`, `channel-supervisor.ts`, `channel-effects.ts`,
`resolver-entry.ts`). All claim/lease/generation/epoch fencing, wake recovery, retention sweeps.
The `InteractiveExecutor` pattern is **re-implemented** as the worker's `parking-executor.ts` (it is
small and self-contained), not carried over.

### `src/client/` (2,082 lines) — REMOVE entirely

`client.ts` (1,109), `protocol.ts`, `view.ts`, `resolver.ts`, `materialize.ts`, `activity.ts`,
`index.ts`. The `pi-workflows.client.v1` socket transport goes away. Replaced by the extension's
`worker-adapter.ts` speaking the tiny protocol to the worker.

### `src/herdr/` (742) + `src/viewer/` (1,148) + `plugins/herdr` + `tui/` — REMOVE

External TUI viewer, `piw` CLI, Herdr pane-placement adapter, loopback WebSocket relay.
`package.json`: drop `bin.pi-workflows`, the `./client` and `./resource-managers` exports, and
`plugins/herdr` + `herdr-plugin.toml` from `files`.

### `src/resource-managers/` (2,493 lines) — REMOVE entirely

The Kubernetes-style controller runtime. Entirely a multi-process durable-service feature. Remove the
`./resource-managers` export and `/resource-manager` command.

### `src/channels/` (1,184 lines) — REMOVE entirely

Telegram decision-channel adapters + supervised children. Human decisions keep their in-Pi
presentation and `/workflow answer` path; only the external channel transport is removed.

### `src/builtins/` (10,162 lines) — REMOVE entirely

The named built-in workflows and helpers. Product content, not engine infrastructure. Remove the
loader's optional `catalog` param in place (`loader.ts:104`; alpha policy — no compatibility shim):
the worker loads user `*.workflow.ts` files directly. Remove the one extension import
(`BUILTIN_WORKFLOW_METADATA`, `index.ts:4`) and the two server imports (deleted with the server). A
deliberate product change.

### `src/state/` (4,121 lines) — REDUCE to the worker's semantic store support

- **Keep (trimmed):** `json.ts` (48), `database.ts` (389, trimmed to open/verify the worker DB),
  `mutation.ts` (438, trimmed to revision checks only — no lease/token/generation fencing).
- **Remove:** `viewer.ts` (356), `prune.ts` (839, retention sweeps), `workflow-messages.ts` (861,
  server-owned Pi message queue → a minimal `notifications` table for the notify node plus the
  interaction protocol; see seam 6),
  `project-store.ts` (218).
- **`attempt-time.ts` (124) — re-home into the worker, do NOT delete.** See "Active-time accounting."
- `schema.ts` (805) shrinks to the semantic tables the worker store uses.

### `src/workflows/` server-era files — REMOVE (kept by inertia; verified dead in the target world)

- `queue.ts` (2,129) — the run-launch/reservation queue ("one active run per Pi session"). Only
  used by `src/server/` + the index re-export; imports `project-store`, `viewer`, and
  `workflow-messages`, all removed above. The worker owns runs directly; there is no cross-process
  launch queue.
- `catalog.ts` (154) — built-in catalog registration; only used by `loader.ts` +
  `src/builtins/`. Goes with the loader's catalog param.
- `prompt-evidence.ts` (344) + `command-batch.ts` (254) — consumers are only built-ins
  (autoimplement, sanity-check, change-verification) + index re-exports.
- `session-reducer.ts` (330) — the `session_entries` journal; sole external consumer is
  `src/server/view.ts` (viewer projection). Remove it and the `session_entries` table with its
  store queries.

### Repo artifacts outside `src/` — REMOVE or reduce (base-repo authority shipping with the package)

- `skills/` — delete `autodoc`, `autoimplement`, `autoplan`, `monitor`, `sanity-check` (each
  requires a built-in workflow that no longer exists). Rewrite only `pi-workflows` for the lean
  tool surface.
- `examples/` — keep only generic DSL examples (`echo`, `branch`, `shell`, `two-turn`,
  `live-settings`, `human-decision`); delete `resource-managers/` and the built-in-specific
  workflows (autoimplement, autoplan, sanity-check, plain-summary, autoresearch, approved-plan).
- `fixtures/layout/` (graph-layout fixtures for the removed `render/graph`) and
  `fixtures/session-events/` (journal fixtures) — delete both.
- `schemas/` — the ten human-decision JSON schemas are referenced by no code; delete.
- `protocol/` — spec + fixtures for the removed `pi-workflows.client.v1` socket protocol; delete.
- `scripts/` — delete `export-layout-fixtures.mjs`, `generate-viewer-benchmark.ts`,
  `snapshot-api.mjs`; keep `prepare.mjs`; reduce `live-e2e*.mjs` to at most one lean live check
  (or drop it).
- `package.json` — also drop the `./builtins` export, the `@earendil-works/pi-tui`
  devDependency, and `skills`/`examples`/`schemas`/`protocol` from `files` as their contents go.
- `vitest.config.ts` — rewrite minimal: remove the resource-managers alias, the coverage excludes
  for removed dirs, and the 85% coverage thresholds. No coverage gate in the lean repo.
- `README.md` — rewrite as "a lean framework for configuring workflows."
- `check-baseline.log`, `e2e-baseline.log` at the repo root — delete.
- `docs/MONITOR.md` — documents the removed monitor built-in; delete, not keep.

---

## The four seams + the interaction paths (verified against the code)

The engine and the parking executor depend on **two small store interfaces**, not the concrete
class. `SqliteWorkflowStore` implements both:

```text
WorkflowEngine
    → WorkflowExecutionStore ┐
                             ├→ SqliteWorkflowStore  (owns SQLite directly)
ParkingAgentExecutor         │
    → InteractionStore ──────┘
```

### 1. `WorkflowExecutionStore` (`src/workflows/store.ts:184-233`)

The engine talks to persistence only through this interface. The worker's `SqliteWorkflowStore`
implements it and owns SQLite directly. No engine call sites change; no `store.*` RPC crosses the
boundary.

### 2. `InteractionStore` (new small interface — the parking executor's seam)

The engine persists a parked agent step via `commitTransition` (a `WorkflowExecutionStore` method),
so resume knows to re-run the node. But the _pending interaction record_ that the origin session
finds and answers is a separate concern. Today it lives only on the server's RPC-backed store
(`requestInteraction` / `acceptInteraction` / `rejectInteraction`,
`src/server/workflow-runner-store.ts:160-184`) — **not** on `WorkflowExecutionStore`. If the parking
executor called those directly, it would depend on the giant concrete (or server RPC) store and
reintroduce the coupling we are removing.

So introduce a small interface the executor depends on:

```ts
interface InteractionStore {
  requestInteraction(options: {
    attemptId: string;
    kind: "agent" | "assistant" | "decision";
    contract: JsonValue;
  }): Promise<void>;
  readInteraction(attemptId: string): Promise<StoredInteraction | undefined>;
  submitInteraction(options: {
    requestId: string;
    submissionId: string;
    value: JsonValue;
  }): Promise<void>;
  acceptInteraction(options: {
    requestId: string;
    submissionId: string;
    attemptId: string;
    value: JsonValue;
  }): Promise<void>;
  rejectInteraction(options: {
    requestId: string;
    submissionId: string;
    attemptId: string;
    error: string;
  }): Promise<void>;
}
```

`SqliteWorkflowStore` implements it (backed by a small `interactions` table). The executor never
touches the concrete store or any server RPC.

The record moves through **`pending → validating → accepted | rejected`**, mirroring the current
server flow, where `beginInteractionValidation` persists the candidate with status "validating"
_before_ the run resumes (`server.ts:2669+`) and the resumed runner re-bootstraps with that
candidate (`workflow-runner-entry.ts:50,365`). Consequences for the worker:

- The `agent.submit` handler calls `submitInteraction(...)` **before** `engine.resumeRun(...)`, so
  a worker crash between submission and validation re-bootstraps with the persisted candidate
  instead of silently losing the submission. Idempotent on `(requestId, submissionId)`.
- The resumed parking executor reads the candidate back via `readInteraction(attemptId)`, validates,
  and records `acceptInteraction(...)` or `rejectInteraction(...)`; rejection returns the record to
  `pending` so the model can correct.

### 3. `AgentStepExecutor` (`src/workflows/types.ts:900-910`) — a _parking_ executor in the worker

The engine delegates agent steps via `runAgentStep(...)` (`engine.ts:1350`). The worker's
`parking-executor.ts` (the `InteractiveExecutor` pattern, `workflow-runner-entry.ts:207`) depends on
`InteractionStore`, not the concrete store:

- Fresh step → `interactionStore.requestInteraction({ attemptId, kind, contract })` then
  `throw new RunParkedError()`. The engine parks; the worker emits `agent.request` to the host.
- `agent.submit` → the worker persists the candidate via `submitInteraction(...)` **before**
  resuming (crash-safe; see seam 2), then `engine.resumeRun(...)`.
- Resumed with a persisted candidate → the executor reads it back, validates via
  `request.accept(output)` (assistant steps use the assistant validation), and records
  `acceptInteraction(...)` or `rejectInteraction(...)` + re-park so the model can correct.

The **extension** presents the step to the model (reusing `step-message.ts`) and sends
`agent.submit` / `agent.update`. **Missing-submission reminders** live in the extension's
worker-adapter (bounded in-process counter, no coordinator epoch).

**`agent.update` while parked:** there is no live `AgentStepRequest.publishUpdate` after
`RunParkedError` unwinds — the engine's `publishUpdate` throws unless the attempt is its active
attempt (`engine.ts:162-175`). The worker resolves the durable interaction to its run/attempt and
calls the store's semantic operation directly: `store.publishUpdate(runId, nodeId, attemptId,
update)` (`store.ts:1561`), whose check already accepts a parked attempt
(`state.currentNode ?? state.waitingOn === nodeId`). No ephemeral callback is retained in memory.

### 4. Checkpoint answers

The engine reads a checkpoint answer from `store.readCheckpoint(runId, attemptId)` on resume
(`engine.ts:1162`). Answering a checkpoint is: settle the checkpoint in the worker's store, then
resume the parked run via `engine.resumeRun(...)`. The extension sends `checkpoint.answer`; the
worker settles + resumes.

### 5. Human decisions — preserve validation/settlement, invoke locally

Do NOT collapse these to "write the settled request." Human decisions carry verification and
settlement semantics that must be preserved, now invoked in the worker instead of the server:

- Response validation: `validateHumanDecisionResponse` / `validateHumanDecisionSubmission`
  (`src/workflows/human-decision.ts:240,286`) — choice/audience/input validation.
- Request-integrity checks: `validateHumanDecisionRequestIntegrity`
  (`src/workflows/decision-presentation.ts:56`).
- Timeout / default policy: `defaultResponse` + `expiresAt` (auto-settle to the default choice when
  the deadline passes with no accepted answer).

The extension sends `decision.answer`; the worker runs these same functions, settles in its store,
then resumes. The settlement logic is unchanged — only its caller moves from `src/server/server.ts`
into the worker.

### 6. Notifications (`notify` node) — request/ack, not fire-and-forget

The engine requires a `WorkflowNotificationSink` when a workflow uses `notify` (`engine.ts:1137`),
and the sink contract is **request/receipt**: `notify(request): MaybePromise<WorkflowNotificationReceipt>`
(`types.ts:929-930`). The notify node's output **is the receipt** — `const receipt = await
this.notificationSink.notify(...); return { output: receipt }` (`engine.ts:1148-1156`) — so a
fire-and-forget event would change durable state.

Verified current durability semantics (all must be preserved):

- The server creates a **durable `workflowMessages` record with status `pending` before returning
  the receipt** (`server.ts:4581-4630`; insert at `state/workflow-messages.ts:199-200`). The
  receipt `{ notificationId, targetSessionId }` (with
  `notificationId = notification-${runId}-${attemptId}-${index}`) is assembled after that write.
- Delivery to the origin Pi session happens afterward; on delivery the record transitions
  `pending → sent` with a `pi_session_entry_id` (`state/workflow-messages.ts:497-501`).
- A `pending` record survives restarts and stays outstanding — the store's visibility queries treat
  pending messages as work Pi still owes (`state/workflow-messages.ts:20-80`). A failed or missing
  delivery therefore never loses the notification.

In the worker this becomes a minimal durable record plus a tiny request/ack interaction:

```text
worker → extension   event { type: "notification.request", notificationRequestId, notification }
extension → worker   { id, type: "notification.delivered", notificationRequestId, ok, error? }
```

- The worker's SQLite gains a small `notifications` table (notificationId, runId, attemptId,
  notificationIndex, kind, content, targetSessionId, status `pending | delivered`, timestamps) —
  the minimal durable state that preserves the behavior above. It is **not** the general
  `workflowMessages` queue: step/decision/terminal presentation moves to the interaction protocol;
  only notifications keep a durable delivery record.
- Sink flow: persist the record as `pending` → emit `notification.request` → await the correlated
  ack → on `ok` mark it `delivered` and return the receipt (computed exactly as today — both values
  are already known to the worker, so the ack confirms delivery only). On ack error or timeout the
  record stays `pending` and the sink **still returns the receipt**: a failed Pi delivery never
  fails the node (today's "receipt = enqueued" semantics).
- Redelivery: when a run is resumed, the worker re-emits `notification.request` for every `pending`
  record of that run. Delivery is at-least-once, so the extension dedupes by `notificationId`.
- No RPC. The 861-line `src/state/workflow-messages.ts` subsystem (viewer projections, ordering,
  turn tracking) is removed; only the pending/delivered record above carries over.

### 7. Active-time accounting (verified — do not delete `attempt-time.ts` wholesale)

This interacts with node timeout / pause / resume semantics, so it is **semantic**, not a server
artifact. Verified facts:

- The engine reads accumulated active time and subtracts it from the node timeout on resume:
  `timeoutMs = persistedTimeoutMs - elapsedMs` (`engine.ts:1009`). It never samples active time
  itself.
- Live sampling (start/sample/stop/recover of monotonic intervals) lives in the server today
  (`src/server/state.ts:520-610`), writing to the SQLite `attempt_active_intervals` table
  (`src/state/attempt-time.ts`).
- On resume, the store computes `activeElapsedMs = SUM(elapsed_ms)` per attempt (`store.ts:3806`) →
  `state.currentNodeElapsedMs` (`store.ts:3632`) → engine subtracts it from the timeout.
- Gated by `AgentStepExecutor.preservesActiveTimeBudget` (`types.ts:907`); only agent steps whose
  executor sets it to `true` get a persisted budget.

**In the worker:** the sampling loop moves from `src/server/state.ts` into the worker (it owns the
process lifetime and the SQLite table). The worker's parking executor sets
`preservesActiveTimeBudget = true`. Behavior is unchanged: an agent step's timeout budget excludes
parked/waiting time and survives pause/resume. `attempt-time.ts` is kept (trimmed); the server's
sampling calls are re-homed into the worker.

---

## Phases (ordered so behavior is verified before the risky store/worker swap)

The ordering keeps the existing SQLite `WorkflowRunStore` in place through Phases 1–2, so all
workflow behavior is verified **while persistence is unchanged**. The worker + stripped store are
introduced last.

### Phase 0 — Baseline (no code change)

- Record a baseline with the lean gate only: `npm run typecheck && npm run build &&
npm run test`. The old pre-finish regime (`npm run check` with its 85% coverage threshold, the
  e2e baseline ritual, API-surface snapshot diffing) is base-repo authority and does not carry
  over.
- **Triage the pre-existing failures first.** The clean tree already carries ~17 failing test
  files, so "verify green" has no working gate today. For each: fix it, delete it (if it tests
  doomed code), or document it as known-failing with a reason. Later phases' "verify green" steps
  are only meaningful against a triaged baseline.

### Phase 1 — Build the worker (additive; existing store unchanged)

Create `src/worker/`:

- **`worker-entry.ts`** — process entry, speaks the tiny protocol over Node's dedicated IPC channel
  (`process.send` / `process.on("message")`), owns active engines. Constructs engines with the **existing** `WorkflowRunStore` (SQLite) for now:
  ```ts
  const engine = new WorkflowEngine({
    executor: parkingExecutor,
    notificationSink: workerNotificationSink,
    store: new WorkflowRunStore(dbPath), // existing SQLite store for now
    onRunStarted: (_runId, state) => emit("run.started", { runId, state }),
    onEvent: (_event, state) => emit("run.changed", { runId: state.runId, state }),
  });
  ```
- **`parking-executor.ts`** — the `InteractiveExecutor` pattern (fresh → `requestInteraction` +
  park; resumed → validate + accept). Emits `agent.request` to the host on park.
- **`worker-notification-sink.ts`** — emits `notification.request`, awaits the correlated
  `notification.delivered` ack, returns the receipt (see seam 6).

The dependency direction from "Architectural constraint" applies to the new code (`src/worker →
src/workflows, src/state`; no Pi imports). No slophammer or other boundary tooling — the constraint
is enforced by review until the old layers are gone. Do NOT touch `src/client/` or `src/server/`
yet; the old path still compiles.

### Phase 2 — Rewire the extension to the worker (existing store unchanged)

Create `src/extension/worker-adapter.ts` (`fork` the worker with an IPC channel, supervise it,
correlate request IDs to replies, and expose `await adapter.request(...)` + `adapter.onEvent(...)`).
In `src/extension/index.ts`:

- Replace `new WorkflowClient(...)` + `ensureAvailable()` (lines 183, 690, 887, 954, 1440) with the
  worker-adapter.
- `executeCommand` (line 897) and the `workflow` tool (`toolInputToCommand`, line 1363) call
  `await adapter.request(...)` instead of `client.request(...)`. Each mints an `id`, awaits the
  correlated `reply`, and rejects on a matching `error`:
  - `pause`/`resume`/`cancel`/`restart` → `run.pause` / `run.resume` / `run.cancel` / `run.restart`.
  - `submit`/`update` → `agent.submit` / `agent.update`.
  - `answer` (checkpoint) → `checkpoint.answer`; (human decision) → `decision.answer`.
  - `status` → from the latest `run.changed` event state.
- The widget subscribes via `adapter.onEvent(...)` to `run.started` / `run.changed` instead of
  `client.watchSession(...)`.
- **Agent-step presentation:** on `agent.request`, present via `step-message.ts` + trigger a model
  turn; on the model's `workflow submit`, send `agent.submit`. **Missing-submission reminders** live
  here (bounded, no coordinator epoch).
- **Post-workflow model turn:** on `run.finished`, the extension gives the model its turn (direct,
  not server-scheduled).
- **Rewire the entangled extension tests to the worker adapter in this phase** (not Phase 5):
  `extension.test.ts` (2,046), `workflow-message-coordinator.test.ts` (988), and
  `session-run-adapter.test.ts` (210) test surviving extension code but import `src/client/` (the
  coordinator test also imports `state/workflow-messages`, removed in Phase 4). They cannot survive
  Phase 3 as-is.

**Verify all workflow behavior here while persistence is unchanged.** This proves the integration is
behavior-preserving before we strip the store.

### Phase 3 — Delete the client/server stack + builtins (verify green)

Delete, in dependency order (leaves first):

1. `src/channels/` + its extension wiring (`/workflow-channel`).
2. `src/resource-managers/` + `/resource-manager` command + export.
3. `src/herdr/`, `src/viewer/`, `plugins/herdr`, `tui/`, `herdr-plugin.toml`.
4. `src/render/graph.ts`, `graph-render.ts`, `canvas.ts`, `ansi.ts` (keep `format.ts`,
   `node-type.ts`).
5. `src/client/`.
6. `src/server/`.
7. `src/builtins/` + the extension's `BUILTIN_WORKFLOW_METADATA` import (`index.ts:4`) and its use
   (line 1356).

The server/client/builtins **test files were already deleted on the `deletions/barebones` branch**
(pulled forward from this phase), so Phase 3 is now mostly pure source deletion. What remains of
the test work:

- Delete the doomed-subsystem test files orphaned by steps 1–3: herdr, viewer, resource-manager,
  and channel tests.
- **Migrate `test/e2e/workflow.e2e.test.ts` (1,488) to the worker path in this phase.** It imports
  `src/client/` directly (`WorkflowClient`, `clientSocketPath`); after Phase 2 it already exercises
  the worker through the extension, so only its direct client usage needs replacing. Delete
  `test/e2e/package-resources.e2e.test.ts` with the client. Without this step Phase 3 lands with a
  broken e2e and no integration coverage until Phase 5.
- **Re-home the temp-server test utility's server dependency.** `test/temp-workflow-servers.ts`
  (imported by `global-setup.ts`, so the whole suite depends on it) imports
  `src/server/processes.js` for process-identity matching; move that small pure helper into the
  test tree so the utility no longer depends on `src/server`. Delete
  `test/temp-workflow-servers.test.ts` with the server.
- Update `loader.test.ts` to stop using the builtins catalog as fixtures (it tests the surviving
  loader; the catalog param itself goes in Phase 4).

Update `package.json` (drop `bin`, the `./client`, `./resource-managers`, and `./builtins`
exports, the `@earendil-works/pi-tui` devDependency, herdr files, and the removed artifact dirs
from `files`). Delete `slophammer.yml` entirely — no boundary tooling carries over.

At the end of Phase 3 the system runs as `Pi → worker → WorkflowEngine → existing SQLite store`.
Behavior is already verified from Phase 2.

### Phase 4 — Strip the store to a clean semantic `SqliteWorkflowStore`

Now that the server is gone, reduce `src/workflows/store.ts` + `src/state/`:

- Remove server-era coordination from `WorkflowRunStore`: lease/token/generation fencing, viewer
  projections, session message queue, and the `session_entries` journal (queries + table). Keep the
  semantic tables and the `WorkflowExecutionStore` interface. Rename to `SqliteWorkflowStore` (or
  keep the name; the interface is what matters).
- Remove the server-era engine files: `queue.ts`, `catalog.ts` (+ the loader's optional catalog
  param), `prompt-evidence.ts`, `command-batch.ts`, `session-reducer.ts`.
- Strip server-era imports from kept files: `requests.ts` and `workflow-message-content.ts`
  (`state/workflow-messages.js`), `human-decision.ts` (`state/viewer.js`).
- Re-home active-time sampling into the worker (per "Active-time accounting").
- Remove `src/state/`: `viewer.ts`, `prune.ts`, `workflow-messages.ts`, `project-store.ts`. Trim
  `schema.ts`, `database.ts`, `mutation.ts` to the semantic store's needs, adding the minimal
  `notifications` table (seam 6). Keep `attempt-time.ts` (trimmed) for the worker.

Re-run the full behavior checklist. This is the only phase that changes the store internals; because
Phases 1–3 already proved the integration, any regression here is isolated to the store strip.

### Phase 5 — Tests + docs

- **Test reduction target: 81 test files → ~25–35** (rebased — the server, builtins, and client
  test files were already deleted on the `deletions/barebones` branch; herdr/viewer/
  resource-manager/channel tests go with their source in Phase 3). Then reduce the engine tests to
  one file per behavior-checklist item plus the worker integration and durable-resume suites below.
  The 85% coverage threshold is gone; do not re-add tests to satisfy a coverage gate.
- Engine unit tests instantiate `WorkflowEngine` directly with a store — they already work against
  the interface; add a `MemoryWorkflowStore` for fast in-process tests (no worker, no SQLite).
- Add worker integration tests: start/status/pause/resume/cancel/restart over the protocol,
  agent-step park + submit rendezvous, checkpoint continuation, decision answer (validation +
  timeout/default), settings patch, notification delivery, missing-submission reminder bound,
  post-workflow turn.
- **Durable-resume tests** — for each, kill Pi _and_ the worker, restart both, then resume the same
  run and assert the pending interaction is reconstructed and re-presented:
  - a checkpoint is waiting;
  - an agent step is waiting for submission;
  - a human decision is waiting;
  - a human decision **expires while Pi is down** (auto-settles to its default choice on resume);
  - the run is paused.
- Update `docs/`: rewrite `WORKFLOWS.md` (drop server/runner/socket sections; add the worker +
  protocol), delete `WORKFLOW_SERVER.md`, `RESOURCE_MANAGERS.md`, `TUI_VIEWER.md`,
  `LIVE_REPLAY_PROTOCOL.md`, `SESSION_EVENT_JOURNAL.md`, `MONITOR.md`; replace `SQLITE_STATE.md`
  with a short "worker SQLite state" note. Keep `CONTROL_LOOPS.md`, `HUMAN_DECISIONS.md`,
  `WORKFLOW_COMPOSITION.md`, `WORKFLOW_UPDATES.md`, `DESIGN_PHILOSOPHY.md` (trim
  durable-runs/required-recovery to the worker reality).
- **Replace `AGENTS.md` wholesale** with a short lean doc: the check command (typecheck + build +
  test, no coverage), the dependency direction, temp-dir-only tests, and the two recovery
  behaviors stated as this repo's own design decisions. No inherited mandates, no slophammer, no
  real-model E2E requirement unless you add one back deliberately.
- Apply the "Repo artifacts outside `src/`" removals (skills, examples, fixtures, schemas,
  protocol, scripts, README rewrite, baseline logs).

---

## Architectural constraint: this must end as a library, not another monolith

The refactor is not complete merely when the server is gone. The resulting codebase must have
clear module boundaries and independently usable components.

### Dependency direction

Dependencies only point inward:

```text
extension ──────→ worker protocol
                     ↓
worker ─────────→ workflows/core
                     ↓
sqlite ─────────→ WorkflowExecutionStore interface

extension must not know SQLite internals
workflows/core must not know Pi
workflows/core must not know child_process / IPC
workflows/core must not know the concrete SQLite implementation
sqlite must not know Pi
worker must not know Pi presentation APIs
```

## Workflow-language behavior checklist (must stay green)

These are the preserved semantics from the goal. Each must behave identically after the change:

- [ ] Every node type behaves identically: agent (submit + assistantMessage), compute, action,
      shell, checkpoint, decision, humanDecision.
- [ ] Composition (`includeWorkflow`, named exits, `includedResult`) unchanged.
- [ ] Control loops (`controlLoop`) unchanged.
- [ ] Checkpoints park and resume in the same run; answers become node output.
- [ ] Human decisions: in-Pi presentation, `/workflow answer`, `onTimeout` default policy.
- [ ] Live settings via JSON Patch affect only later attempts.
- [ ] Updates/notifications (`publishUpdate`, `notify`) unchanged.
- [ ] Pause/resume/cancel/restart semantics unchanged (restart = new run from original input).
- [ ] **Post-workflow model turn** still fires after terminal runs (not on cancellation).
- [ ] **Missing-submission reminders** still bounded (≤2) for the exact pending request.
- [ ] Effect idempotency: repeated key + same request adopts; uncertain manual effect → ambiguous.
- [ ] `maxSteps`, node timeouts, `$result.outcome` routing unchanged.
- [ ] **Active-time accounting:** an agent step's timeout budget excludes parked/waiting time and
      survives pause/resume.
- [ ] **Durable resume:** kill Pi (and the worker) → restart → resume the same run from the
      worker's SQLite, with the pending interaction reconstructed and re-presented — verified for a
      waiting checkpoint, a waiting agent step, a waiting human decision, a human decision that
      expires while Pi is down (auto-settles to default), and a paused run.

## Intentionally removed behavior (confirm these are acceptable)

These are server-specific operational behaviors, not workflow-language semantics. The plan removes
them by default; flag any you want kept and we add it back as an opt-in.

- **Multi-process ownership** (claims, leases, epochs, fencing, runner supervision, process
  handoff/recovery): removed with the server. The worker is a single isolated process; no
  exactly-one-owner coordination across many processes.
- **External decision channels** (Telegram): removed with `src/channels/`. In-Pi decisions remain.
- **Remote viewing** (`piw`, Herdr, WebSocket relay): removed with the viewer/herdr stack.
- **Follow-ups** (`queue-follow-up`/`remove-follow-up`/post-completion prompts): removed from the
  core runtime. Not part of the `*.workflow.ts` programming model.
- **Built-in workflows** (`autoimplement`, `autoplan`, `autodoc`, `sanity-check`, `monitor`,
  `plan-approval`, `plain-summary`, and the non-discoverable helpers): removed with `src/builtins/`.
  A product change — the named workflows disappear. User `*.workflow.ts` files still load (the
  loader's catalog param is optional).

## Estimated reduction

| Area                                                                                                | Lines        | Action                      |
| --------------------------------------------------------------------------------------------------- | ------------ | --------------------------- |
| `src/server/`                                                                                       | 12,611       | remove                      |
| `src/client/`                                                                                       | 2,082        | remove                      |
| `src/resource-managers/`                                                                            | 2,493        | remove                      |
| `src/viewer/` + `src/herdr/`                                                                        | 1,890        | remove                      |
| `src/channels/`                                                                                     | 1,184        | remove                      |
| `src/builtins/`                                                                                     | 10,162       | remove                      |
| `src/render/` (4 files)                                                                             | ~1,372       | remove                      |
| `src/state/` (4 files)                                                                              | ~2,280       | remove; trim rest           |
| `src/workflows/store.ts` server-era coordination                                                    | ~1,500       | strip (keep semantic store) |
| `src/workflows/` server-era files (queue, catalog, prompt-evidence, command-batch, session-reducer) | ~3,211       | remove                      |
| **Total removed/replaced**                                                                          | **~38,000+** | of ~51,000 src lines        |

Beyond `src/`: 118 test files reduced to ~25–35 (server/builtins/client batches already done on
the `deletions/barebones` branch: 118 → 81); `skills/` (5 of 6 deleted), built-in-specific
`examples/`, both `fixtures/` dirs, `schemas/`, `protocol/`, most of `scripts/`, `slophammer.yml`,
the baseline logs, and `docs/MONITOR.md` removed; `README.md` rewritten.

Kept: `src/workflows/` engine + stripped `SqliteWorkflowStore`, `src/extension/` (rewired to the
worker adapter), new `src/worker/` (entry + parking executor + notification sink),
`src/render/{format,node-type}.ts`. User `*.workflow.ts` files still load directly (the loader's
catalog param is removed, not left optional).
