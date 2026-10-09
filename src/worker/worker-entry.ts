import { createHash, randomUUID } from "node:crypto";

import { workflowStatePath } from "../state/database.js";
import { canonicalJson, type JsonValue } from "../state/json.js";
import { tokenHash } from "../state/mutation.js";
import { WorkflowMessageStore } from "../state/workflow-messages.js";
import { compositionMetadata } from "../workflows/composition.js";
import { WorkflowEngine } from "../workflows/engine.js";
import { errorMessage } from "../workflows/errors.js";
import { HumanDecisionStore } from "../workflows/human-decision.js";
import { resolveWorkflowRef } from "../workflows/loader.js";
import {
  readWorkflowRequest,
  requestForAttempt,
  type InteractiveRequestRecord,
} from "../workflows/requests.js";
import { createRunId, WorkflowRunStore, type RunWriteAuthority } from "../workflows/store.js";
import type {
  AgentStepContract,
  AgentStepPresentation,
  HumanDecisionRequest,
  HumanDecisionResponse,
  WorkflowDefinition,
  WorkflowRunState,
  WorkflowSource,
  WorkflowUpdateInput,
} from "../workflows/types.js";
import { validateWorkflowUpdate } from "../workflows/updates.js";
import { prepareInteractionResume, WorkerInteractionStore } from "./interaction-store.js";
import {
  ParkingExecutor,
  type ParkedAgentRequest,
  type ValidationOutcome,
} from "./parking-executor.js";
import {
  isRecord,
  parseWorkerRequest,
  requireNonNegativeInteger,
  requireOptionalString,
  requireString,
  WORKER_PROTOCOL_SCHEMA,
  type HostMessage,
  type WorkerEvent,
  type WorkerFrame,
  type WorkerRequest,
} from "./protocol.js";
import {
  cancelParkedWorkflowRun,
  pauseWaitingWorkflowRun,
  resolveVerifiedWorkflow,
  restartRunIdFor,
  resumePausedWaitingRun,
  runHasUnsettledEffects,
} from "./run-control.js";
import {
  markNotificationDelivered,
  WorkerNotificationSink,
} from "./worker-notification-sink.js";

export type WorkerConfig = {
  databasePath: string;
  originSessionId: string;
  homeDir?: string;
  cwd: string;
};

const TERMINAL_RUN_STATUSES = ["completed", "failed", "timed_out", "cancelled"] as const;

type EngineWorkflow = {
  definition: WorkflowDefinition;
  source: WorkflowSource;
};

type EngineHandle = {
  engine: WorkflowEngine;
  workflow: EngineWorkflow;
  current: Promise<void>;
};

type DeferredValidation = {
  pending: boolean;
  promise: Promise<void>;
  resolve: () => void;
  reject: (error: Error) => void;
};

type StoredAgentContract = {
  contract: AgentStepContract;
  prompt: string;
  presentation?: AgentStepPresentation;
};

export function readWorkerConfig(env: NodeJS.ProcessEnv = process.env): WorkerConfig {
  const raw = env.PI_WORKFLOWS_WORKER_CONFIG;
  if (raw === undefined || raw === "") {
    throw new Error("PI_WORKFLOWS_WORKER_CONFIG is not set");
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("PI_WORKFLOWS_WORKER_CONFIG is not valid JSON");
  }
  if (!isRecord(parsed)) {
    throw new Error("PI_WORKFLOWS_WORKER_CONFIG must be a JSON object");
  }
  const originSessionId = requireString(parsed.originSessionId, "originSessionId");
  const databasePath =
    requireOptionalString(parsed.databasePath, "databasePath") ?? workflowStatePath();
  const homeDir = requireOptionalString(parsed.homeDir, "homeDir");
  const cwd = requireOptionalString(parsed.cwd, "cwd") ?? process.cwd();
  return {
    databasePath,
    originSessionId,
    ...(homeDir === undefined ? {} : { homeDir }),
    cwd,
  };
}

/**
 * The single worker process of the localized architecture: it owns the
 * workflow engine for one Pi session and drives the durable store directly,
 * replacing the old server/runner/queue split with one IPC boundary.
 */
export class WorkflowWorker {
  readonly store: WorkflowRunStore;
  private readonly decisions: HumanDecisionStore;
  private readonly messages: WorkflowMessageStore;
  private readonly interactions: WorkerInteractionStore;
  private readonly engines = new Map<string, EngineHandle>();
  private readonly validations = new Map<string, DeferredValidation>();
  private readonly claims = new Map<string, RunWriteAuthority>();

  constructor(
    readonly config: WorkerConfig,
    private readonly emitEvent: (event: WorkerEvent) => void,
  ) {
    this.store = new WorkflowRunStore(config.databasePath);
    this.decisions = new HumanDecisionStore(config.databasePath, {
      state: this.store.state,
      authorityProvider: (runId) => this.claims.get(runId),
    });
    this.messages = new WorkflowMessageStore(this.store.state);
    this.interactions = new WorkerInteractionStore(this.store.state, this.messages);
  }

  async handle(message: HostMessage): Promise<JsonValue> {
    switch (message.type) {
      case "run.start":
        return this.handleStart(message);
      case "run.list":
        return this.handleList(message.status);
      case "run.get":
        return this.handleGet(message.runId);
      case "run.resume":
        return this.handleResume(message.runId);
      case "run.pause":
        return this.handlePause(message.runId);
      case "run.cancel":
        return this.handleCancel(message.runId);
      case "run.restart":
        return this.handleRestart(message.runId, message.expectedRevision);
      case "agent.submit":
        return this.handleAgentSubmit(message.requestId, message.submission);
      case "agent.update":
        return this.handleAgentUpdate(message.requestId, message.update);
      case "notification.delivered":
        return this.handleNotificationDelivered(
          message.notificationRequestId,
          message.ok,
          message.piSessionEntryId,
        );
      case "checkpoint.answer":
        return this.handleCheckpointAnswer(message.requestId, message.input);
      case "decision.answer":
        return this.handleDecisionAnswer(message.requestId, message.response);
      case "settings.patch":
        return this.handleSettingsPatch(message.runId, message.patch);
      default: {
        const exhaustive: never = message;
        throw new Error(`Unsupported worker message: ${String(exhaustive)}`);
      }
    }
  }

  private async handleStart(message: HostMessage & { type: "run.start" }): Promise<JsonValue> {
    const resolved = await resolveWorkflowRef(message.workflow, {
      cwd: this.config.cwd,
      ...(this.config.homeDir === undefined ? {} : { homeDir: this.config.homeDir }),
    });
    const runId = createRunId(resolved.definition.name);
    this.launchRun(
      runId,
      { definition: resolved.definition, source: resolved.source },
      message.input ?? null,
    );
    return { runId };
  }

  private launchRun(runId: string, workflow: EngineWorkflow, input: unknown): void {
    const handle = this.createEngineHandle(workflow);
    this.engines.set(runId, handle);
    this.trackRun(
      runId,
      handle,
      handle.engine.run(workflow.definition, input, {
        runId,
        workflowSource: workflow.source,
      }),
    );
  }

  private createEngineHandle(workflow: EngineWorkflow): EngineHandle {
    const executor = new ParkingExecutor(
      this.interactions,
      this.config.originSessionId,
      (request) => this.emitAgentRequest(request),
      (outcome) => this.settleValidation(outcome),
    );
    const engine = new WorkflowEngine({
      executor,
      notificationSink: new WorkerNotificationSink(
        this.messages,
        this.config.originSessionId,
        (event) =>
          this.emitEvent({
            type: "notification.request",
            runId: event.runId,
            notificationRequestId: event.notificationRequestId,
            kind: event.kind,
            content: event.content,
          }),
      ),
      store: this.store,
      onRunStarted: (startedRunId, state) => {
        this.ensureRunBinding(startedRunId);
        this.emitEvent({
          type: "run.started",
          runId: startedRunId,
          // SAFETY: run state is canonical-JSON persisted, so it is a JsonValue.
          state: state as unknown as JsonValue,
        });
      },
      onEvent: (event, state) => {
        this.emitEvent({
          type: "run.changed",
          runId: state.runId,
          seq: event.seq,
          status: state.status,
        });
        if (event.type === "checkpoint_requested") {
          this.emitCheckpointRequest(event.payload, state.runId);
        }
      },
    });
    return { engine, workflow, current: Promise.resolve() };
  }

  private emitCheckpointRequest(payload: Record<string, unknown>, runId: string): void {
    if (!isRecord(payload)) return;
    const requestId = requireString(payload.requestId, "checkpoint request id");
    const record = readWorkflowRequest(this.store.state, requestId);
    if (record === undefined) return;
    this.emitEvent({
      type: record.kind === "decision" ? "decision.request" : "checkpoint.request",
      runId,
      requestId,
      contract: record.contract,
    });
  }

  private trackRun(
    runId: string,
    handle: EngineHandle,
    result: Promise<{ runId: string; state: WorkflowRunState }>,
  ): void {
    handle.current = result.then(
      (finished) => {
        const state = finished.state;
        if (isTerminalRunStatus(state.status)) {
          this.engines.delete(runId);
          this.emitEvent({
            type: "run.finished",
            runId,
            status: state.status,
            ...(state.finalOutput === undefined
              ? {}
              : { finalOutput: state.finalOutput as JsonValue }),
            ...(state.error === undefined ? {} : { error: state.error }),
          });
        } else if (state.status === "waiting") {
          const request =
            state.currentAttemptId === undefined
              ? undefined
              : requestForAttempt(this.store.state, runId, state.currentAttemptId);
          if (
            request !== undefined &&
            (request.kind === "checkpoint" || request.kind === "decision")
          ) {
            this.emitPendingRequest(request);
          }
        }
      },
      (error: unknown) => {
        const state = this.store.readRunState(runId);
        if (state === null) {
          this.engines.delete(runId);
          this.emitEvent({
            type: "run.finished",
            runId,
            status: "failed",
            error: errorMessage(error),
          });
        } else if (isTerminalRunStatus(state.status)) {
          this.engines.delete(runId);
          this.emitEvent({
            type: "run.finished",
            runId,
            status: state.status,
            ...(state.finalOutput === undefined
              ? {}
              : { finalOutput: state.finalOutput as JsonValue }),
            ...(state.error === undefined ? {} : { error: state.error }),
          });
        }
      },
    );
  }

  private ensureRunBinding(runId: string): void {
    this.store.state.connection
      .prepare(
        `INSERT INTO run_bindings(run_id, origin_session_id, execution_mode, created_at)
         VALUES (?, ?, 'interactive', ?)
         ON CONFLICT(run_id) DO NOTHING`,
      )
      .run(runId, this.config.originSessionId, Date.now());
  }

  private handleList(status: "active" | "waiting" | "paused" | undefined): JsonValue {
    const runs = this.store.listRuns().map((loaded) => {
      const state = loaded.state;
      return {
        runId: state.runId,
        workflowName: state.workflowName,
        status: state.status,
        paused: state.paused === true,
        updatedAt: state.updatedAt,
        originSessionId: this.store.originSessionId(state.runId) ?? null,
      };
    });
    const filtered =
      status === undefined
        ? runs
        : runs.filter((run) => {
            if (status === "active") return run.status === "running" || run.status === "waiting";
            if (status === "waiting") return run.status === "waiting";
            return run.paused;
          });
    return { runs: filtered as JsonValue };
  }

  private handleGet(runId: string): JsonValue {
    const state = this.requireRunState(runId);
    return {
      run: {
        runId: state.runId,
        workflowName: state.workflowName,
        status: state.status,
        revision: this.store.runRevision(runId),
        paused: state.paused === true,
        statusDetail: state.statusDetail ?? null,
        currentNode: state.currentNode ?? null,
        waitingOn: state.waitingOn ?? null,
        startedAt: state.startedAt,
        finishedAt: state.finishedAt ?? null,
        updatedAt: state.updatedAt,
        finalOutput: (state.finalOutput ?? null) as JsonValue,
        error: state.error ?? null,
      },
    };
  }

  private handlePause(runId: string): JsonValue {
    const state = this.requireSessionRun(runId);
    if (isTerminalRunStatus(state.status)) {
      throw new Error("Workflow run is already terminal");
    }
    const handle = this.engines.get(runId);
    if (state.status === "running" && handle !== undefined) {
      handle.engine.pause();
    } else {
      pauseWaitingWorkflowRun(this.store.state, {
        runId,
        actorId: this.config.originSessionId,
      });
    }
    return { runId, paused: true };
  }

  private async handleResume(runId: string): Promise<JsonValue> {
    const state = this.requireSessionRun(runId);
    if (isTerminalRunStatus(state.status)) {
      throw new Error("Workflow run is already terminal");
    }
    const handle = this.engines.get(runId);
    if (state.paused === true) {
      if (state.status === "running" && handle !== undefined) {
        handle.engine.resume();
      } else {
        resumePausedWaitingRun(this.store.state, {
          runId,
          actorId: this.config.originSessionId,
        });
      }
    }
    const resumed = this.store.readRunState(runId);
    if (resumed === null) throw new Error(`Workflow run not found: ${runId}`);
    if (resumed.status === "waiting") {
      await this.sweepExpiredDecisions(runId);
      const resumeAttemptId = await prepareInteractionResume(this.store, this.interactions, runId);
      if (resumeAttemptId !== undefined) {
        await this.resumeRun(runId, resumeAttemptId);
        return { runId, resumed: true };
      }
      const request =
        resumed.currentAttemptId === undefined
          ? undefined
          : requestForAttempt(this.store.state, runId, resumed.currentAttemptId);
      if (request !== undefined) {
        this.emitPendingRequest(request);
        return { runId, resumed: false, waitingForInteraction: true, requestId: request.requestId };
      }
      return { runId, resumed: false, waitingForInteraction: false };
    }
    if (resumed.status === "running") {
      if (handle !== undefined) return { runId, resumed: false };
      await this.resumeRun(runId, undefined);
      return { runId, resumed: true };
    }
    throw new Error(`Workflow run is not resumable: ${runId}`);
  }

  private async handleCancel(runId: string): Promise<JsonValue> {
    const state = this.requireSessionRun(runId);
    if (isTerminalRunStatus(state.status)) {
      throw new Error("Workflow run is already terminal");
    }
    const handle = this.engines.get(runId);
    if (state.status === "running" && handle !== undefined) {
      handle.engine.cancel();
      await handle.current;
      const cancelled = this.store.readRunState(runId);
      return { runId, status: cancelled?.status ?? "cancelled" };
    }
    const outcome = cancelParkedWorkflowRun(this.store.state, this.messages, {
      runId,
      actorId: this.config.originSessionId,
    });
    if (outcome === "missing") throw new Error(`Workflow run not found: ${runId}`);
    if (outcome === "terminal") throw new Error("Workflow run is already terminal");
    this.engines.delete(runId);
    this.emitEvent({ type: "run.finished", runId, status: "cancelled" });
    return { runId, status: "cancelled" };
  }

  private async handleRestart(runId: string, expectedRevision: number): Promise<JsonValue> {
    const source = this.requireSessionRun(runId);
    requireNonNegativeInteger(expectedRevision, "expectedRevision");
    if (!isTerminalRunStatus(source.status)) {
      throw new Error("Restart requires a terminal run");
    }
    if (runHasUnsettledEffects(this.store.state, runId)) {
      throw new Error("Resolve the run's unsettled effects before requesting a fresh restart");
    }
    if (this.store.runRevision(runId) !== expectedRevision) {
      throw new Error("Workflow run revision changed since the terminal state");
    }
    const restartRunId = restartRunIdFor(runId, expectedRevision);
    if (
      this.store.state.connection.prepare("SELECT 1 FROM runs WHERE run_id = ?").get(restartRunId) !==
      undefined
    ) {
      return { runId: restartRunId, parentRunId: runId, adopted: true };
    }
    if (source.workflowSource === undefined) {
      throw new Error("Workflow run has no recorded source");
    }
    const workflow = await resolveVerifiedWorkflow(runId, {
      root: source.workflowSource,
      mounted: source.workflowSources ?? [],
    });
    this.launchRun(
      restartRunId,
      { definition: workflow, source: source.workflowSource },
      source.input,
    );
    return { runId: restartRunId, parentRunId: runId };
  }

  private async handleAgentSubmit(requestId: string, submission: JsonValue): Promise<JsonValue> {
    const request = this.requireInteractionRequest(requestId);
    if (request.kind !== "agent" && request.kind !== "assistant") {
      throw new Error("Only an agent step accepts submissions");
    }
    const runId = request.runId;
    this.requireSessionRun(runId);
    const settled = this.interactions.acceptedInteraction(runId);
    if (settled !== undefined && settled.requestId === requestId) {
      const resumeAttemptId = await prepareInteractionResume(this.store, this.interactions, runId);
      if (resumeAttemptId !== undefined) await this.resumeRun(runId, resumeAttemptId);
      return {
        requestId,
        runId,
        submissionId: settled.submissionId,
        status: "accepted",
      };
    }
    const deferred = createDeferredValidation();
    this.validations.set(requestId, deferred);
    try {
      this.interactions.beginValidation({
        requestId,
        submissionId: submissionIdFor(requestId),
        idempotencyKey: `agent-submit-${requestId}`,
        expectedRevision: request.revision,
        payload: submission,
      });
      const resumeAttemptId = await prepareInteractionResume(this.store, this.interactions, runId);
      if (resumeAttemptId === undefined) {
        throw new Error("Interactive submission validation is not awaiting this request");
      }
      await Promise.all([this.resumeRun(runId, resumeAttemptId), deferred.promise]);
    } catch (error) {
      this.validations.delete(requestId);
      if (deferred.pending) deferred.reject(error as Error);
      throw error;
    }
    this.validations.delete(requestId);
    return {
      requestId,
      runId,
      submissionId: submissionIdFor(requestId),
      status: "accepted",
    };
  }

  private async handleAgentUpdate(
    requestId: string,
    update: WorkflowUpdateInput,
  ): Promise<JsonValue> {
    const request = this.requireInteractionRequest(requestId);
    if (request.kind !== "agent" && request.kind !== "assistant") {
      throw new Error("Only an agent step accepts updates");
    }
    const contract = agentContract(request.contract);
    this.requireSessionRun(request.runId);
    const validated = validateWorkflowUpdate(update);
    await this.store.publishUpdate(request.runId, contract.contract.nodeId, contract.contract.attemptId, validated);
    return { requestId, runId: request.runId };
  }

  private handleNotificationDelivered(
    notificationRequestId: string,
    ok: boolean,
    piSessionEntryId: string | undefined,
  ): JsonValue {
    markNotificationDelivered(this.store.state, {
      notificationRequestId,
      ok,
      ...(piSessionEntryId === undefined ? {} : { piSessionEntryId }),
    });
    return { ok: true };
  }

  private handleCheckpointAnswer(requestId: string, input: JsonValue | undefined): JsonValue {
    const request = this.requireInteractionRequest(requestId);
    if (request.kind === "decision") {
      throw new Error("Protected human decisions cannot be answered by the workflow tool");
    }
    if (request.kind !== "checkpoint") {
      throw new Error("Only an ordinary checkpoint accepts an answer");
    }
    const runId = request.runId;
    const state = this.requireSessionRun(runId);
    if (state.paused === true) throw new Error("Workflow run is paused");
    const submissionId = submissionIdFor(requestId);
    const settled = this.interactions.submitInteraction({
      requestId,
      submissionId,
      idempotencyKey: `checkpoint-answer-${requestId}`,
      expectedRevision: request.revision,
      payload: input ?? null,
      accepted: true,
      receipt: { requestId, runId },
    });
    void this.resumeAfterSubmission(runId);
    return {
      requestId,
      runId,
      submissionId,
      outcome: settled.outcome,
      receipt: settled.receipt,
    };
  }

  private handleDecisionAnswer(requestId: string, response: JsonValue): JsonValue {
    const request = this.requireInteractionRequest(requestId);
    if (request.kind !== "decision") {
      throw new Error(`Decision request not found: ${requestId}`);
    }
    const runId = request.runId;
    const state = this.requireSessionRun(runId);
    if (state.paused === true) throw new Error("Workflow run is paused");
    // SAFETY: commitTransition validated this contract when it created the decision request.
    const decision = request.contract as unknown as HumanDecisionRequest;
    const idempotencyKey = `decision-answer-${requestId}`;
    const accepted = this.decisions.acceptSync(decision, {
      ...(response as HumanDecisionResponse),
      decisionId: decision.decisionId,
      requestDigest: decision.requestDigest,
      source: {
        channel: "pi",
        actorId: this.config.originSessionId,
        eventId: idempotencyKey,
      },
      idempotencyKey,
    });
    if (accepted.status === "conflict") {
      throw new Error("Another human decision answer already won");
    }
    const settled = this.interactions.submitInteraction({
      requestId,
      submissionId: submissionIdFor(requestId),
      idempotencyKey,
      expectedRevision: request.revision,
      payload: response,
      accepted: true,
      // SAFETY: the resolved decision receipt is canonical-JSON persisted.
      receipt: accepted.decision as unknown as JsonValue,
    });
    void this.resumeAfterSubmission(runId);
    return {
      requestId,
      runId,
      outcome: settled.outcome,
      // SAFETY: the resolved decision receipt is canonical-JSON persisted.
      decision: accepted.decision as unknown as JsonValue,
    };
  }

  private async handleSettingsPatch(runId: string, patch: JsonValue): Promise<JsonValue> {
    this.requireSessionRun(runId);
    const scopes = this.store.listSettingsScopes(runId);
    const [scope] = scopes;
    if (scope === undefined) throw new Error("Workflow run has no settings scope");
    if (scopes.length > 1) {
      throw new Error("Workflow run has more than one settings scope");
    }
    const state = this.requireRunState(runId);
    if (state.workflowSource === undefined) {
      throw new Error("Workflow run has no recorded source");
    }
    const workflow = await resolveVerifiedWorkflow(runId, {
      root: state.workflowSource,
      mounted: state.workflowSources ?? [],
    });
    const settings =
      scope.mountPath === ""
        ? workflow.settings
        : compositionMetadata(workflow)?.scopes[scope.mountPath]?.settings;
    if (settings === undefined) throw new Error("Workflow settings definition is missing");
    const result = await this.store.changeSettings(settings, {
      runId,
      scopeId: scope.scopeId,
      requestId: `settings-${createHash("sha256")
        .update(`${runId}:${scope.scopeId}:${canonicalJson(patch)}`)
        .digest("hex")
        .slice(0, 40)}`,
      expectedChangeNumber: scope.changeNumber,
      actor: { type: "session", id: this.config.originSessionId },
      source: "pi-session",
      patch,
    });
    return {
      runId,
      scopeId: scope.scopeId,
      changeNumber: result.change.changeNumber,
      adopted: result.adopted,
    };
  }

  private async resumeAfterSubmission(runId: string): Promise<void> {
    try {
      const resumeAttemptId = await prepareInteractionResume(this.store, this.interactions, runId);
      if (resumeAttemptId !== undefined) await this.resumeRun(runId, resumeAttemptId);
    } catch (error) {
      this.emitEvent({
        type: "run.finished",
        runId,
        status: "failed",
        error: errorMessage(error),
      });
    }
  }

  private async resumeRun(runId: string, resumeAttemptId: string | undefined): Promise<void> {
    const handle = this.engines.get(runId) ?? (await this.adoptEngine(runId));
    this.trackRun(
      runId,
      handle,
      handle.engine.resumeRun(handle.workflow.definition, runId, {
        workflowSource: handle.workflow.source,
        ...(resumeAttemptId === undefined ? {} : { resumeInteractionAttemptId: resumeAttemptId }),
      }),
    );
    await handle.current;
  }

  private async adoptEngine(runId: string): Promise<EngineHandle> {
    const state = this.requireRunState(runId);
    if (state.workflowSource === undefined) {
      throw new Error("Workflow run has no recorded source");
    }
    const workflow = await resolveVerifiedWorkflow(runId, {
      root: state.workflowSource,
      mounted: state.workflowSources ?? [],
    });
    const handle = this.createEngineHandle({
      definition: workflow,
      source: state.workflowSource,
    });
    this.engines.set(runId, handle);
    return handle;
  }

  private async sweepExpiredDecisions(runId: string): Promise<void> {
    const now = new Date();
    const expired = (await this.decisions.listExpiredDefaultRequests(now)).filter(
      (request) => request.runId === runId && request.defaultResponse !== undefined,
    );
    if (expired.length === 0) return;
    this.claimRunLease(runId);
    try {
      for (const request of expired) {
        const interaction = readWorkflowRequest(this.store.state, request.decisionId);
        if (interaction === undefined || interaction.status !== "pending") continue;
        const accepted = this.decisions.resolveTimeoutSync(request, now);
        this.interactions.submitInteraction({
          requestId: request.decisionId,
          submissionId: `timeout-${request.decisionId}`,
          idempotencyKey: `timeout-${request.decisionId}`,
          expectedRevision: interaction.revision,
          payload: (request.defaultResponse ?? null) as JsonValue,
          accepted: true,
          // SAFETY: the resolved decision receipt is canonical-JSON persisted.
          receipt: accepted.decision as unknown as JsonValue,
        });
      }
    } finally {
      this.releaseRunLease(runId);
    }
  }

  private claimRunLease(runId: string): RunWriteAuthority {
    const now = Date.now();
    const row = this.store.state.connection
      .prepare(
        `SELECT r.resource_id AS resourceId, l.generation AS generation
         FROM runs r JOIN leases l ON l.resource_id = r.resource_id
         WHERE r.run_id = ?`,
      )
      .get(runId) as { resourceId: string; generation: number } | undefined;
    if (row === undefined) throw new Error(`Workflow run not found: ${runId}`);
    const ownerId = `worker-${this.config.originSessionId}`;
    const authority: RunWriteAuthority = {
      actor: { type: "system", id: ownerId },
      ownerType: "system",
      ownerId,
      token: `lease-${randomUUID()}`,
      generation: row.generation,
      leaseMs: 60_000,
    };
    this.claims.set(runId, authority);
    this.store.state.connection
      .prepare(
        `UPDATE leases
         SET owner_type = 'system', owner_id = ?, token_hash = ?, acquired_at = ?,
             heartbeat_at = ?, expires_at = ?
         WHERE resource_id = ?`,
      )
      .run(authority.ownerId, tokenHash(authority.token), now, now, now + 60_000, row.resourceId);
    return authority;
  }

  private releaseRunLease(runId: string): void {
    this.claims.delete(runId);
    this.store.state.connection
      .prepare(
        `UPDATE leases
         SET owner_type = NULL, owner_id = NULL, token_hash = NULL, acquired_at = NULL,
             heartbeat_at = NULL, expires_at = NULL
         WHERE resource_id = (SELECT resource_id FROM runs WHERE run_id = ?)`,
      )
      .run(runId);
  }

  private emitAgentRequest(request: ParkedAgentRequest): void {
    this.emitEvent({
      type: "agent.request",
      runId: request.runId,
      requestId: request.requestId,
      contract: request.contract,
      prompt: request.prompt,
      ...(request.presentation === undefined ? {} : { presentation: request.presentation }),
    });
  }

  private emitPendingRequest(request: InteractiveRequestRecord): void {
    if (request.kind === "agent" || request.kind === "assistant") {
      const stored = agentContract(request.contract);
      this.emitEvent({
        type: "agent.request",
        runId: request.runId,
        requestId: request.requestId,
        contract: stored.contract,
        prompt: stored.prompt,
        ...(stored.presentation === undefined ? {} : { presentation: stored.presentation }),
      });
      return;
    }
    this.emitEvent({
      type: request.kind === "decision" ? "decision.request" : "checkpoint.request",
      runId: request.runId,
      requestId: request.requestId,
      contract: request.contract,
    });
  }

  private settleValidation(outcome: ValidationOutcome): void {
    const deferred = this.validations.get(outcome.requestId);
    if (deferred === undefined || !deferred.pending) return;
    this.validations.delete(outcome.requestId);
    if (outcome.accepted) deferred.resolve();
    else deferred.reject(new Error(outcome.error ?? "Agent submission was rejected"));
  }

  private requireRunState(runId: string): WorkflowRunState {
    const state = this.store.readRunState(runId);
    if (state === null) throw new Error(`Workflow run not found: ${runId}`);
    return state;
  }

  private requireSessionRun(runId: string): WorkflowRunState {
    const state = this.requireRunState(runId);
    if (this.store.originSessionId(runId) !== this.config.originSessionId) {
      throw new Error("Workflow run belongs to another Pi session");
    }
    return state;
  }

  private requireInteractionRequest(requestId: string): InteractiveRequestRecord {
    const request = readWorkflowRequest(this.store.state, requestId);
    if (request === undefined) throw new Error(`Workflow request not found: ${requestId}`);
    return request;
  }
}

function createDeferredValidation(): DeferredValidation {
  let pending = true;
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  void promise.catch(() => undefined);
  return {
    get pending(): boolean {
      return pending;
    },
    set pending(value: boolean) {
      pending = value;
    },
    promise,
    resolve: () => {
      pending = false;
      resolve();
    },
    reject: (error: Error) => {
      pending = false;
      reject(error);
    },
  };
}

function submissionIdFor(requestId: string): string {
  return `submission-${createHash("sha256").update(requestId).digest("hex").slice(0, 40)}`;
}

function agentContract(value: JsonValue): StoredAgentContract {
  if (!isRecord(value) || !isRecord(value.contract)) {
    throw new Error("Agent request contract is missing");
  }
  const prompt = requireString(value.prompt, "prompt");
  const presentation = value.presentation;
  if (presentation !== undefined && !isRecord(presentation)) {
    throw new Error("Agent request presentation is invalid");
  }
  // SAFETY: parkInteraction wrote this wrapper from the live executor contract.
  return {
    contract: value.contract as unknown as AgentStepContract,
    prompt,
    ...(presentation === undefined
      ? {}
      : { presentation: presentation as unknown as AgentStepPresentation }),
  };
}

function isTerminalRunStatus(status: string): boolean {
  return (TERMINAL_RUN_STATUSES as readonly string[]).includes(status);
}

export function main(): void {
  let config: WorkerConfig;
  try {
    config = readWorkerConfig();
  } catch (error) {
    process.stderr.write(`pi-workflows worker failed to start: ${errorMessage(error)}\n`);
    process.exit(1);
  }
  const send = (frame: WorkerFrame): void => {
    process.send?.(frame);
  };
  const worker = new WorkflowWorker(config, (event) =>
    send({ schema: WORKER_PROTOCOL_SCHEMA, event }),
  );
  process.on("message", (frame: unknown) => {
    let request: WorkerRequest;
    try {
      request = parseWorkerRequest(frame);
    } catch (error) {
      if (isRecord(frame) && typeof frame.id === "string") {
        send({ schema: WORKER_PROTOCOL_SCHEMA, id: frame.id, ok: false, error: errorMessage(error) });
      }
      return;
    }
    worker.handle(request.message).then(
      (result) => send({ schema: WORKER_PROTOCOL_SCHEMA, id: request.id, ok: true, result }),
      (error) =>
        send({ schema: WORKER_PROTOCOL_SCHEMA, id: request.id, ok: false, error: errorMessage(error) }),
    );
  });
  process.on("disconnect", () => {
    process.exit(0);
  });
}

if (process.env.PI_WORKFLOWS_WORKER_CONFIG !== undefined && typeof process.send === "function") {
  main();
}
