import { createHash, randomUUID } from "node:crypto";
import type { StateDatabase } from "../state/database.js";
import { closeRunTime } from "../state/attempt-time.js";
import { canonicalJson } from "../state/json.js";
import { recordViewerDeltas } from "../state/viewer.js";
import type { WorkflowMessageStore } from "../state/workflow-messages.js";
import { builtinWorkflowCatalog } from "../builtins/catalog.js";
import { compositionMetadata } from "../workflows/composition.js";
import { WorkflowSourceChangedError } from "../workflows/errors.js";
import { hashWorkflowSource, resolveWorkflowSource } from "../workflows/loader.js";
import type { WorkflowDefinition, WorkflowMountedSource, WorkflowSource } from "../workflows/types.js";

export type VerifiedWorkflowSources = {
  root: WorkflowSource;
  mounted: WorkflowMountedSource[];
};

export async function resolveVerifiedWorkflow(
  runId: string,
  sources: VerifiedWorkflowSources,
): Promise<WorkflowDefinition> {
  for (const source of [sources.root, ...sources.mounted.map((mounted) => mounted.source)]) {
    if (source.kind === "file") {
      if ((await hashWorkflowSource(source.path)) !== source.hash) {
        throw new WorkflowSourceChangedError(runId);
      }
    } else {
      builtinWorkflowCatalog.resolve(source, runId);
    }
  }
  const workflow = await resolveWorkflowSource(sources.root, builtinWorkflowCatalog, runId);
  const observed = compositionMetadata(workflow)?.sources ?? [];
  if (
    canonicalJson(sortMountedSources(observed)) !== canonicalJson(sortMountedSources(sources.mounted))
  ) {
    throw new WorkflowSourceChangedError(runId);
  }
  return workflow;
}

function sortMountedSources(sources: WorkflowMountedSource[]): WorkflowMountedSource[] {
  return [...sources].sort((left, right) =>
    left.mountPath.join("/").localeCompare(right.mountPath.join("/")),
  );
}

export function restartRunIdFor(sourceRunId: string, parentRunRevision: number): string {
  return `restart-${createHash("sha256")
    .update(canonicalJson([sourceRunId, parentRunRevision]))
    .digest("hex")}`;
}

export function runHasUnsettledEffects(state: StateDatabase, runId: string): boolean {
  return (
    state.connection
      .prepare(
        "SELECT 1 FROM effects e JOIN runs r ON r.resource_id = e.source_resource_id WHERE r.run_id = ? AND e.status IN ('pending', 'applying', 'ambiguous') LIMIT 1",
      )
      .get(runId) !== undefined
  );
}

export type ParkedCancelOutcome = "cancelled" | "missing" | "terminal";

/**
 * Terminal-cancels a nonrunning run without the queue while keeping the
 * durable writes the old queue path performed: closed active-time intervals,
 * cancelled attempts, settled effects and human decisions, rejected pending
 * submissions, and the `run.queue_cancelled` trace event piw replays.
 */
export function cancelParkedWorkflowRun(
  state: StateDatabase,
  messages: WorkflowMessageStore,
  options: { runId: string; actorId: string },
): ParkedCancelOutcome {
  const now = Date.now();
  return state.transaction(() => {
    const row = state.connection
      .prepare("SELECT resource_id AS resourceId, status FROM runs WHERE run_id = ?")
      .get(options.runId) as { resourceId: string; status: string } | undefined;
    if (row === undefined) return "missing";
    if (["completed", "failed", "timed_out", "cancelled"].includes(row.status)) return "terminal";
    const revision = resourceRevision(state, row.resourceId);
    const errorHash = state.putText("Workflow run cancelled", now);
    state.connection
      .prepare(
        `UPDATE runs SET status = 'cancelled', paused = 0, status_detail = NULL,
           error_hash = ?, updated_at = ?, finished_at = ?
         WHERE run_id = ?`,
      )
      .run(errorHash, now, now, options.runId);
    messages.cancelPendingForRun(options.runId, now);
    closeRunTime(state, options.runId);
    state.connection
      .prepare(
        `UPDATE node_attempts
         SET status = 'cancelled', error_hash = COALESCE(error_hash, ?),
             updated_at = ?, finished_at = COALESCE(finished_at, ?)
         WHERE run_id = ? AND status IN ('pending', 'running', 'waiting', 'interrupted')`,
      )
      .run(errorHash, now, now, options.runId);
    const effects = (
      state.connection
        .prepare(
          `SELECT e.effect_id AS effectId, e.resource_id AS resourceId,
                  e.status, e.attempt_count AS attemptCount
           FROM effects e JOIN runs r ON r.resource_id = e.source_resource_id
           WHERE r.run_id = ? AND e.owner_scope = 'run' AND e.status IN ('pending', 'applying')
           ORDER BY e.effect_id`,
        )
        .all(options.runId) as Array<{
        effectId: string;
        resourceId: string;
        status: "pending" | "applying";
        attemptCount: number;
      }>
    ).filter(
      (effect): effect is { effectId: string; resourceId: string; status: "pending" | "applying"; attemptCount: number } =>
        typeof effect.effectId === "string" &&
        typeof effect.resourceId === "string" &&
        (effect.status === "pending" || effect.status === "applying") &&
        typeof effect.attemptCount === "number",
    );
    for (const effect of effects) {
      const status = effect.status === "applying" ? "ambiguous" : "cancelled";
      const revision = resourceRevision(state, effect.resourceId);
      const changed = state.connection
        .prepare(
          `UPDATE effects
           SET status = ?, next_attempt_at = NULL, error_hash = ?, updated_at = ?, settled_at = ?
           WHERE effect_id = ? AND status = ? AND attempt_count = ?`,
        )
        .run(status, errorHash, now, now, effect.effectId, effect.status, effect.attemptCount);
      if (changed.changes !== 1) {
        throw new Error(`Workflow effect ${effect.effectId} changed during cancellation`);
      }
      if (effect.status === "applying") {
        state.connection
          .prepare(
            `UPDATE effect_attempts
             SET finished_at = ?, outcome = 'interrupted', error_hash = ?
             WHERE effect_id = ? AND attempt_number = ? AND finished_at IS NULL`,
          )
          .run(now, errorHash, effect.effectId, effect.attemptCount);
      }
      bumpResource(state, effect.resourceId, revision, now);
      insertEvent(
        state,
        effect.resourceId,
        revision + 1,
        `effect.${status}`,
        options.actorId,
        { runId: options.runId, reason: "workflowCancelled" },
        now,
      );
    }
    state.connection
      .prepare(
        `INSERT INTO human_decision_resolutions(
           decision_id, outcome, provenance, response_hash, reason, channel,
           actor_id, request_digest, resolved_at
         )
         SELECT d.decision_id, 'cancelled', 'explicit_cancel', NULL,
                'Workflow run cancelled', NULL, ?, d.request_digest, ?
         FROM human_decisions d
         LEFT JOIN human_decision_resolutions r ON r.decision_id = d.decision_id
         WHERE d.run_id = ? AND r.decision_id IS NULL`,
      )
      .run(options.actorId, now, options.runId);
    const receiptHash = state.putJson(
      { status: "rejected", error: "Workflow run cancelled" },
      now,
    );
    state.connection
      .prepare(
        `UPDATE interactive_submissions SET outcome = 'rejected', receipt_hash = ?
         WHERE outcome = 'validating'
           AND request_id IN (SELECT request_id FROM interactive_requests WHERE run_id = ?)`,
      )
      .run(receiptHash, options.runId);
    bumpResource(state, row.resourceId, revision, now);
    insertEvent(
      state,
      row.resourceId,
      revision + 1,
      "run.queue_cancelled",
      options.actorId,
      { status: "cancelled", code: "cancelled" },
      now,
    );
    recordViewerDeltas(
      state,
      options.runId,
      [{ targetType: "summary" }, { targetType: "replay" }],
      now,
    );
    return "cancelled";
  });
}

export function pauseWaitingWorkflowRun(
  state: StateDatabase,
  options: { runId: string; actorId: string },
): "paused" | "already-paused" | "not-waiting" {
  const now = Date.now();
  return state.transaction(() => {
    const row = state.connection
      .prepare(
        "SELECT resource_id AS resourceId, status, paused FROM runs WHERE run_id = ?",
      )
      .get(options.runId) as
      | { resourceId: string; status: string; paused: number }
      | undefined;
    if (
      row === undefined ||
      row.status !== "waiting" ||
      state.connection
        .prepare(
          "SELECT 1 FROM interactive_requests WHERE run_id = ? AND status = 'pending' LIMIT 1",
        )
        .get(options.runId) === undefined
    ) {
      return "not-waiting";
    }
    if (row.paused === 1) return "already-paused";
    const changed = state.connection
      .prepare(
        `UPDATE runs SET paused = 1, status_detail = 'paused', updated_at = ?
         WHERE run_id = ? AND paused = 0 AND status = 'waiting'`,
      )
      .run(now, options.runId);
    if (changed.changes !== 1) return "not-waiting";
    closeRunTime(state, options.runId);
    const revision = resourceRevision(state, row.resourceId);
    bumpResource(state, row.resourceId, revision, now);
    insertEvent(
      state,
      row.resourceId,
      revision + 1,
      "run.paused",
      options.actorId,
      { status: "waiting" },
      now,
    );
    recordViewerDeltas(
      state,
      options.runId,
      [{ targetType: "summary" }, { targetType: "replay" }],
      now,
    );
    return "paused";
  });
}

export function resumePausedWaitingRun(
  state: StateDatabase,
  options: { runId: string; actorId: string },
): "resumed" | "not-paused" {
  const now = Date.now();
  return state.transaction(() => {
    const row = state.connection
      .prepare(
        "SELECT resource_id AS resourceId, status, paused FROM runs WHERE run_id = ?",
      )
      .get(options.runId) as
      | { resourceId: string; status: string; paused: number }
      | undefined;
    if (row === undefined || row.status !== "waiting" || row.paused !== 1) return "not-paused";
    const changed = state.connection
      .prepare(
        `UPDATE runs
         SET paused = 0, status_detail = 'waiting for origin-session input', updated_at = ?
         WHERE run_id = ? AND status = 'waiting' AND paused = 1`,
      )
      .run(now, options.runId);
    if (changed.changes !== 1) return "not-paused";
    const revision = resourceRevision(state, row.resourceId);
    bumpResource(state, row.resourceId, revision, now);
    insertEvent(
      state,
      row.resourceId,
      revision + 1,
      "run.resumed",
      options.actorId,
      { status: "waiting" },
      now,
    );
    recordViewerDeltas(
      state,
      options.runId,
      [{ targetType: "summary" }, { targetType: "replay" }],
      now,
    );
    return "resumed";
  });
}

function resourceRevision(state: StateDatabase, resourceId: string): number {
  const row = state.connection
    .prepare("SELECT revision FROM resources WHERE resource_id = ?")
    .get(resourceId) as { revision: number } | undefined;
  if (row === undefined || typeof row.revision !== "number") {
    throw new Error("Workflow resource is missing");
  }
  return row.revision;
}

function bumpResource(
  state: StateDatabase,
  resourceId: string,
  expectedRevision: number,
  now: number,
): void {
  const result = state.connection
    .prepare(
      "UPDATE resources SET revision = revision + 1, updated_at = ? WHERE resource_id = ? AND revision = ?",
    )
    .run(now, resourceId, expectedRevision);
  if (result.changes !== 1) throw new Error("Resource revision conflict");
  const run = state.connection
    .prepare(
      `SELECT r.run_id AS runId
       FROM runs r JOIN viewer_runs v ON v.run_id = r.run_id
       WHERE r.resource_id = ?`,
    )
    .get(resourceId) as { runId?: unknown } | undefined;
  if (typeof run?.runId === "string") {
    recordViewerDeltas(state, run.runId, viewerBumpTargets, now);
  }
}

const viewerBumpTargets = [
  { targetType: "summary" },
  { targetType: "graph" },
  { targetType: "replay" },
  { targetType: "inspector", targetKey: "run" },
] as const;

function insertEvent(
  state: StateDatabase,
  resourceId: string,
  revision: number,
  type: string,
  actorId: string,
  payload: unknown,
  now: number,
): void {
  const eventId = `event-${randomUUID()}`;
  const payloadHash = state.putJson(payload, now);
  state.connection
    .prepare(
      `INSERT INTO events(
         event_id, resource_id, resource_revision, event_type, actor_type,
         actor_id, lease_generation, payload_hash, recorded_at
       ) VALUES (?, ?, ?, ?, 'control', ?, NULL, ?, ?)`,
    )
    .run(eventId, resourceId, revision, type, actorId, payloadHash, now);
}
