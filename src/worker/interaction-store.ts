import type { StateDatabase } from "../state/database.js";
import type { JsonValue } from "../state/json.js";
import { recordViewerDeltas } from "../state/viewer.js";
import type { WorkflowMessageStore } from "../state/workflow-messages.js";
import {
  ensureWorkflowRequest,
  readWorkflowRequest,
  recordWorkflowSubmission,
  type InteractiveRequestRecord,
  type WorkflowRequestKind,
  workflowRequestId,
} from "../workflows/requests.js";
import type { WorkflowRunStore } from "../workflows/store.js";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export type InteractionCandidate = {
  requestId: string;
  attemptId: string;
  nodeId: string;
  submissionId: string;
  payload: JsonValue;
};

type InteractionCandidateRow = {
  requestId: string;
  attemptId: string;
  nodeId: string;
  submissionId: string;
  payloadHash: Buffer;
};

function isInteractionCandidateRow(value: unknown): value is InteractionCandidateRow {
  return (
    isRecord(value) &&
    typeof value.requestId === "string" &&
    typeof value.attemptId === "string" &&
    typeof value.nodeId === "string" &&
    typeof value.submissionId === "string" &&
    Buffer.isBuffer(value.payloadHash)
  );
}

type SubmissionDetailRow = {
  requestId: string;
  submissionId: string;
  idempotencyKey: string;
  outcome: string;
  payloadHash: Buffer;
  receiptHash: Buffer | null;
  submittedAt: number;
};

function isSubmissionDetailRow(value: unknown): value is SubmissionDetailRow {
  return (
    isRecord(value) &&
    typeof value.requestId === "string" &&
    typeof value.submissionId === "string" &&
    typeof value.idempotencyKey === "string" &&
    typeof value.outcome === "string" &&
    Buffer.isBuffer(value.payloadHash) &&
    (value.receiptHash === null || Buffer.isBuffer(value.receiptHash)) &&
    typeof value.submittedAt === "number"
  );
}

/** Durable interactions for one worker process, mirroring the server store minus queue claims. */
export class WorkerInteractionStore {
  constructor(
    private readonly state: StateDatabase,
    private readonly messages: WorkflowMessageStore,
  ) {}

  readRequest(requestId: string): InteractiveRequestRecord | undefined {
    return readWorkflowRequest(this.state, requestId);
  }

  acceptedInteraction(runId: string): InteractionCandidate | undefined {
    const row = this.state.connection
      .prepare(
        `SELECT i.request_id AS requestId, i.attempt_id AS attemptId, a.node_id AS nodeId,
                s.submission_id AS submissionId, s.payload_hash AS payloadHash
         FROM interactive_requests i
         JOIN node_attempts a ON a.attempt_id = i.attempt_id
         JOIN interactive_submissions s ON s.submission_id = i.accepted_submission_id
         WHERE i.run_id = ? AND i.status = 'settled' AND i.consumed_at IS NULL
         ORDER BY i.settled_at DESC LIMIT 1`,
      )
      .get(runId);
    if (!isInteractionCandidateRow(row)) return undefined;
    return {
      requestId: row.requestId,
      attemptId: row.attemptId,
      nodeId: row.nodeId,
      submissionId: row.submissionId,
      payload: this.state.readJson(row.payloadHash),
    };
  }

  validatingInteraction(runId: string): InteractionCandidate | undefined {
    const row = this.state.connection
      .prepare(
        `SELECT i.request_id AS requestId, i.attempt_id AS attemptId, a.node_id AS nodeId,
                s.submission_id AS submissionId, s.payload_hash AS payloadHash
         FROM interactive_requests i
         JOIN node_attempts a ON a.attempt_id = i.attempt_id
         JOIN interactive_submissions s ON s.request_id = i.request_id
         WHERE i.run_id = ? AND i.status = 'pending'
           AND s.outcome = 'validating'
         ORDER BY s.submitted_at DESC LIMIT 1`,
      )
      .get(runId);
    if (!isInteractionCandidateRow(row)) return undefined;
    return {
      requestId: row.requestId,
      attemptId: row.attemptId,
      nodeId: row.nodeId,
      submissionId: row.submissionId,
      payload: this.state.readJson(row.payloadHash),
    };
  }

  /**
   * Parks a run for one origin-session interaction: durable request row plus
   * waiting attempt/run rows, mirroring the server's park path minus its queue claim.
   */
  parkInteraction(options: {
    runId: string;
    attemptId: string;
    targetSessionId: string;
    kind: WorkflowRequestKind;
    contract: JsonValue;
  }): InteractiveRequestRecord {
    const outer = options.contract;
    if (
      typeof outer !== "object" ||
      outer === null ||
      Array.isArray(outer) ||
      typeof (outer as { contract?: unknown }).contract !== "object"
    ) {
      throw new Error("Interactive contract does not match its durable request");
    }
    const requestId = workflowRequestId(options.runId, options.attemptId);
    const inner = (outer as { contract: Record<string, unknown> }).contract;
    if (
      inner.requestId !== requestId ||
      inner.runId !== options.runId ||
      inner.attemptId !== options.attemptId
    ) {
      throw new Error("Interactive contract does not match its durable request");
    }
    return this.state.transaction(() => {
      const request = ensureWorkflowRequest(this.state, {
        requestId,
        runId: options.runId,
        attemptId: options.attemptId,
        targetSessionId: options.targetSessionId,
        kind: options.kind,
        contract: options.contract,
      });
      const now = Date.now();
      this.state.connection
        .prepare(
          `UPDATE node_attempts SET status = 'waiting', updated_at = ?
           WHERE attempt_id = ? AND run_id = ? AND status = 'running'`,
        )
        .run(now, options.attemptId, options.runId);
      this.state.connection
        .prepare(
          `UPDATE runs SET status = 'waiting', status_detail = ?, updated_at = ?, finished_at = ?
           WHERE run_id = ? AND status = 'running'`,
        )
        .run("waiting for origin Pi session", now, null, options.runId);
      recordViewerDeltas(
        this.state,
        options.runId,
        [{ targetType: "summary" }, { targetType: "replay" }, { targetType: "conversation" }],
        now,
      );
      return request;
    });
  }

  beginValidation(options: {
    requestId: string;
    submissionId: string;
    idempotencyKey: string;
    expectedRevision: number;
    payload: JsonValue;
    receipt?: JsonValue;
  }): {
    interaction: InteractiveRequestRecord;
    submissionId: string;
    outcome: "accepted" | "adopted";
    receipt: JsonValue;
  } {
    return recordWorkflowSubmission(this.state, {
      ...options,
      outcome: "validating",
      settle: false,
    });
  }

  submitInteraction(options: {
    requestId: string;
    submissionId: string;
    idempotencyKey: string;
    expectedRevision: number;
    payload: JsonValue;
    accepted: boolean;
    receipt?: JsonValue;
  }): {
    interaction: InteractiveRequestRecord;
    submissionId: string;
    outcome: "accepted" | "adopted";
    receipt: JsonValue;
  } {
    return recordWorkflowSubmission(this.state, {
      ...options,
      outcome: options.accepted ? "accepted" : "rejected",
      settle: options.accepted,
    });
  }

  hasInteractionSubmission(requestId: string, idempotencyKey: string): boolean {
    return (
      this.state.connection
        .prepare(
          "SELECT 1 FROM interactive_submissions WHERE request_id = ? AND idempotency_key = ?",
        )
        .get(requestId, idempotencyKey) !== undefined
    );
  }

  finishValidation(options: {
    requestId: string;
    submissionId: string;
    accepted: boolean;
    receipt: JsonValue;
  }): { requestId: string; submissionId: string; outcome: string; receipt: JsonValue } {
    const now = Date.now();
    return this.state.transaction(() => {
      const submission = this.submission(options.requestId, options.submissionId);
      const expectedOutcome = options.accepted ? "accepted" : "rejected";
      if (submission?.outcome === expectedOutcome) return submission;
      if (submission === undefined || submission.outcome !== "validating") {
        throw new Error("Interactive submission is not awaiting validation");
      }
      const request = readWorkflowRequest(this.state, options.requestId);
      if (request === undefined) throw new Error("Workflow request is missing");
      const receiptHash = this.state.putJson(options.receipt, now);
      this.state.connection
        .prepare(
          `UPDATE interactive_submissions SET outcome = ?, receipt_hash = ?
           WHERE request_id = ? AND submission_id = ? AND outcome = 'validating'`,
        )
        .run(
          options.accepted ? "accepted" : "rejected",
          receiptHash,
          options.requestId,
          options.submissionId,
        );
      if (options.accepted) {
        const changed = this.state.connection
          .prepare(
            `UPDATE interactive_requests
             SET status = 'settled', accepted_submission_id = ?,
                 revision = revision + 1, updated_at = ?, settled_at = ?
             WHERE request_id = ? AND revision = ? AND status = 'pending'`,
          )
          .run(
            options.submissionId,
            now,
            now,
            options.requestId,
            request.revision,
          );
        if (changed.changes !== 1) throw new Error("Interactive request validation is stale");
        this.messages.cancelPendingForSource(options.requestId, "step", now);
        this.messages.cancelPendingForSource(options.requestId, "decision", now);
      }
      const settled = this.submission(options.requestId, options.submissionId);
      if (settled === undefined) throw new Error("Interactive submission result is missing");
      return settled;
    });
  }

  /**
   * Settles a rejected candidate and re-parks the run so the origin session
   * sees the interaction again, mirroring the server's rejected-submission path.
   */
  settleRejectedInteraction(options: {
    runId: string;
    requestId: string;
    submissionId: string;
    attemptId: string;
    error: string;
  }): void {
    const now = Date.now();
    this.state.transaction(() => {
      this.finishValidation({
        requestId: options.requestId,
        submissionId: options.submissionId,
        accepted: false,
        receipt: { status: "rejected", error: options.error },
      });
      this.state.connection
        .prepare(
          `UPDATE node_attempts SET status = 'waiting', updated_at = ?
           WHERE attempt_id = ? AND run_id = ? AND status IN ('running', 'interrupted')`,
        )
        .run(now, options.attemptId, options.runId);
      this.state.connection
        .prepare(
          `UPDATE runs SET status = 'waiting', status_detail = ?, updated_at = ?, finished_at = ?
           WHERE run_id = ? AND status = 'running'`,
        )
        .run(options.error, now, null, options.runId);
      recordViewerDeltas(
        this.state,
        options.runId,
        [{ targetType: "summary" }, { targetType: "replay" }, { targetType: "conversation" }],
        now,
      );
    });
  }

  private submission(requestId: string, submissionId: string) {
    const row = this.state.connection
      .prepare(
        `SELECT request_id AS requestId, submission_id AS submissionId,
                idempotency_key AS idempotencyKey, outcome, payload_hash AS payloadHash,
                receipt_hash AS receiptHash, submitted_at AS submittedAt
         FROM interactive_submissions WHERE request_id = ? AND submission_id = ?`,
      )
      .get(requestId, submissionId);
    if (!isSubmissionDetailRow(row) || row.requestId !== requestId) return undefined;
    return {
      requestId: row.requestId,
      submissionId: row.submissionId,
      idempotencyKey: row.idempotencyKey,
      outcome: row.outcome,
      payload: this.state.readJson(row.payloadHash),
      receipt: row.receiptHash === null ? null : this.state.readJson(row.receiptHash),
      submittedAt: new Date(row.submittedAt).toISOString(),
    };
  }
}

/**
 * Commits the interaction resume transition when a durable candidate exists,
 * and reports the attempt the engine should resume, mirroring the server's
 * `prepareInteractionResume` sequencing.
 */
export async function prepareInteractionResume(
  runStore: WorkflowRunStore,
  interactions: WorkerInteractionStore,
  runId: string,
): Promise<string | undefined> {
  const accepted = interactions.acceptedInteraction(runId);
  const candidate = accepted ?? interactions.validatingInteraction(runId);
  if (candidate === undefined) return undefined;
  const loaded = runStore.readRun(runId);
  if (loaded === null) return undefined;
  if (loaded.state.status !== "waiting") return undefined;
  loaded.state.status = "running";
  delete loaded.state.statusDetail;
  delete loaded.state.finishedAt;
  await runStore.commitTransition(runId, {
    kind: "resumeInteraction",
    event: {
      scope: "node",
      type: accepted === undefined ? "interaction_validation_started" : "interaction_accepted",
      nodeId: candidate.nodeId,
      attemptId: candidate.attemptId,
      payload: { requestId: candidate.requestId, submissionId: candidate.submissionId },
    },
  });
  return candidate.attemptId;
}
