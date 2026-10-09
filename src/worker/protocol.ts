import type { JsonValue } from "../state/json.js";
import type {
  AgentStepContract,
  AgentStepPresentation,
  AssistantMessageReceipt,
  ConversationRange,
  WorkflowUpdateInput,
} from "../workflows/types.js";

export const WORKER_PROTOCOL_SCHEMA = "pi-workflows.worker-protocol.v1";

export type WorkerRunStatusFilter = "active" | "waiting" | "paused";

export type AgentSubmitPayload = {
  output: JsonValue;
  assistantMessage?: AssistantMessageReceipt;
  conversation?: ConversationRange;
};

export type HostMessage =
  | { type: "run.start"; workflow: string; input?: JsonValue }
  | { type: "run.list"; status?: WorkerRunStatusFilter }
  | { type: "run.get"; runId: string }
  | { type: "run.resume"; runId: string }
  | { type: "run.pause"; runId: string }
  | { type: "run.cancel"; runId: string }
  | { type: "run.restart"; runId: string; expectedRevision: number }
  | { type: "agent.submit"; requestId: string; submission: AgentSubmitPayload }
  | { type: "agent.update"; requestId: string; update: WorkflowUpdateInput }
  | {
      type: "notification.delivered";
      notificationRequestId: string;
      ok: boolean;
      error?: string;
      piSessionEntryId?: string;
    }
  | { type: "checkpoint.answer"; requestId: string; input?: JsonValue }
  | { type: "decision.answer"; requestId: string; response: JsonValue }
  | { type: "settings.patch"; runId: string; patch: JsonValue };

export type WorkerRequest = {
  schema: typeof WORKER_PROTOCOL_SCHEMA;
  id: string;
  message: HostMessage;
};

export type WorkerReply = {
  schema: typeof WORKER_PROTOCOL_SCHEMA;
  id: string;
  ok: boolean;
  result?: JsonValue;
  error?: string;
};

export type WorkerEvent =
  | { type: "run.started"; runId: string; state: JsonValue }
  | { type: "run.changed"; runId: string; seq: number; status: string }
  | {
      type: "agent.request";
      runId: string;
      requestId: string;
      contract: AgentStepContract;
      prompt: string;
      presentation?: AgentStepPresentation;
    }
  | { type: "checkpoint.request"; runId: string; requestId: string; contract: JsonValue }
  | { type: "decision.request"; runId: string; requestId: string; contract: JsonValue }
  | {
      type: "notification.request";
      runId: string;
      notificationRequestId: string;
      kind: "progress" | "final";
      content: string;
    }
  | { type: "run.finished"; runId: string; status: string; finalOutput?: JsonValue; error?: string };

export type WorkerEventFrame = {
  schema: typeof WORKER_PROTOCOL_SCHEMA;
  event: WorkerEvent;
};

export type WorkerFrame = WorkerRequest | WorkerReply | WorkerEventFrame;

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${field} must be a non-empty string`);
  }
  return value;
}

export function requireOptionalString(value: unknown, field: string): string | undefined {
  return value === undefined ? undefined : requireString(value, field);
}

export function requireNonNegativeInteger(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new Error(`${field} must be a non-negative integer`);
  }
  return value as number;
}

export function parseWorkerRequest(frame: unknown): WorkerRequest {
  if (!isRecord(frame)) throw new Error("Worker request must be an object");
  if (frame.schema !== WORKER_PROTOCOL_SCHEMA) throw new Error("Worker request schema is unknown");
  const id = requireString(frame.id, "id");
  const message = frame.message;
  if (!isRecord(message)) throw new Error("Worker request message must be an object");
  switch (message.type as HostMessage["type"]) {
    case "run.start":
      requireString(message.workflow, "workflow");
      return { schema: WORKER_PROTOCOL_SCHEMA, id, message: message as HostMessage };
    case "run.list": {
      const status = message.status;
      if (
        status !== undefined &&
        status !== "active" &&
        status !== "waiting" &&
        status !== "paused"
      ) {
        throw new Error("run.list status must be 'active', 'waiting' or 'paused'");
      }
      return { schema: WORKER_PROTOCOL_SCHEMA, id, message: message as HostMessage };
    }
    case "run.get":
    case "run.resume":
    case "run.pause":
    case "run.cancel":
      requireString(message.runId, "runId");
      return { schema: WORKER_PROTOCOL_SCHEMA, id, message: message as HostMessage };
    case "run.restart":
      requireString(message.runId, "runId");
      requireNonNegativeInteger(message.expectedRevision, "expectedRevision");
      return { schema: WORKER_PROTOCOL_SCHEMA, id, message: message as HostMessage };
    case "agent.submit":
      requireString(message.requestId, "requestId");
      if (!isRecord(message.submission)) throw new Error("submission must be an object");
      return { schema: WORKER_PROTOCOL_SCHEMA, id, message: message as HostMessage };
    case "agent.update":
      requireString(message.requestId, "requestId");
      if (message.update === undefined) throw new Error("update is required");
      return { schema: WORKER_PROTOCOL_SCHEMA, id, message: message as HostMessage };
    case "notification.delivered":
      requireString(message.notificationRequestId, "notificationRequestId");
      if (typeof message.ok !== "boolean") throw new Error("ok must be a boolean");
      requireOptionalString(message.error, "error");
      requireOptionalString(message.piSessionEntryId, "piSessionEntryId");
      return { schema: WORKER_PROTOCOL_SCHEMA, id, message: message as HostMessage };
    case "checkpoint.answer":
    case "decision.answer":
      requireString(message.requestId, "requestId");
      return { schema: WORKER_PROTOCOL_SCHEMA, id, message: message as HostMessage };
    case "settings.patch":
      requireString(message.runId, "runId");
      if (message.patch === undefined) throw new Error("patch is required");
      return { schema: WORKER_PROTOCOL_SCHEMA, id, message: message as HostMessage };
    default:
      throw new Error(`Unknown worker message type: ${String(message.type)}`);
  }
}
