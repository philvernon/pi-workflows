import { createHash } from "node:crypto";
import { RunParkedError } from "../workflows/errors.js";
import type {
  AgentStepContract,
  AgentStepExecutor,
  AgentStepPresentation,
  AgentStepRequest,
  AgentStepSubmission,
  AssistantMessageReceipt,
  ConversationRange,
} from "../workflows/types.js";
import type { WorkerInteractionStore } from "./interaction-store.js";

export type ParkedAgentRequest = {
  runId: string;
  requestId: string;
  contract: AgentStepContract;
  prompt: string;
  presentation?: AgentStepPresentation;
};

export type ValidationOutcome = {
  requestId: string;
  accepted: boolean;
  error?: string;
};

/**
 * Parks every agent step as a durable origin-session interaction and settles
 * candidate submissions inline, so one worker process replaces the
 * runner-to-server handoff the old architecture performed over RPC.
 */
export class ParkingExecutor implements AgentStepExecutor {
  readonly enforcesToolAllowlist = true as const;
  readonly assistantMessageMode = "visible" as const;
  readonly preservesActiveTimeBudget = true as const;

  constructor(
    private readonly interactions: WorkerInteractionStore,
    private readonly targetSessionId: string,
    private readonly onRequest: (request: ParkedAgentRequest) => void,
    private readonly onValidation: (outcome: ValidationOutcome) => void,
  ) {}

  async runAgentStep(request: AgentStepRequest, signal: AbortSignal): Promise<AgentStepSubmission> {
    if (signal.aborted) throw new Error("Agent step aborted before execution");
    const candidate = this.interactions.validatingInteraction(request.contract.runId);
    if (
      candidate !== undefined &&
      candidate.nodeId === request.contract.nodeId &&
      candidate.attemptId === request.contract.attemptId
    ) {
      const submission = interactionSubmission(candidate.payload);
      let value: AgentStepSubmission;
      if (request.contract.completion === "assistant") {
        const accepted = validateAcceptedAssistantSubmission(submission, request.contract);
        if (!accepted.ok) return await this.rejectCandidate(request.contract, accepted.error);
        value = accepted.value;
      } else {
        const accepted = await request.accept(submission.output);
        if (!accepted.ok) return await this.rejectCandidate(request.contract, accepted.error);
        value = { ...submission, output: accepted.value };
      }
      this.interactions.finishValidation({
        requestId: candidate.requestId,
        submissionId: candidate.submissionId,
        accepted: true,
        receipt: { status: "accepted" },
      });
      this.onValidation({ requestId: candidate.requestId, accepted: true });
      return value;
    }
    const requestRecord = this.interactions.parkInteraction({
      runId: request.contract.runId,
      attemptId: request.contract.attemptId,
      targetSessionId: this.targetSessionId,
      kind: request.contract.completion === "assistant" ? "assistant" : "agent",
      contract: {
        contract: request.contract,
        prompt: request.prompt,
        ...(request.presentation === undefined ? {} : { presentation: request.presentation }),
      },
    });
    this.onRequest({
      runId: request.contract.runId,
      requestId: requestRecord.requestId,
      contract: request.contract,
      prompt: request.prompt,
      ...(request.presentation === undefined ? {} : { presentation: request.presentation }),
    });
    throw new RunParkedError();
  }

  private async rejectCandidate(contract: AgentStepContract, error: string): Promise<never> {
    const candidate = this.interactions.validatingInteraction(contract.runId);
    if (candidate === undefined) throw new RunParkedError();
    this.interactions.settleRejectedInteraction({
      runId: contract.runId,
      requestId: candidate.requestId,
      submissionId: candidate.submissionId,
      attemptId: candidate.attemptId,
      error,
    });
    this.onValidation({ requestId: candidate.requestId, accepted: false, error });
    throw new RunParkedError();
  }
}

function interactionSubmission(payload: unknown): AgentStepSubmission {
  if (
    typeof payload === "object" &&
    payload !== null &&
    !Array.isArray(payload) &&
    Object.hasOwn(payload, "output")
  ) {
    return payload as AgentStepSubmission;
  }
  return { output: payload };
}

function validateAcceptedAssistantSubmission(
  submission: AgentStepSubmission,
  contract: AgentStepContract,
): { ok: true; value: AgentStepSubmission } | { ok: false; error: string } {
  if (contract.completion !== "assistant") {
    return { ok: false, error: "The interaction is not an assistant response" };
  }
  if (typeof submission.output !== "string" || submission.output.trim().length === 0) {
    return { ok: false, error: "Assistant response has no visible text" };
  }
  if (contract.maxOutputChars !== undefined && submission.output.length > contract.maxOutputChars) {
    return {
      ok: false,
      error: `Assistant response has ${submission.output.length} characters, above the configured limit of ${contract.maxOutputChars}`,
    };
  }
  const receipt: AssistantMessageReceipt | undefined = submission.assistantMessage;
  const conversation: ConversationRange | undefined = submission.conversation;
  const digest = createHash("sha256").update(submission.output).digest("hex");
  if (
    receipt === undefined ||
    receipt.sha256 !== digest ||
    typeof receipt.entryId !== "string" ||
    receipt.entryId.length === 0 ||
    receipt.recovered !== true ||
    receipt.maxChars !== contract.maxOutputChars ||
    conversation === undefined ||
    typeof conversation.firstEntryId !== "string" ||
    conversation.firstEntryId.length === 0 ||
    conversation.lastEntryId !== receipt.entryId
  ) {
    return { ok: false, error: "Assistant response receipt is invalid" };
  }
  return {
    ok: true,
    value: {
      output: submission.output,
      assistantMessage: receipt,
      conversation,
    },
  };
}
