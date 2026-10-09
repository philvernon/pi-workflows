import type { StateDatabase } from "../state/database.js";
import {
  workflowMessageIdFor,
  type WorkflowMessageStore,
} from "../state/workflow-messages.js";
import { notificationWorkflowMessageContent } from "../workflows/workflow-message-content.js";
import type {
  WorkflowNotificationReceipt,
  WorkflowNotificationRequest,
} from "../workflows/types.js";

export type NotificationRequestEvent = {
  runId: string;
  notificationRequestId: string;
  kind: "progress" | "final";
  content: string;
};

/**
 * Persists each notification as a durable pending `workflow_messages` row for
 * the origin session, then re-emits it to the host without waiting for delivery.
 */
export class WorkerNotificationSink {
  constructor(
    private readonly messages: WorkflowMessageStore,
    private readonly targetSessionId: string,
    private readonly onNotification: (event: NotificationRequestEvent) => void,
  ) {}

  notify(request: WorkflowNotificationRequest): WorkflowNotificationReceipt {
    const notificationId = `notification-${request.runId}-${request.attemptId}-${request.notificationIndex}`;
    const workflowMessageId = notificationMessageId(notificationId);
    const content = notificationWorkflowMessageContent({
      workflowMessageId,
      notificationId,
      runId: request.runId,
      kind: request.kind,
      content: request.content,
    });
    this.messages.create({
      workflowMessageId,
      runId: request.runId,
      targetSessionId: this.targetSessionId,
      kind: "notification",
      sourceId: notificationId,
      idempotencyKey: "initial",
      content,
    });
    this.onNotification({
      runId: request.runId,
      notificationRequestId: notificationId,
      kind: request.kind,
      content: request.content,
    });
    return { notificationId, targetSessionId: this.targetSessionId };
  }
}

export function markNotificationDelivered(
  state: StateDatabase,
  options: {
    notificationRequestId: string;
    ok: boolean;
    piSessionEntryId?: string;
  },
): void {
  if (!options.ok) return;
  const now = Date.now();
  state.connection
    .prepare(
      `UPDATE workflow_messages SET status = 'sent', pi_session_entry_id = ?, updated_at = ?
       WHERE workflow_message_id = ? AND status = 'pending'`,
    )
    .run(options.piSessionEntryId ?? null, now, notificationMessageId(options.notificationRequestId));
}

function notificationMessageId(notificationId: string): string {
  return workflowMessageIdFor("notification", notificationId, "initial");
}
