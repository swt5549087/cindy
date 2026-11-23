import { createHash } from 'node:crypto';
import { redactSensitiveText } from '@cindy/maker-shared/error-redaction';
import type { AgentInputQueuedMessage } from '../../shared/agentInputQueue.js';
import { truncateSummary } from '../mcp-integrations/taskPhase.js';

/** Bind the remote authorization to the status-only effect, rather than an ordinary send. */
export const TASK_STATUS_RECEIPT_OPERATION = 'task-status-receipt';

export function taskDispatchClientId(sessionId: string, remote = false): string {
  // Remote queue restoration treats the authorization as ephemeral.
  return `${remote ? 'remote-task-action:' : ''}task-dispatch:${sessionId}`;
}

export interface TaskDispatchInput {
  clientId: string;
  origin: Extract<NonNullable<AgentInputQueuedMessage['origin']>, { kind: 'session' }>;
}

export interface TaskDispatchOutcome {
  status: 'finished' | 'failed' | 'cancelled' | 'interrupted' | 'blocked';
  text?: string;
  assistantId?: string;
}

export type FinishTaskDispatch = (outcome: TaskDispatchOutcome) => Promise<void>;

export function taskDispatchInput(
  item: Pick<AgentInputQueuedMessage, 'clientId' | 'origin' | 'retrySourceClientId' | 'supersedesUserClientId' | 'autoResume'> | undefined,
): TaskDispatchInput | undefined {
  if (item?.origin?.kind !== 'session' || item.origin.taskDispatch !== true
    || !item.origin.senderSessionId || item.origin.automaticTaskReceipt) return;
  const retryOf = item.retrySourceClientId ?? item.supersedesUserClientId;
  if (retryOf && !item.autoResume) return;
  return { clientId: retryOf ?? item.clientId, origin: item.origin };
}

export function readTaskDispatchParent(agentMeta: string | null, sessionId: string): string | undefined {
  if (!agentMeta) return;
  try {
    const origin = JSON.parse(agentMeta)?.origin;
    if (origin?.kind === 'session' && origin.taskDispatch === true && origin.dispatchedChildSessionId === sessionId
      && typeof origin.senderSessionId === 'string' && origin.senderSessionId !== sessionId) return origin.senderSessionId;
  } catch { /* Legacy messages may have no parseable metadata. */ }
}

/** Renderer input may keep its display origin, but cannot mint a reply binding. */
export function stripTaskDispatchOrigin<T>(origin: T): T {
  if (!origin || typeof origin !== 'object' || !('kind' in origin) || origin.kind !== 'session') return origin;
  const sanitized = { ...origin } as T & Record<string, unknown>;
  delete sanitized.taskDispatch;
  delete sanitized.dispatchedChildSessionId;
  delete sanitized.automaticTaskReceipt;
  return sanitized;
}

interface ReceiptAccess {
  readTask(sessionId: string, input: TaskDispatchInput, assistantId?: string): Promise<{
    parentSessionId?: string;
    childAddress?: string;
    title?: string | null;
    assistantText?: string;
  } | undefined>;
  deliver(receipt: { childSessionId: string; parentSessionId: string; title?: string | null; clientId: string; message: string }): Promise<void>;
}

interface DispatchTurn {
  clientId: string;
  outbound: Set<Promise<void>>;
  sent: boolean;
  finished: boolean;
}

export function createTaskDispatchReceipts(deps: {
  captureAccess(): ReceiptAccess | undefined;
  warn(sessionId: string): void;
}) {
  const turns = new Map<string, DispatchTurn>();
  const getTurn = (sessionId: string, clientId: string): DispatchTurn => {
    const existing = turns.get(sessionId);
    if (existing?.clientId === clientId) return existing;
    const turn: DispatchTurn = { clientId, outbound: new Set(), sent: false, finished: false };
    turns.set(sessionId, turn);
    return turn;
  };
  return {
    trackSend<T extends { ok: boolean }>(
      sessionId: string, input: TaskDispatchInput | undefined, targetSessionId: string, send: () => Promise<T>,
    ): Promise<T> {
      if (!input || input.origin.senderSessionId !== targetSessionId) return send();
      const turn = getTurn(sessionId, input.clientId);
      // Register before entering the delivery code. A terminal may arrive while it is awaiting acceptance.
      const result = Promise.resolve().then(send);
      const accepted = result.then(result => {
        // An uncertain remote delivery may already have reached the parent.
        turn.sent ||= result.ok || ('errorCode' in result && result.errorCode === 'DELIVERY_UNKNOWN');
      }, () => {});
      turn.outbound.add(accepted);
      void accepted.then(() => turn.outbound.delete(accepted));
      return result;
    },
    capture(sessionId: string, input: TaskDispatchInput | undefined): FinishTaskDispatch | undefined {
      if (!input || input.origin.senderSessionId === sessionId) return;
      const access = deps.captureAccess();
      if (!access) return;
      const turn = getTurn(sessionId, input.clientId);
      return async outcome => {
        if (turn.finished) return;
        turn.finished = true;
        try {
          await Promise.all(turn.outbound);
          if (turn.sent) return;
          const task = await access.readTask(sessionId, input, outcome.assistantId);
          const parentSessionId = input.origin.senderSessionId;
          if (!task || task.parentSessionId !== parentSessionId) return;
          const title = truncateSummary(redactSensitiveText((task.title || 'Untitled task').slice(0, 1024)), 160);
          const text = outcome.status === 'finished'
            ? task.assistantText?.trim() || outcome.text?.trim() || ''
            : outcome.text?.trim() || task.assistantText?.trim() || '';
          const summary = truncateSummary(redactSensitiveText(text.slice(0, 4096)), 800);
          const clientId = 'task-status:' + createHash('sha256').update(JSON.stringify([sessionId, input.clientId])).digest('hex');
          const message = [
            'Automatic task status receipt',
            `session_id: ${task.childAddress ?? sessionId}`,
            `title: ${JSON.stringify(title)}`,
            `status: ${outcome.status}`,
            ...(summary ? [`summary: ${JSON.stringify(summary)}`] : []),
          ].join('\n');
          await access.deliver({ childSessionId: sessionId, parentSessionId, title, clientId, message });
        } catch {
          deps.warn(sessionId);
        }
      };
    },
    clear(sessionId: string) {
      turns.delete(sessionId);
    },
  };
}
