import { normalizeAutoTitle } from '@cindy/maker-shared/session-title';
import { z } from 'zod';
import { creationOperation, remoteSessionCreationSchema, type RemoteSessionCreation, type RemoteSessionCreated } from '../../shared/remoteSessionCreation.js';
import type { CapturedAgentRequest } from '../maker-ipc/forwardedRequest.js';
import type { ForwardedRequestSource } from '../../shared/forwardedRequest.js';
import {
  DeviceLinkError,
  DL_SESSION_HANDOFF_CHANNEL,
  type InvokeResultPayload,
} from '@cindy/device-link';
import { issueRemoteTaskActionReceipt, releaseRemoteTaskActionReceipt, STOP_TURN_RECEIPT_BODY } from '../maker-ipc/remoteTaskActionReceipt.js';
import { TASK_STATUS_RECEIPT_OPERATION } from '../maker-ipc/taskDispatchReceipt.js';

/**
 * send_to_session 的跨电脑发送端：把消息交给目标电脑，由它用本机 send_to_session 的
 * 同一条通路投递到既有任务（恢复、排队、落库一致）。授权沿用同账号远程控制。
 * 不重试：没有回执时报 DELIVERY_UNKNOWN，由调用方先查再决定是否重发。
 */
export interface QualifiedSessionTarget {
  deviceId: string;
  sessionId: string;
}

export interface RemoteSessionHandoffRequest {
  sessionId: string;
  action?: 'create';
  create?: RemoteSessionCreation;
  message: string;
  sender: { sessionId: string; title?: string } | { kind: 'plugin'; pluginId: string; name?: string };
  requestSource?: ForwardedRequestSource;
  taskActionReceipt?: string;
  taskStatusReceipt?: true;
}

export type RemoteSessionHandoffResult =
  | ({
      ok: true;
      agentKind: 'claude-code' | 'codex' | 'pi' | 'antigravity';
      wakeKind: 'resumed' | 'already-active' | 'queued' | 'created';
      targetTitle: string | null;
      targetLastUserSendAt: string | null;
    } & Partial<RemoteSessionCreated>)
  | { ok: false; errorCode: RemoteSessionHandoffErrorCode; message: string };

export type RemoteSessionHandoffErrorCode =
  | 'NOT_FOUND'
  | 'ARCHIVED'
  | 'DELETED'
  | 'UNSUPPORTED_CAPABILITY'
  | 'INVALID_ARGS'
  | 'HOST_NOT_READY'
  | 'REMOTE_UNSUPPORTED'
  | 'REMOTE_DEVICE_OFFLINE'
  | 'REMOTE_DISABLED'
  | 'REMOTE_UNAVAILABLE'
  | 'DELIVERY_UNKNOWN'
  | 'CREATED_NOT_DELIVERED'
  | 'WORKTREE_UNAVAILABLE'
  | 'BUDGET_MODEL_REQUIRES_API_MODE'
  | 'PROVIDER_ROUTE_UNAVAILABLE';

/**
 * 目标电脑明确拒绝、消息没有进入目标任务的错误码（与本机 send_to_session 同名同义）。
 * AGENT_NOT_READY 不在其中：它可能发生在消息已记入目标任务之后，按结果未知处理。
 */
const TARGET_REJECTIONS = new Set<string>([
  'NOT_FOUND',
  'ARCHIVED',
  'DELETED',
  'UNSUPPORTED_CAPABILITY',
  'INVALID_ARGS',
  'HOST_NOT_READY',
  'WORKTREE_UNAVAILABLE',
  'BUDGET_MODEL_REQUIRES_API_MODE',
  'PROVIDER_ROUTE_UNAVAILABLE',
]);
const AGENT_KINDS = new Set(['claude-code', 'codex', 'pi', 'antigravity']);
const WAKE_KINDS = new Set(['resumed', 'already-active', 'queued']);

function encodedIpcCode(message: string): string | null {
  return /(?:^|:\s*(?:Error:\s*)?)\[([A-Z0-9_]+)]/.exec(message)?.[1] ?? null;
}

/** 只有能证明「请求没有到达对方业务处理」的错误才报未发出；其余一律按结果未知。 */
function transportFailure(
  code: string,
  message: string,
): RemoteSessionHandoffResult & { ok: false } {
  const actual = code === 'IPC_ERROR' ? (encodedIpcCode(message) ?? code) : code;
  switch (actual) {
    case 'CHANNEL_NOT_ALLOWED':
    case 'VERSION_MISMATCH':
      return {
        ok: false,
        errorCode: 'REMOTE_UNSUPPORTED',
        message: '目标电脑的版本不支持接收其它电脑的任务消息，消息没有发出。',
      };
    case 'DEVICE_OFFLINE':
      return {
        ok: false,
        errorCode: 'REMOTE_DEVICE_OFFLINE',
        message: '目标电脑不在线，消息没有发出。',
      };
    case 'REMOTE_DISABLED':
    case 'DEVICE_LINK_CONTROL_DISABLED':
    case 'ACCESS_REVOKED':
    case 'PERMISSION_DENIED':
      return {
        ok: false,
        errorCode: 'REMOTE_DISABLED',
        message: '目标电脑未允许本机远程控制，消息没有发出。',
      };
    case 'LINK_NOT_OPEN':
    case 'NOT_CONNECTED':
    case 'BACKPRESSURE':
    case 'DEVICE_LINK_NOT_CONNECTED':
    case 'DEVICE_LINK_STANDBY':
      return {
        ok: false,
        errorCode: 'REMOTE_UNAVAILABLE',
        message: '暂时无法连接目标电脑，消息没有发出。',
      };
    // 隧道层的 NOT_FOUND 不在此列：被控端在 handler 完成后复核可见性与账号时也抛它，
    // 无法与投递前的拒绝区分，落到下面的结果未知。目标任务不存在由 handler 以数据返回。
    case 'INVALID_PARAMS':
      return { ok: false, errorCode: 'INVALID_ARGS', message: '目标电脑拒绝了这条消息的参数。' };
    default:
      return {
        ok: false,
        errorCode: 'DELIVERY_UNKNOWN',
        message: '没有收到目标电脑的回执，消息可能已送达。重发前请先读取该任务的聊天记录确认。',
      };
  }
}

interface RemoteHandoffTransport {
  captureAccess(): () => void;
  invoke(deviceId: string, channel: string, args: unknown[], options?: { preSend?: () => void }): Promise<InvokeResultPayload>;
}

const turnFailure = z.object({
  ok: z.literal(false),
  errorCode: z.enum(['NOT_FOUND', 'NO_ACTIVE_TURN', 'UNSUPPORTED_CAPABILITY', 'DELIVERY_FAILED']),
  message: z.string(),
});
const steerReply = z.union([turnFailure, z.object({ ok: z.literal(true), queuedMessageId: z.string().min(1) })]);
const stopReply = z.union([turnFailure.omit({ errorCode: true }).extend({ errorCode: z.enum(['NOT_FOUND', 'UNSUPPORTED_CAPABILITY']) }),
  z.object({
    ok: z.literal(true),
    status: z.enum(['no-active-turn', 'waiting-for-safe-point', 'requested', 'unconfirmed']),
    turnGeneration: z.number().int().nonnegative().optional(),
    reason: z.string().optional(),
  })]);

export function createRemoteSessionTurnControl(deps: RemoteHandoffTransport) {
  async function invokeTurn<T>(
    targetId: string, sender: () => Promise<{ sessionId: string; title?: string } | null>,
    action: 'steer' | 'stop-turn', message: string, schema: z.ZodType<T>,
  ) {
    const separator = targetId.indexOf('::');
    const target = { deviceId: targetId.slice(0, separator), sessionId: targetId.slice(separator + 2) };
    if (separator <= 0 || !target.sessionId || target.sessionId.includes('::') || target.sessionId.length > 512
      || !message.trim() || message.length > 200_000) {
      return { ok: false as const, errorCode: 'INVALID_ARGS' as const, message: 'Invalid remote task or message.' };
    }
    const unavailable = { ok: false as const, errorCode: 'REMOTE_UNAVAILABLE' as const,
      message: 'Could not check the target computer. No turn control request was sent.' };
    const unsupported = { ok: false as const, errorCode: 'UNSUPPORTED_CAPABILITY' as const,
      message: 'Update the target computer to use cross-computer turn control.' };
    const unknown = { ok: false as const, errorCode: 'DELIVERY_UNKNOWN' as const,
      message: 'Could not confirm the action. Check the current task state before retrying.' };
    const probeFailure = (code: string, message: string) => {
      const { errorCode } = transportFailure(code, message);
      return errorCode === 'REMOTE_UNSUPPORTED' || errorCode === 'INVALID_ARGS' ? unsupported : unavailable;
    };
    let receiptId: string | undefined;
    try {
      const assertCurrent = deps.captureAccess();
      const actor = await sender();
      if (!actor) return { ...unsupported, message: 'Cross-computer turn control requires a sending task.' };
      const support = await deps.invoke(target.deviceId, DL_SESSION_HANDOFF_CHANNEL,
        [{ action: 'task-action-capability', sessionId: target.sessionId }], { preSend: assertCurrent });
      assertCurrent();
      if (!support.ok) return probeFailure(support.error.code, support.error.message);
      const capabilities = support.result as { taskActionVersion?: unknown; turnControlVersion?: unknown } | null;
      if (capabilities?.taskActionVersion !== 1 || capabilities.turnControlVersion !== 1) return unsupported;
      receiptId = issueRemoteTaskActionReceipt(
        { ...target, senderSessionId: actor.sessionId, operation: action, message }, assertCurrent, 'immediate',
      );
      const response = await deps.invoke(target.deviceId, DL_SESSION_HANDOFF_CHANNEL,
        [{ action, sessionId: target.sessionId, message, sender: actor, taskActionReceipt: receiptId }], { preSend: assertCurrent });
      assertCurrent();
      if (!response.ok) return unknown;
      const result = schema.safeParse(response.result);
      return result.success ? result.data : unknown;
    } catch (error) {
      if (!receiptId && error instanceof DeviceLinkError) return probeFailure(error.code, error.message);
      if (receiptId && error instanceof DeviceLinkError && error.inFlight === false) return unavailable;
      return receiptId ? unknown : unavailable;
    } finally {
      // Turn actions are immediate. Loss of the reply never grants a queued input
      // authority to run in a later turn.
      if (receiptId) releaseRemoteTaskActionReceipt(receiptId);
    }
  }
  return {
    steer: (target: string, sender: () => Promise<{ sessionId: string; title?: string } | null>, message: string) =>
      invokeTurn(target, sender, 'steer', message, steerReply),
    stop: (target: string, sender: () => Promise<{ sessionId: string; title?: string } | null>) =>
      invokeTurn(target, sender, 'stop-turn', STOP_TURN_RECEIPT_BODY, stopReply),
  };
}

export function createRemoteSessionHandoff(deps: RemoteHandoffTransport & {
  captureRequest?: (sessionId: string, supplement: string, target?: { sessionId?: string }) => Promise<CapturedAgentRequest>;
}) {
  return async (
    target: QualifiedSessionTarget,
    request: {
      message: string;
      /** 在账号记录之后读取，使来源查询也落在同一道 owner 检查之内。 */
      sender(): Promise<RemoteSessionHandoffRequest['sender']>;
      create?(): Promise<RemoteSessionCreation>;
      /** Host-generated summary without forwarded user authority. */
      taskStatusReceipt?: true;
    },
  ): Promise<RemoteSessionHandoffResult> => {
    const notSent = (): RemoteSessionHandoffResult => ({
      ok: false,
      errorCode: 'HOST_NOT_READY',
      message: '账号正在切换，消息没有发出。',
    });
    // 同一份账号记录覆盖整次调用：读来源之前记录，发送前与收到回执后各核对一次。
    let assertCurrent: () => void;
    let sender: RemoteSessionHandoffRequest['sender'];
    let captured: CapturedAgentRequest | undefined;
    let creation: RemoteSessionCreation | undefined;
    try {
      assertCurrent = deps.captureAccess();
      sender = await request.sender();
      assertCurrent();
    } catch {
      return notSent();
    }
    if (request.create) {
      try {
        creation = remoteSessionCreationSchema.parse(await request.create());
      } catch (error) {
        try { assertCurrent(); } catch { return notSent(); }
        const code = error && typeof error === 'object' && 'code' in error ? String(error.code) : undefined;
        const message = error instanceof Error ? error.message : 'Invalid remote task creation request';
        if (code === 'PROVIDER_ROUTE_UNAVAILABLE' || code === 'UNSUPPORTED_CAPABILITY' || code === 'INVALID_ARGS') {
          return { ok: false, errorCode: code, message };
        }
        if (message.includes('ANTIGRAVITY_LOCAL_ONLY')) {
          return { ok: false, errorCode: 'PROVIDER_ROUTE_UNAVAILABLE', message };
        }
        return { ok: false, errorCode: 'INVALID_ARGS', message };
      }
    }
    try {
      if ('sessionId' in sender && !request.taskStatusReceipt) {
        captured = await deps.captureRequest?.(sender.sessionId, request.message, { sessionId: target.sessionId });
      }
      assertCurrent();
    } catch {
      return notSent();
    }
    let response: InvokeResultPayload;
    let taskActionReceipt: string | undefined;
    let mutationStarted = false;
    const releaseReceipt = () => {
      if (taskActionReceipt) releaseRemoteTaskActionReceipt(taskActionReceipt);
    };
    const finishTransportFailure = (code: string, message: string, responseReceived = false) => {
      // The remote DB can overload while reading a receipt after its write.
      const actual = code === 'IPC_ERROR' ? encodedIpcCode(message) : code;
      const result = transportFailure(responseReceived && actual === 'BACKPRESSURE' ? 'UNKNOWN' : code, message);
      if (result.errorCode !== 'DELIVERY_UNKNOWN') releaseReceipt();
      return result;
    };
    const capabilityProbeFailure = (code: string, message: string) => {
      const failure = transportFailure(code, message);
      return creation && failure.errorCode === 'DELIVERY_UNKNOWN'
        ? { ok: false as const, errorCode: 'REMOTE_UNAVAILABLE' as const, message: 'Could not check the destination capability. No creation request was sent.' }
        : failure;
    };
    try {
      // Old Hosts cannot recheck queued authority. Probe before sending any content.
      const support = await deps.invoke(target.deviceId, DL_SESSION_HANDOFF_CHANNEL,
        [{ action: 'task-action-capability', sessionId: target.sessionId }], { preSend: assertCurrent });
      if (!support.ok) return capabilityProbeFailure(support.error.code, support.error.message);
      const capability = support.result as {
        taskActionVersion?: unknown; plainRequestBody?: unknown; ordinaryCreateVersion?: unknown;
        ordinaryCreateAutoTitle?: unknown; pluginCaller?: unknown; taskStatusReceipt?: unknown;
      } | null;
      if (capability?.taskActionVersion !== 1)
        return { ok: false, errorCode: 'REMOTE_UNSUPPORTED', message: 'The other computer must support task-action authorization before a message can be sent.' };
      if (creation && capability.ordinaryCreateVersion !== 1)
        return { ok: false, errorCode: 'REMOTE_UNSUPPORTED', message: 'Update the destination computer to support ordinary task creation. No task was created.' };
      if (request.taskStatusReceipt && capability.taskStatusReceipt !== 1)
        return { ok: false, errorCode: 'REMOTE_UNSUPPORTED', message: 'The other computer does not accept automatic task status receipts.' };
      if ('pluginId' in sender && capability.pluginCaller !== 1)
        return { ok: false, errorCode: 'REMOTE_UNSUPPORTED', message: 'The other computer does not support Plugin callers; no message was sent.' };
      assertCurrent();
      // Older hosts verify the quoted 用户原话 / Agent补充 envelope; keep it for them unchanged.
      const delivered = capability.plainRequestBody === 1 || !captured?.legacy ? captured : { ...captured, ...captured.legacy };
      const body = delivered?.message ?? request.message;
      const source = delivered?.autoReviewUserText.request;
      // Bind the fallback to the same receipt as the create that older hosts will persist.
      if (creation && !creation.title?.trim() && 'sessionId' in sender && capability.ordinaryCreateAutoTitle !== 1) {
        creation = { ...creation, title: normalizeAutoTitle(request.message) };
      }
      taskActionReceipt = issueRemoteTaskActionReceipt({ ...target, ...('pluginId' in sender
        ? { senderPluginId: sender.pluginId } : { senderSessionId: sender.sessionId }), message: body,
        ...(creation ? { operation: creationOperation(creation) }
          : request.taskStatusReceipt ? { operation: TASK_STATUS_RECEIPT_OPERATION } : {}) }, assertCurrent);
      const wire: RemoteSessionHandoffRequest = {
        sessionId: target.sessionId,
        ...(creation ? { action: 'create' as const, create: creation } : {}),
        ...(request.taskStatusReceipt ? { taskStatusReceipt: true as const } : {}),
        message: body,
        // A plain body sends only a checked hand-off; its provenance stays on this computer.
        ...(source && (source.plainBody ? source.delegatedText !== undefined : source.userText != null)
          ? { requestSource: source } : {}),
        sender,
        taskActionReceipt,
      };
      mutationStarted = true;
      response = await deps.invoke(target.deviceId, DL_SESSION_HANDOFF_CHANNEL, [wire], {
        preSend: assertCurrent,
      });
      // Queued/unknown deliveries retain their receipt; completed or proven-unsent
      // attempts release it so offline retries cannot exhaust the live receipt cap.
      if (response.ok && (response.result as { ok?: boolean; wakeKind?: string } | null)?.ok === true
        && ['resumed', 'already-active'].includes((response.result as { wakeKind: string }).wakeKind)) releaseReceipt();
    } catch (error) {
      if (creation && !mutationStarted) {
        return error instanceof DeviceLinkError ? capabilityProbeFailure(error.code, error.message)
          : capabilityProbeFailure('IPC_ERROR', error instanceof Error ? error.message : '');
      }
      // inFlight = 请求帧已发出、只是回执丢了：对端可能已执行，不能报「没有发出」。
      if (error instanceof DeviceLinkError) {
        if (error.inFlight === false) releaseReceipt();
        return finishTransportFailure(error.inFlight ? 'UNKNOWN' : error.code, error.message);
      }
      // 发送前的本机拒绝（另一实例持有连接、本机关闭了对该设备的控制等）以 [CODE] 前缀抛出。
      return finishTransportFailure('IPC_ERROR', error instanceof Error ? error.message : '');
    }
    try {
      assertCurrent();
    } catch {
      // 请求已发出、账号随后切换：回执属于上一个账号，不读取也不返回其中的任务信息。
      return transportFailure('UNKNOWN', '');
    }
    if (!response.ok) return finishTransportFailure(response.error.code, response.error.message, true);
    const result = response.result as Record<string, unknown> | null;
    if (!result || typeof result !== 'object' || Array.isArray(result))
      return transportFailure('UNKNOWN', '');
    if (result.ok === false) {
      if (creation && result.errorCode === 'CREATED_NOT_DELIVERED') {
        releaseReceipt();
        return { ok: false, errorCode: 'CREATED_NOT_DELIVERED', message: 'The task was created but message delivery was not confirmed. Inspect its history and queue before sending again.' };
      }
      // 其它错误（如目标电脑的内部错误）可能发生在消息已记入目标任务之后，不能说没有发出。
      if (typeof result.errorCode !== 'string' || !TARGET_REJECTIONS.has(result.errorCode)) {
        return transportFailure('UNKNOWN', '');
      }
      releaseReceipt();
      return {
        ok: false,
        errorCode: result.errorCode as RemoteSessionHandoffErrorCode,
        message:
          typeof result.message === 'string'
            ? result.message.slice(0, 500)
            : '目标电脑没有接受这条消息。',
      };
    }
    if (
      result.ok !== true ||
      !AGENT_KINDS.has(String(result.agentKind)) ||
      !(creation ? result.wakeKind === 'created' && result.targetSessionId === target.sessionId : WAKE_KINDS.has(String(result.wakeKind)))
    ) {
      return transportFailure('UNKNOWN', '');
    }
    return {
      ...(creation ? {
        targetSessionId: target.sessionId,
        projectDir: typeof result.projectDir === 'string' ? result.projectDir : null,
        workspaceKind: result.workspaceKind === 'project' ? 'project' as const : 'dialogue' as const,
        workingDir: typeof result.workingDir === 'string' ? result.workingDir : '',
        worktreePath: typeof result.worktreePath === 'string' ? result.worktreePath : null,
        model: typeof result.model === 'string' ? result.model : undefined,
        providerId: typeof result.providerId === 'string' ? result.providerId : null,
        effort: remoteSessionCreationSchema.shape.effort.safeParse(result.effort).data ?? null,
        fastMode: remoteSessionCreationSchema.shape.fast.safeParse(result.fastMode).data ?? false,
      } : {}),
      ok: true,
      agentKind: result.agentKind as 'claude-code' | 'codex' | 'pi' | 'antigravity',
      wakeKind: result.wakeKind as 'resumed' | 'already-active' | 'queued' | 'created',
      targetTitle: typeof result.targetTitle === 'string' ? result.targetTitle : null,
      targetLastUserSendAt:
        typeof result.targetLastUserSendAt === 'string' ? result.targetLastUserSendAt : null,
    };
  };
}
