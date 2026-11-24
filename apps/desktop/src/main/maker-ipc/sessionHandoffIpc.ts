import { getCurrentDbClientSnapshot } from '../localDb/client/current.js';
import { normalizeAutoTitle } from '@cindy/maker-shared/session-title';
import { z } from 'zod';
import { remoteSessionCreationSchema, creationOperation, type RemoteSessionCreated } from '../../shared/remoteSessionCreation.js';
import type { createRemoteSessionOpener } from './remoteSessionCreation.js';
import { ipcMain } from 'electron';
import { matchesForwardedRequest, readForwardedRequestSource, type AgentRequestReceipt } from '../../shared/forwardedRequest.js';
import { DL_SESSION_HANDOFF_CHANNEL } from '@cindy/device-link';
import { sanitizeSourceName } from '@cindy/maker-shared/message-source';
import type { AgentInputQueuedMessage } from '../../shared/agentInputQueue.js';
import { getDeviceLinkInvokeContext } from '../device-link/invoke-context.js';
import { assertRemoteBotInvocationAllowed, hasRemoteBotSessionLookup } from '../device-link/remoteBotSessionBoundary.js';
import { requireObject, throwIpcError } from '../utils/ipcValidate.js';
import type { TaskActionDeliveryGuard } from './taskActionAuthorization.js';
import { STOP_TURN_RECEIPT_BODY, validateRemoteTaskActionReceipt } from './remoteTaskActionReceipt.js';
import { TASK_STATUS_RECEIPT_OPERATION, taskDispatchClientId } from './taskDispatchReceipt.js';
import type { SessionSteerResult, SessionStopResult } from './sessionControlService.js';

/**
 * `maker:session:handoff` 的被控端业务体：同账号另一台电脑上的任务，把一条消息发给
 * 本机的普通任务（可显式创建）。投递复用本机 send_to_session 的同一条通路（恢复、繁忙排队、落库）。
 *
 * - 不带 action 只投递既有任务；action=create 创建普通任务并标记首条输入的发起会话。
 * - 来源只用于归属展示，不是权限判据：发送方任务 id 加上**服务端认证的控制端设备 id**
 *   前缀（`deviceId::sessionId`，接收方可原样用它回信）；标题来自对端，按不可信文本清洗。
 * - 恢复不在运行的目标任务时按它落库的权限档启动，另一台电脑的来信不提权。
 */
const MAX_MESSAGE_CHARS = 200_000;

export interface SessionHandoffSendParams {
  autoReviewUserText: AgentRequestReceipt | string;
  clientId?: string;
  forceQueue?: boolean;
  taskActionDeliveryGuard?: TaskActionDeliveryGuard;
  targetSessionId: string;
  message: string;
  origin?: NonNullable<AgentInputQueuedMessage['origin']>;
  sourcePlugin?: AgentInputQueuedMessage['sourcePlugin'];
  inheritTargetPlanMode?: true;
  resumeWithStoredPermissionMode: true;
}

export type SessionHandoffSendResult =
  | ({
      ok: true;
      agentKind: string;
      wakeKind: 'resumed' | 'already-active' | 'created' | 'queued';
      targetTitle: string | null;
      targetLastUserSendAt: string | null;
    } & Partial<Omit<RemoteSessionCreated, 'agentKind'>>)
  | { ok: false; errorCode: string; message: string };

function boundedString(value: unknown, name: string, max: number): string {
  if (typeof value !== 'string' || !value.trim() || value.length > max) {
    throwIpcError('INVALID_PARAMS', `Invalid ${name}`);
  }
  return value as string;
}

export function createSessionHandoffHandler(deps: {
  send(params: SessionHandoffSendParams): Promise<SessionHandoffSendResult>;
  create?: ReturnType<typeof createRemoteSessionOpener>;
  refineTitle?(sessionId: string, subject: string, initialTitle: string): void;
  listProjects?(options: { includeHidden: boolean; offset: number; limit: number }): Promise<unknown>;
  canReceiveTaskStatusReceipt?(target: string): boolean;
  captureTurnControlOwner?(): () => void;
  turnControl?(input: {
    action: 'steer' | 'stop-turn';
    targetSessionId: string;
    senderSessionId: string;
    senderSessionTitle?: string;
    message: string;
    receiptId: string;
    guard: TaskActionDeliveryGuard;
    beforeProviderStart: () => Promise<void>;
  }): Promise<SessionSteerResult | SessionStopResult>;
  validate?(controllerDeviceId: string, input: { receiptId: string; sessionId?: string; pluginId?: string; targetSessionId: string; message: string; release?: boolean; operation?: string }): Promise<boolean>;
}) {
  // Claim before the first await. The sender receipt expires after one hour; retain
  // claims longer so transport replay cannot enqueue a second copy in that lifetime.
  const claimed = new Map<string, number>();
  return async (controllerDeviceId: string, value: unknown): Promise<SessionHandoffSendResult | SessionSteerResult | SessionStopResult> => {
    const request = requireObject(value, 'request');
    const targetSessionId = boundedString(request.sessionId, 'sessionId', 512);
    const turnAction = request.action === 'steer' || request.action === 'stop-turn' ? request.action : undefined;
    const creation = request.action === 'create' ? remoteSessionCreationSchema.parse(request.create) : undefined;
    if (request.action !== undefined && request.action !== 'create' && !turnAction) throwIpcError('INVALID_PARAMS', 'Unknown handoff action');
    if (creation && (!deps.create || !z.uuid().safeParse(targetSessionId).success))
      return { ok: false, errorCode: 'INVALID_ARGS', message: 'Remote creation requires a new UUID and a supported host.' };
    const automaticReceipt = request.taskStatusReceipt === true;
    if (request.taskStatusReceipt !== undefined && (!automaticReceipt || creation || request.requestSource !== undefined))
      throwIpcError('INVALID_PARAMS', 'Invalid automatic task status receipt');
    const operation = creation ? creationOperation(creation) : turnAction ?? (automaticReceipt ? TASK_STATUS_RECEIPT_OPERATION : undefined);
    // 目标必须是本机任务；带设备前缀的 id 属于别的电脑，不代为转发。
    if (targetSessionId.includes('::'))
      throwIpcError('INVALID_PARAMS', 'Handoff cannot forward to another device');
    const body = boundedString(request.message, 'message', MAX_MESSAGE_CHARS);
    // Same-account host transport, never an Agent tool argument. Old peers have no receipt.
    const requestSource = readForwardedRequestSource(request.requestSource);
    if (request.requestSource !== undefined && (!requestSource || !matchesForwardedRequest(requestSource, body)))
      throwIpcError('INVALID_PARAMS', 'Request source does not match the forwarded body');
    const message = body;
    const sender = requireObject(request.sender, 'sender');
    // 任务 id 是不透明字符串（IM 任务的 id 可以很长）；只拒绝会伪造设备前缀或说明结构的写法。
    const plugin = sender.kind === 'plugin';
    if (turnAction && (plugin || request.requestSource !== undefined || request.taskStatusReceipt !== undefined
      || request.create !== undefined || (turnAction === 'stop-turn' && body !== STOP_TURN_RECEIPT_BODY))) {
      throwIpcError('INVALID_PARAMS', 'Invalid remote turn action');
    }
    const senderPluginId = plugin ? boundedString(sender.pluginId, 'sender.pluginId', 512) : undefined;
    const senderSessionId = plugin ? undefined : boundedString(sender.sessionId, 'sender.sessionId', 512);
    const actorId = senderPluginId ?? senderSessionId!;
    if (actorId.includes('::') || /[\s()「」]/.test(actorId) || (plugin && (sender.sessionId !== undefined || request.requestSource !== undefined || automaticReceipt))) {
      throwIpcError('INVALID_PARAMS', 'Invalid sender.sessionId');
    }
    const senderSessionTitle = sanitizeSourceName(sender.title);
    const senderPluginName = plugin ? sanitizeSourceName(sender.name) : undefined;
    const actor = plugin ? { pluginId: senderPluginId! } : { sessionId: senderSessionId! };
    if (typeof request.taskActionReceipt !== 'string' || !deps.validate) {
      return { ok: false, errorCode: 'UNSUPPORTED_CAPABILITY', message: 'Task action authorization is required; update the sending computer.' };
    }
    if (automaticReceipt && deps.canReceiveTaskStatusReceipt?.(targetSessionId) !== true) {
      return { ok: false, errorCode: 'UNSUPPORTED_CAPABILITY', message: 'The task does not accept automatic status receipts.' };
    }
    const receiptId = boundedString(request.taskActionReceipt, 'taskActionReceipt', 128);
    if (turnAction && (!deps.turnControl || !deps.captureTurnControlOwner)) {
      return { ok: false, errorCode: 'UNSUPPORTED_CAPABILITY', message: 'Remote turn control is unavailable.' };
    }
    for (const [key, expires] of claimed) if (expires <= Date.now()) claimed.delete(key);
    const claimKey = JSON.stringify([controllerDeviceId, receiptId]);
    if (claimed.has(claimKey) || claimed.size >= 1024)
      return { ok: false, errorCode: creation ? 'DELIVERY_UNKNOWN' : 'UNSUPPORTED_CAPABILITY', message: 'Task action already claimed or pending limit reached; inspect the target before retrying.' };
    claimed.set(claimKey, Date.now() + 2 * 60 * 60_000);
    if (turnAction) {
      const assertOwner = deps.captureTurnControlOwner!();
      let active = true;
      let authorized: Promise<boolean> | undefined;
      let consumed = false;
      const verify = (release: boolean) => deps.validate!(controllerDeviceId, {
        receiptId, ...actor, targetSessionId, message: body, operation: turnAction, release,
      });
      const assertLocal = async () => {
        assertOwner();
        await assertRemoteBotInvocationAllowed([{ sessionId: targetSessionId }], DL_SESSION_HANDOFF_CHANNEL);
        assertOwner();
        if (!active) throw new Error('TASK_ACTION_AUTHORIZATION_EXPIRED');
      };
      const guard: TaskActionDeliveryGuard = async () => {
        await assertLocal();
        if (!await (authorized ??= verify(false))) throw new Error('TASK_ACTION_AUTHORIZATION_EXPIRED');
        assertOwner();
        if (!active) throw new Error('TASK_ACTION_AUTHORIZATION_EXPIRED');
      };
      const beforeProviderStart = async () => {
        await assertLocal();
        consumed = await verify(true);
        if (!consumed) throw new Error('TASK_ACTION_AUTHORIZATION_EXPIRED');
        await assertLocal();
      };
      try {
        return await deps.turnControl!({
          action: turnAction, targetSessionId, senderSessionId: `${controllerDeviceId}::${senderSessionId}`,
          senderSessionTitle, message: body, receiptId, guard, beforeProviderStart,
        });
      } finally {
        active = false;
        // A rejected/local-only attempt has no input to resume later. Settle
        // valid unused receipts before removing their replay claim.
        const verified = authorized && await authorized.catch(() => false);
        if (consumed || !verified || await verify(true).catch(() => false)) claimed.delete(claimKey);
      }
    }
    const taskActionDeliveryGuard: TaskActionDeliveryGuard = async () => {
      if (automaticReceipt && deps.canReceiveTaskStatusReceipt?.(targetSessionId) !== true)
        throw new Error('TASK_STATUS_RECEIPT_UNAVAILABLE');
      if (!await deps.validate!(controllerDeviceId, { receiptId, ...actor, targetSessionId, message: body, ...(operation ? { operation } : {}) }))
        throw new Error('TASK_ACTION_AUTHORIZATION_EXPIRED');
    };
    taskActionDeliveryGuard.onSettled = async () => {
      // Once the sender forgets the receipt, a replay fails validation even after
      // removing our claim. Failed release keeps the original expiry as fallback.
      if (await deps.validate!(controllerDeviceId, { receiptId, ...actor,
        targetSessionId, message: body, release: true, ...(operation ? { operation } : {}) })) claimed.delete(claimKey);
    };
    try { await taskActionDeliveryGuard(); } catch {
      return { ok: false, errorCode: 'UNSUPPORTED_CAPABILITY', message: 'Task action authorization expired; no message was sent.' };
    }
    const titleOwner = deps.refineTitle && creation && !plugin && !creation.title?.trim() ? getCurrentDbClientSnapshot() : null;
    let created: RemoteSessionCreated | undefined;
    if (creation) {
      const initial = !plugin && !creation.title?.trim() ? { ...creation, title: normalizeAutoTitle(body) } : creation;
      const opened = await deps.create!(targetSessionId, initial, taskActionDeliveryGuard);
      if (!opened.ok) {
        if (opened.errorCode !== 'DELIVERY_UNKNOWN') await taskActionDeliveryGuard.onSettled().catch(() => undefined);
        return opened;
      }
      created = opened;
    }
    const result = await deps.send({
      autoReviewUserText: plugin ? body : { kind: 'agent-request', request: requestSource ?? { userText: null } },
      clientId: created && !plugin ? taskDispatchClientId(targetSessionId, true) : `remote-task-action:${receiptId}`,
      forceQueue: true,
      taskActionDeliveryGuard,
      targetSessionId,
      message,
      ...(automaticReceipt ? { inheritTargetPlanMode: true as const } : {}),
      ...(plugin ? { inheritTargetPlanMode: true as const, sourcePlugin: { pluginId: `${controllerDeviceId}::${senderPluginId}`,
        ...(senderPluginName ? { name: senderPluginName } : {}) } } : { origin: {
        kind: 'session',
        ...(automaticReceipt ? { automaticTaskReceipt: true } : { taskDispatch: true }),
        senderSessionId: `${controllerDeviceId}::${senderSessionId}`,
        displayText: message,
        ...(created ? { dispatchedChildSessionId: targetSessionId } : {}),
        ...(senderSessionTitle ? { senderSessionTitle } : {}),
      } as NonNullable<AgentInputQueuedMessage['origin']> }),
      resumeWithStoredPermissionMode: true,
    });
    if (!result.ok) {
      await taskActionDeliveryGuard.onSettled().catch(() => undefined);
      return { ok: false, errorCode: created ? 'CREATED_NOT_DELIVERED' : result.errorCode,
        message: created ? `Task ${targetSessionId} was created; inspect its queue/history before sending again. ${result.message}` : result.message };
    }
    if (titleOwner && titleOwner === getCurrentDbClientSnapshot() && created?.targetTitle) {
      deps.refineTitle?.(targetSessionId, body, created.targetTitle);
    }
    return {
      ...created,
      ok: true,
      agentKind: result.agentKind,
      wakeKind: created ? 'created' : result.wakeKind,
      targetTitle: result.targetTitle ?? created?.targetTitle ?? null,
      targetLastUserSendAt: result.targetLastUserSendAt,
    };
  };
}

/**
 * 不经 preload 暴露。只有已认证的同账号 device-link 隧道能派发；共享任务对端不可用。
 * 隐藏伙伴任务的可见性检查由 dispatch 在可见性查询就绪后执行，未就绪时这里直接拒绝。
 */
/** plainRequestBody: this host accepts the agent's own text with a checked hand-off receipt (2026-11-05). */
export const SESSION_HANDOFF_CAPABILITY = { taskActionVersion: 1, plainRequestBody: 1, pluginCaller: 1 } as const;

export function registerSessionHandoffIpc(
  deps: Parameters<typeof createSessionHandoffHandler>[0],
): void {
  const handle = createSessionHandoffHandler(deps);
  ipcMain.handle(DL_SESSION_HANDOFF_CHANNEL, async (_event, value: unknown) => {
    const context = getDeviceLinkInvokeContext();
    if (!context || context.sharedTask || context.channel !== DL_SESSION_HANDOFF_CHANNEL) {
      throwIpcError('PERMISSION_DENIED', 'An authorized device link is required');
    }
    if (!hasRemoteBotSessionLookup())
      throwIpcError('PRECONDITION_FAILED', 'Task visibility is not ready');
    const request = requireObject(value, 'request');
    if (request.action === 'task-action-capability') return {
      ...SESSION_HANDOFF_CAPABILITY,
      ...(deps.create ? { ordinaryCreateVersion: 1 } : {}),
      ...(deps.create && deps.refineTitle ? { ordinaryCreateAutoTitle: 1 } : {}),
      ...(deps.listProjects ? { projectCatalogVersion: 1 } : {}),
      ...(deps.canReceiveTaskStatusReceipt ? { taskStatusReceipt: 1 } : {}),
      ...(deps.turnControl && deps.captureTurnControlOwner ? { turnControlVersion: 1 } : {}),
    };
    if (request.action === 'list-projects') {
      if (!deps.listProjects) throwIpcError('UNSUPPORTED_CAPABILITY', 'Project catalog is unavailable');
      const options = z.object({ includeHidden: z.boolean().default(true), offset: z.number().int().min(0).default(0),
        limit: z.number().int().min(1).max(500).default(100) }).strict().parse(request.options ?? {});
      return deps.listProjects(options);
    }
    if (request.action === 'validate-task-action') {
      const pluginId = request.pluginId === undefined ? undefined : boundedString(request.pluginId, 'pluginId', 512);
      const sessionId = request.sessionId === undefined ? undefined : boundedString(request.sessionId, 'sessionId', 512);
      if ((pluginId === undefined) === (sessionId === undefined)) throwIpcError('INVALID_PARAMS', 'Exactly one caller identity is required');
      const valid = await validateRemoteTaskActionReceipt(context!.controllerDeviceId, {
        receiptId: boundedString(request.receiptId, 'receiptId', 128),
        ...(pluginId ? { pluginId } : { sessionId }),
        targetSessionId: boundedString(request.targetSessionId, 'targetSessionId', 512),
        message: boundedString(request.message, 'message', MAX_MESSAGE_CHARS),
        release: request.release === true,
        ...(request.operation !== undefined ? { operation: boundedString(request.operation, 'operation', 12000) } : {}),
      });
      return { valid, ...(request.release === true ? { released: valid } : {}) };
    }
    return handle(context!.controllerDeviceId, value);
  });
}
