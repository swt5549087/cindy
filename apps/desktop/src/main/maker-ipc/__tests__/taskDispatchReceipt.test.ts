import { describe, expect, it, vi } from 'vitest';
import { createTaskDispatchReceipts, readTaskDispatchParent, stripTaskDispatchOrigin, taskDispatchInput, type TaskDispatchInput } from '../taskDispatchReceipt.js';
import type { AgentInputQueuedMessage } from '../../../shared/agentInputQueue.js';

// Unit regression: drive the host receipt service with the first input's persisted
// origin and a terminal callback. Existing handoff tests stop at queue acceptance,
// so they missed even a two-session dispatch with one accepted input and one done.
function setup(parent = 'parent') {
  const origin = { kind: 'session', taskDispatch: true, senderSessionId: parent, displayText: 'Work', dispatchedChildSessionId: 'child' } as const;
  const first = JSON.stringify({ origin });
  const input = taskDispatchInput({ clientId: 'request', origin });
  const deliver = vi.fn(async () => {});
  const warn = vi.fn();
  const readTask = vi.fn(async (sessionId: string, _input: TaskDispatchInput, assistantId?: string) => ({
    parentSessionId: readTaskDispatchParent(first, sessionId),
    childAddress: parent.includes('::') ? `mac-mini::${sessionId}` : sessionId,
    title: 'Child task',
    assistantText: assistantId === 'reply' ? 'Finished the work' : undefined,
  }));
  const service = createTaskDispatchReceipts({ captureAccess: () => ({ readTask, deliver }), warn });
  return { input, service, deliver, readTask, warn, origin };
}

describe('task dispatch status receipt', () => {
  it.each(['parent', 'windows::parent'])('reports the exact turn to its persisted parent %s once', async parent => {
    const { input, service, deliver } = setup(parent);
    const finish = service.capture('child', input)!;
    await finish({ status: 'finished', assistantId: 'reply' });
    await finish({ status: 'failed', text: 'late tail' });
    expect(deliver).toHaveBeenCalledExactlyOnceWith({
      childSessionId: 'child', parentSessionId: parent, title: 'Child task', clientId: expect.stringMatching(/^task-status:/),
      message: expect.stringContaining(`session_id: ${parent.includes('::') ? 'mac-mini::child' : 'child'}\ntitle: "Child task"\nstatus: finished\nsummary: "Finished the work"`),
    });
  });

  it.each(['failed', 'blocked', 'interrupted'] as const)('reports a %s turn without reading the previous answer', async status => {
    const { input, service, deliver } = setup();
    await service.capture('child', input)!({ status, text: 'Could not proceed' });
    expect(deliver.mock.calls).toEqual([[expect.objectContaining({
      message: expect.stringContaining(`status: ${status}\nsummary: "Could not proceed"`),
    })]]);
  });

  it.each([true, false])('waits for an in-flight explicit reply and deduplicates only on acceptance (%s)', async accepted => {
    const { input, service, deliver } = setup();
    let settle!: (result: { ok: boolean }) => void;
    const sent = service.trackSend('child', input, 'parent', () => new Promise<{ ok: boolean }>(resolve => { settle = resolve; }));
    const finished = service.capture('child', input)!({ status: 'finished' });
    await Promise.resolve();
    expect(deliver).not.toHaveBeenCalled();
    settle({ ok: accepted });
    await Promise.all([finished, sent]);
    expect(deliver).toHaveBeenCalledTimes(accepted ? 0 : 1);
    const next = { ...input!, clientId: 'later-dispatch' };
    await service.capture('child', next)!({ status: 'finished' });
    expect(deliver).toHaveBeenCalledTimes(accepted ? 1 : 2);
  });

  it('leaves manual, unbound, forked and reverse receipt turns quiet', async () => {
    const { origin, service, deliver } = setup();
    const reverse: AgentInputQueuedMessage['origin'] = { kind: 'session', taskDispatch: true, senderSessionId: 'child', displayText: 'Receipt' };
    expect(service.capture('child', taskDispatchInput({ clientId: 'manual' }))).toBeUndefined();
    expect(service.capture('child', taskDispatchInput({
      clientId: 'manual-retry', retrySourceClientId: 'request', origin,
    }))).toBeUndefined();
    expect(service.capture('child', taskDispatchInput({ clientId: 'renderer', origin: stripTaskDispatchOrigin(origin) }))).toBeUndefined();
    expect(service.capture('parent', taskDispatchInput({ clientId: 'automatic', origin: { ...reverse, automaticTaskReceipt: true } }))).toBeUndefined();
    await service.capture('parent', taskDispatchInput({ clientId: 'remote-automatic', origin: reverse }))?.({ status: 'finished' });
    await service.capture('fork', taskDispatchInput({ clientId: 'forked', origin }))?.({ status: 'finished' });
    await service.capture('child', taskDispatchInput({ clientId: 'stranger', origin: { ...origin, senderSessionId: 'other' } }))?.({ status: 'finished' });
    expect(deliver).not.toHaveBeenCalled();
    expect(readTaskDispatchParent(JSON.stringify({ origin: reverse }), 'parent')).toBeUndefined();
  });
});
