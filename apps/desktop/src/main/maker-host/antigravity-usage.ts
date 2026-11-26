import fs from 'node:fs';
import type { SubscriptionCodexUsageBucket } from '../../shared/subscriptionAccounts.js';

const LOAD_URL = 'https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist';
const QUOTA_URL = 'https://daily-cloudcode-pa.googleapis.com/v1internal:retrieveUserQuotaSummary';
const MAX_TOKEN_FILE_BYTES = 32 * 1024;
const CACHE_MS = 3 * 60_000;
const TOKEN_EXPIRY_MARGIN_MS = 60_000;
const FAILURE_RETRY_MS = 15_000;
const RENEWAL_RETRY_MS = 30_000;
const TOKEN_CHANGED = Symbol('Antigravity quota token changed');

function record(raw: unknown): Record<string, unknown> {
  return raw && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : {};
}

export function readAntigravityToken(tokenPath: string) {
  try {
    // The CLI owns this file. Never follow a substituted symlink or copy its credentials.
    const initial = fs.lstatSync(tokenPath, { bigint: true });
    if (!initial.isFile()) return null;
    const fd = fs.openSync(tokenPath, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
    try {
      const stat = fs.fstatSync(fd, { bigint: true });
      const uid = process.getuid?.();
      if (!stat.isFile() || stat.nlink !== 1n || (uid !== undefined && stat.uid !== BigInt(uid))
        || stat.dev !== initial.dev || stat.ino !== initial.ino || stat.size > MAX_TOKEN_FILE_BYTES) return null;
      const bytes = Buffer.alloc(MAX_TOKEN_FILE_BYTES + 1);
      const read = fs.readSync(fd, bytes);
      if (read > MAX_TOKEN_FILE_BYTES) return null;
      const current = fs.lstatSync(tokenPath, { bigint: true });
      if (!current.isFile() || current.dev !== stat.dev || current.ino !== stat.ino
        || current.mtimeNs !== stat.mtimeNs || current.ctimeNs !== stat.ctimeNs || current.size !== stat.size) return null;
      const value = record(JSON.parse(bytes.toString('utf8', 0, read)));
      const token = record(value.token);
      return {
        fileVersion: [stat.dev, stat.ino, stat.mtimeNs, stat.ctimeNs, stat.size].join(':'),
        identity: typeof value.id_token_email === 'string' ? value.id_token_email.slice(0, 254) : undefined,
        accessToken: typeof token.access_token === 'string' && /^[!-~]+$/.test(token.access_token) ? token.access_token : undefined,
        expiresAt: typeof token.expiry === 'string' ? Date.parse(token.expiry) : 0,
      };
    } finally { fs.closeSync(fd); }
  } catch {
    return null;
  }
}

export function parseAntigravityQuota(raw: unknown, now: number): SubscriptionCodexUsageBucket[] {
  const { groups } = record(raw);
  if (!Array.isArray(groups)) return [];
  return groups.slice(0, 64).flatMap((value, index) => {
    const group = record(value);
    if (!Array.isArray(group.buckets)) return [];
    let primary: SubscriptionCodexUsageBucket['primary'] = null;
    let secondary: SubscriptionCodexUsageBucket['secondary'] = null;
    for (const value of group.buckets) {
      const bucket = record(value);
      if (bucket.disabled === true) continue;
      const label = [bucket.window, bucket.displayName].filter(value => typeof value === 'string').join(' ');
      const weekly = /week/i.test(label);
      if (!weekly && bucket.window !== 'session' && !/\b5\s*-?\s*h|five.?hour/i.test(label)) continue;
      const reset = typeof bucket.resetTime === 'string' ? Date.parse(bucket.resetTime) : NaN;
      const fraction = bucket.remainingFraction ?? (Number.isFinite(reset) && bucket.remainingAmount === undefined ? 0 : null);
      if (typeof fraction !== 'number' || !Number.isFinite(fraction) || fraction < 0 || fraction > 1) continue;
      const window = {
        usedPercent: 100 - fraction * 100, windowMinutes: weekly ? 10_080 : 300,
        resetsAt: Number.isFinite(reset) ? Math.floor(reset / 1000) : null,
      };
      if (weekly) {
        if (window.usedPercent > (secondary?.usedPercent ?? -1)) secondary = window;
      } else if (window.usedPercent > (primary?.usedPercent ?? -1)) primary = window;
    }
    if (!primary && !secondary) return [];
    return [{
      limitId: String(index),
      limitName: typeof group.displayName === 'string' ? group.displayName.slice(0, 160) : null,
      updatedAt: now, primary, secondary,
    }];
  });
}

type Stage = 'load-project' | 'read-quota';
type Diagnostic =
  | { reason: 'token-expiring' | 'token-unavailable' | 'renewal-failed' | 'token-not-renewed' }
  | { reason: 'network-or-proxy' | 'timeout' | 'invalid-json' | 'missing-fields'; stage: Stage }
  | { reason: 'http'; stage: Stage; status: number };

type QuotaToken = NonNullable<ReturnType<typeof readAntigravityToken>>;
type QuotaFetcher = (url: string, init: RequestInit, beforeDispatch: () => void) => Promise<Response>;
type Snapshot = { fileVersion: string; until: number; buckets: SubscriptionCodexUsageBucket[] };
type ProfileRead = {
  key: string;
  latest?: Snapshot;
  retry?: { fileVersion: string; until: number };
  serverRetryUntil?: number;
  inFlight?: Promise<void>;
};

function stillCurrentWindows(buckets: SubscriptionCodexUsageBucket[], now: number): SubscriptionCodexUsageBucket[] {
  const keep = (window: SubscriptionCodexUsageBucket['primary']) =>
    window?.resetsAt && window.resetsAt * 1000 > now ? window : null;
  // Keep the group names so a fully reset Gemini group cannot promote
  // another model family's live window to the main Gemini readout.
  const current = buckets.map(bucket => ({ ...bucket, primary: keep(bucket.primary), secondary: keep(bucket.secondary) }));
  return current.some(bucket => bucket.primary || bucket.secondary) ? current : [];
}

/** Coalesce renewals and queries across settings and the status card. */
export function createAntigravityUsageReader(fetcher: QuotaFetcher = fetch, {
  renew,
  report = () => {},
}: {
  renew?: (signal: AbortSignal, assertCurrent: () => void) => Promise<void>;
  report?: (diagnostic: Diagnostic) => void;
} = {}) {
  let active: ProfileRead | undefined;
  let unavailable: { key: string; until: number } | undefined;
  const renewAfter = new Map<string, number>();
  return async (tokenPath: string, scope: number, signal: AbortSignal, assertCurrent: () => void): Promise<SubscriptionCodexUsageBucket[]> => {
    const assertActive = () => { signal.throwIfAborted(); assertCurrent(); };
    assertActive();
    const initial = readAntigravityToken(tokenPath);
    const now = Date.now();
    if (!initial?.accessToken) {
      const key = JSON.stringify([tokenPath, scope]);
      if (unavailable?.key !== key || unavailable.until <= now) {
        unavailable = { key, until: now + FAILURE_RETRY_MS };
        report({ reason: 'token-unavailable' });
      }
      return [];
    }
    const key = JSON.stringify([tokenPath, scope, initial.identity]);
    if (active?.key !== key) active = { key };
    const entry = active;
    const result = (token: QuotaToken, at: number) => {
      const latest = entry.latest;
      if (!latest) return [];
      return latest.fileVersion === token.fileVersion && latest.until > at && token.expiresAt > at + TOKEN_EXPIRY_MARGIN_MS
        ? latest.buckets
        : stillCurrentWindows(latest.buckets, at);
    };
    if (!entry.inFlight && (entry.serverRetryUntil ?? 0) <= now
      && (entry.latest?.fileVersion !== initial.fileVersion || entry.latest.until <= now
        || !(initial.expiresAt > now + TOKEN_EXPIRY_MARGIN_MS))
      && (entry.retry?.fileVersion !== initial.fileVersion || entry.retry.until <= now)) {
      entry.inFlight = Promise.resolve().then(async () => {
        assertActive();
        let token = initial;
        let retryAfter = 0;
        const fail = (diagnostic: Diagnostic) => {
          assertActive();
          report(diagnostic);
          return null;
        };
        const readQuota = async () => {
          if (!(token.expiresAt > Date.now() + TOKEN_EXPIRY_MARGIN_MS)) {
            const now = Date.now();
            for (const [path, until] of renewAfter) if (until <= now) renewAfter.delete(path);
            if (!renew || renewAfter.has(tokenPath)) return null;
            renewAfter.set(tokenPath, now + RENEWAL_RETRY_MS);
            fail({ reason: 'token-expiring' });
            try {
              await renew(AbortSignal.any([signal, AbortSignal.timeout(15_000)]), assertActive);
            } catch {
              return fail({ reason: 'renewal-failed' });
            }
            assertActive();
            const updated = readAntigravityToken(tokenPath);
            if (!updated?.accessToken || updated.identity !== initial.identity) return fail({ reason: 'token-unavailable' });
            token = updated;
            if (!(token.expiresAt > Date.now() + TOKEN_EXPIRY_MARGIN_MS)) return fail({ reason: 'token-not-renewed' });
          }
          const assertToken = () => {
            assertActive();
            if (readAntigravityToken(tokenPath)?.fileVersion !== token.fileVersion) throw TOKEN_CHANGED;
          };
          const request = async (stage: Stage, url: string, body: object): Promise<unknown> => {
            assertToken();
            const timeout = AbortSignal.timeout(10_000);
            let response: Response;
            try {
              response = await fetcher(url, {
                method: 'POST', body: JSON.stringify(body), redirect: 'error', signal: AbortSignal.any([signal, timeout]),
                headers: {
                  Authorization: `Bearer ${token.accessToken}`,
                  'Content-Type': 'application/json',
                  'User-Agent': `antigravity/2.31.0 ${process.platform}/${process.arch}`,
                },
              }, assertToken);
            } catch {
              assertToken();
              return fail({ reason: timeout.aborted ? 'timeout' : 'network-or-proxy', stage });
            }
            assertToken();
            if (!response.ok) {
              if (response.status === 429 || response.status === 503) {
                const value = response.headers.get('retry-after')?.trim();
                if (value) {
                  const until = /^\d+$/.test(value) ? Date.now() + Number(value) * 1000 : Date.parse(value);
                  if (Number.isFinite(until)) retryAfter = until;
                }
              }
              void response.body?.cancel().catch(() => {});
              return fail({ reason: 'http', stage, status: response.status });
            }
            try { return record(await response.json()); }
            catch { return fail({ reason: timeout.aborted ? 'timeout' : 'invalid-json', stage }); }
          };
          const projectResponse = await request('load-project', LOAD_URL, {});
          if (projectResponse === null) return null;
          const project = record(projectResponse).cloudaicompanionProject;
          if (typeof project !== 'string' || project.length > 256 || !project) return fail({ reason: 'missing-fields', stage: 'load-project' });
          const quotaResponse = await request('read-quota', QUOTA_URL, { project });
          if (quotaResponse === null) return null;
          assertToken();
          const buckets = parseAntigravityQuota(quotaResponse, Date.now());
          return buckets.length ? buckets : fail({ reason: 'missing-fields', stage: 'read-quota' });
        };
        const buckets = await readQuota().catch(error => {
          if (error === TOKEN_CHANGED) return null;
          throw error;
        });
        assertActive();
        if (buckets) {
          entry.latest = { fileVersion: token.fileVersion, until: Date.now() + CACHE_MS, buckets };
          entry.retry = undefined;
        } else {
          entry.retry = { fileVersion: token.fileVersion, until: Date.now() + FAILURE_RETRY_MS };
          entry.serverRetryUntil = retryAfter;
        }
      }).finally(() => { entry.inFlight = undefined; });
    }
    await entry.inFlight;
    assertActive();
    const current = readAntigravityToken(tokenPath);
    return current?.accessToken && active === entry && current.identity === initial.identity ? result(current, Date.now()) : [];
  };
}
