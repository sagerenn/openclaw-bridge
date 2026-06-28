/**
 * Pluggable storage backends for the webhook receiver.
 *
 * The default (InMemoryMessageStore in core.ts) is volatile and single-
 * instance. On serverless platforms (Vercel functions, Cloudflare Pages
 * functions) every request may hit a fresh instance, so in-memory state is
 * lost between calls — you NEED a remote store. This module provides:
 *
 *   - UpstashRedisStore  (UPSTASH_REDIS_REST_URL + _TOKEN)   — works everywhere
 *   - VercelKVStore      (KV_REST_API_URL)                   — Vercel KV REST
 *   - CloudflareKVStore  (env binding)                       — Cloudflare KV
 *
 * All three implement the same MessageStore contract by modeling each token's
 * queue as a Redis LIST / KV JSON array. pickStoreFromEnv() returns the right
 * one for the current platform, falling back to in-memory.
 *
 * NOTE: the remote stores are intentionally dependency-light. Upstash and
 * Vercel KV speak a plain HTTPS REST protocol, so we use fetch() directly
 * (no SDK install). Cloudflare KV is injected as a binding by the platform,
 * not via env — so CloudflareKVStore takes the binding object in its ctor
 * (see cloudflare/functions).
 */

import type { MessageStore, StoredMessage } from "./core.js";
import { InMemoryMessageStore } from "./core.js";

// ─── Upstash Redis (REST) ────────────────────────────────────────────────────

/**
 * Upstash exposes a REST API (https://...upstash.io) reachable from any FaaS.
 * Each token's queue is a Redis list key `wh:msg:{token}`; entries are JSON
 * StoredMessages. RPUSH appends, LRANGE reads oldest-first, ack LTRIMs.
 *
 * Ack is idempotent: we lrange the list, find the index of the last id to ack,
 * and ltrim to drop everything up to and including it.
 */
export class UpstashRedisStore implements MessageStore {
  private url: string;
  private token: string;

  constructor(url: string, token: string) {
    this.url = url.replace(/\/$/, "");
    this.token = token;
  }

  private key(token: string): string {
    return `wh:msg:${token}`;
  }

  private async exec(args: (string | number)[]): Promise<any[]> {
    const res = await fetch(`${this.url}`, {
      method: "POST",
      headers: {
        "authorization": `Bearer ${this.token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(args),
    });
    if (!res.ok) {
      throw new Error(`Upstash error ${res.status}: ${await res.text()}`);
    }
    const body = (await res.json()) as { result?: unknown };
    return (body.result ?? []) as unknown[];
  }

  async push(token: string, msg: StoredMessage): Promise<StoredMessage> {
    const id = `w${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const stored: StoredMessage = { ...msg, id };
    await this.exec(["RPUSH", this.key(token), JSON.stringify(stored)]);
    return stored;
  }

  async list(token: string, limit: number): Promise<StoredMessage[]> {
    const arr = await this.exec(["LRANGE", this.key(token), 0, Math.max(0, limit - 1)]);
    return (arr as string[])
      .map((s) => safeParse<StoredMessage>(s))
      .filter((x): x is StoredMessage => x !== null);
  }

  async ack(token: string, lastAckId: string): Promise<number> {
    const key = this.key(token);
    const arr = (await this.exec(["LRANGE", key, 0, -1])) as string[];
    let idx = -1;
    for (let i = 0; i < arr.length; i++) {
      const m = safeParse<StoredMessage>(arr[i]);
      if (m && m.id === lastAckId) { idx = i; break; }
    }
    if (idx < 0) return 0;
    const removed = idx + 1;
    // LTRIM keeps [start, end]; to drop the first (idx+1), keep [idx+1, -1].
    await this.exec(["LTRIM", key, idx + 1, -1]);
    return removed;
  }

  async prune(token: string, maxAgeMs: number): Promise<number> {
    const key = this.key(token);
    const arr = (await this.exec(["LRANGE", key, 0, -1])) as string[];
    const cutoff = Date.now() - maxAgeMs;
    let idx = -1;
    for (let i = 0; i < arr.length; i++) {
      const m = safeParse<StoredMessage>(arr[i]);
      if (m && m.timestamp >= cutoff) break;
      idx = i;
    }
    if (idx < 0) return 0;
    const removed = idx + 1;
    await this.exec(["LTRIM", key, idx + 1, -1]);
    return removed;
  }
}

// ─── Vercel KV (REST) ─────────────────────────────────────────────────────────

/**
 * Vercel KV's REST API is Upstash-compatible: same / pipeline with
 * `authorization: Bearer <KV_REST_API_TOKEN>` and `KV_REST_API_URL`. Reuse the
 * Upstash store — only the env names differ (see pickStoreFromEnv).
 */
export class VercelKVStore extends UpstashRedisStore {}

// ─── Cloudflare KV (binding) ──────────────────────────────────────────────────

/**
 * Cloudflare KV is not reachable via env URL+token; the platform injects a
 * `KV` binding into the request context. We model each token's queue as a
 * JSON array stored under key `wh:msg:{token}`.
 *
 * Because CF KV is eventually consistent for writes and has read-after-write
 * caveats, this is acceptable for webhook buffering (messages are append-mostly
 * and the bridge polls on its own cadence) but NOT for strict low-latency ack
 * ordering across regions. For that, use Upstash.
 */
export interface CfKvLike {
  get(key: string, options?: { type?: "json" | "text" }): Promise<any>;
  put(key: string, value: string, options?: { expirationTtl?: number }): Promise<void>;
  list(options?: { prefix?: string }): Promise<{ keys: Array<{ name: string }> }>;
}

export class CloudflareKVStore implements MessageStore {
  constructor(private kv: CfKvLike) {}

  private key(token: string): string {
    return `wh:msg:${token}`;
  }

  async push(token: string, msg: StoredMessage): Promise<StoredMessage> {
    const key = this.key(token);
    const arr = ((await this.kv.get(key, { type: "json" })) ?? []) as StoredMessage[];
    const id = `w${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const stored: StoredMessage = { ...msg, id };
    arr.push(stored);
    // TTL 7 days so abandoned tokens self-clean; bound to keep value < 25MB.
    await this.kv.put(key, JSON.stringify(arr), { expirationTtl: 7 * 24 * 3600 });
    return stored;
  }

  async list(token: string, limit: number): Promise<StoredMessage[]> {
    const arr = ((await this.kv.get(this.key(token), { type: "json" })) ?? []) as StoredMessage[];
    return arr.slice(0, Math.max(1, limit));
  }

  async ack(token: string, lastAckId: string): Promise<number> {
    const key = this.key(token);
    const arr = ((await this.kv.get(key, { type: "json" })) ?? []) as StoredMessage[];
    let idx = -1;
    for (let i = 0; i < arr.length; i++) {
      if (arr[i].id === lastAckId) { idx = i; break; }
    }
    if (idx < 0) return 0;
    const removed = idx + 1;
    const next = arr.slice(idx + 1);
    await this.kv.put(key, JSON.stringify(next), { expirationTtl: 7 * 24 * 3600 });
    return removed;
  }

  async prune(token: string, maxAgeMs: number): Promise<number> {
    const key = this.key(token);
    const arr = ((await this.kv.get(key, { type: "json" })) ?? []) as StoredMessage[];
    const cutoff = Date.now() - maxAgeMs;
    let idx = -1;
    for (let i = 0; i < arr.length; i++) {
      if (arr[i].timestamp >= cutoff) break;
      idx = i;
    }
    if (idx < 0) return 0;
    const removed = idx + 1;
    const next = arr.slice(idx + 1);
    await this.kv.put(key, JSON.stringify(next), { expirationTtl: 7 * 24 * 3600 });
    return removed;
  }
}

// ─── Env-driven factory ───────────────────────────────────────────────────────

/**
 * Pick a store from the environment. Precedence:
 *   1. Cloudflare KV binding (passed in explicitly — not in env)
 *   2. UPSTASH_REDIS_REST_URL (+ _TOKEN)
 *   3. KV_REST_API_URL (+ KV_REST_API_TOKEN)   [Vercel KV]
 *   4. in-memory fallback (single-instance only — logged)
 *
 * Token bindings are also loaded from env here: WH_TOKENS is a JSON array of
 * { token, channel, accountId }, OR a set of WH_TOKEN_<ID> shorthands. See
 * loadTokenBindings.
 */
export function pickStoreFromEnv(kv?: CfKvLike | null): MessageStore {
  if (kv) return new CloudflareKVStore(kv);

  const upstashUrl = envStr("UPSTASH_REDIS_REST_URL");
  if (upstashUrl) {
    const tok = envStr("UPSTASH_REDIS_REST_TOKEN") ?? "";
    return new UpstashRedisStore(upstashUrl, tok);
  }

  const kvUrl = envStr("KV_REST_API_URL");
  if (kvUrl) {
    const tok = envStr("KV_REST_API_TOKEN") ?? "";
    return new VercelKVStore(kvUrl, tok);
  }

  // Fallback: volatile in-process. Will not share state across FaaS instances.
  return new InMemoryMessageStore();
}

/**
 * Load token bindings from env. Format (either):
 *   WH_TOKENS='[{"token":"abc","channel":"msteams","accountId":"default"}]'
 * or repeated:
 *   WH_TOKEN_MSTEAMS_DEFAULT=abc   (binding: channel=msteams, account=default,
 *                                   token = the env value)
 */
export function loadTokenBindings(): Array<{ token: string; channel: string; accountId: string }> {
  const out: Array<{ token: string; channel: string; accountId: string }> = [];

  const json = envStr("WH_TOKENS");
  if (json) {
    try {
      const arr = JSON.parse(json);
      if (Array.isArray(arr)) {
        for (const e of arr) {
          if (e && typeof e.token === "string" && typeof e.channel === "string") {
            out.push({ token: e.token, channel: e.channel, accountId: e.accountId ?? "default" });
          }
        }
      }
    } catch {
      // fall through to shorthand
    }
  }

  for (const [k, v] of Object.entries(envAll())) {
    if (!k.startsWith("WH_TOKEN_")) continue;
    if (typeof v !== "string" || !v) continue;
    const rest = k.slice("WH_TOKEN_".length).toLowerCase(); // e.g. msteams_default
    const sep = rest.includes("__") ? "__" : "_";
    const [channel, accountId = "default"] = rest.split(sep);
    out.push({ token: v, channel, accountId });
  }

  return out;
}

function envStr(name: string): string | undefined {
  const v = (globalThis as any).process?.env?.[name];
  return typeof v === "string" && v.length > 0 ? v : undefined;
}

function envAll(): Record<string, string | undefined> {
  return (globalThis as any).process?.env ?? {};
}

function safeParse<T>(s: string): T | null {
  try { return JSON.parse(s) as T; } catch { return null; }
}
