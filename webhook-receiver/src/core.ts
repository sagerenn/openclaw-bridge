/**
 * Webhook receiver — portable core.
 *
 * Runtime-agnostic: imports NOTHING from node:http. It speaks the Web Fetch
 * `Request`/`Response` shapes only, so the same core drives:
 *   - the bundled Node dev server (src/server.ts)
 *   - Vercel serverless functions (vercel/api/*)
 *   - Cloudflare Pages/Workers functions (cloudflare/functions/*)
 *   - any other FaaS that hands you a Request and wants a Response
 *
 * Data flow:
 *
 *   IM platform --[POST /webhook/{token}]--> receiver (store.push)
 *                                                  |
 *   bridge server <--[GET /webhook/{token}/poll]-- receiver (store.list + ack)
 *        |
 *        --> WS client (as inbound_message)
 *
 * The receiver is a passive buffer: inbound IM events land here, and the
 * (long-lived) bridge polls them out. This is what stateless channels that
 * cannot hold an inbound WebSocket — e.g. MS Teams outbound webhooks — need
 * instead of a persistent gateway.
 *
 * Storage is behind a KV-style interface (see MessageStore). The default is an
 * in-memory store (volatile, single-instance). For multi-instance FaaS, point
 * it at Upstash Redis / Vercel KV / Cloudflare KV via env — see storage.ts.
 */

// ─── Types ───────────────────────────────────────────────────────────────────

/** A buffered inbound message, normalized to the bridge's inbound shape. */
export interface StoredMessage {
  /** Stable, monotonic id assigned by the receiver. Used as the ack cursor. */
  id: string;
  /** Channel this webhook was registered for (e.g. "msteams"). */
  channel: string;
  /** Account id within the channel (e.g. "default"). */
  accountId: string;
  /** Channel-native message id, if the platform sent one. */
  messageId: string;
  /** Conversation / chat id the reply should target. */
  chatId: string;
  /** Sender user id. */
  senderId: string;
  /** Sender display name, if known. */
  senderName?: string;
  /** "text" | "markdown" | "image" | "file" | "system" */
  msgType: string;
  /** Text content. */
  text: string;
  /** Timestamp (ms since epoch). */
  timestamp: number;
  /** Optional media URL the platform supplied. */
  mediaUrl?: string;
  mediaType?: string;
  /** Ready-to-echo reply target (the `to` for the bridge's send_text). */
  replyTo?: string;
  /** Raw platform payload, preserved for advanced use. */
  raw?: unknown;
}

/** What a webhook token resolves to. Registered via registerToken()/env. */
export interface TokenBinding {
  channel: string;
  accountId: string;
}

/** Configuration for a receiver instance. */
export interface ReceiverConfig {
  /**
   * Shared secret used to verify HMAC signatures on inbound POSTs. When set,
   * an inbound request carrying a signature header (configured via
   * `signatureHeader`) is verified; mismatched/missing signatures are rejected
   * with 401. When unset, signature verification is skipped (token-only auth).
   */
  sharedSecret?: string;
  /** Header the platform signs (e.g. "x-signature" for generic, defaults to a
   * case-insensitive match). */
  signatureHeader?: string;
  /**
   * How the signature is encoded. "hex" (default) or "base64". Compared as
   * timing-safe equality.
   */
  signatureEncoding?: "hex" | "base64";
  /** Bearer token the bridge must present when polling (GET .../poll). When
   * unset, the same URL token is the only poll credential. */
  pollBearerToken?: string;
  /** Max messages returned per poll (default 50). */
  maxPollBatch?: number;
  /** Drop messages older than this many ms to bound memory (default 24h). */
  maxAgeMs?: number;
}

// ─── Message Store ───────────────────────────────────────────────────────────

/**
 * The store contract. Backed by memory (default), Upstash Redis, Vercel KV, or
 * Cloudflare KV — see storage.ts. All methods are async so remote backends fit.
 *
 * Messages are kept per token (a FIFO list). The bridge polls, gets the batch
 * up to a cursor, and acks by id; acked ids are trimmed. Each message carries
 * a monotonic `id` so ack is idempotent and survives partial polls.
 */
export interface MessageStore {
  /** Append a message to the token's queue. Returns the stored message. */
  push(token: string, msg: StoredMessage): Promise<StoredMessage>;
  /**
   * Return up to `limit` messages currently buffered for the token, oldest
   * first. Does NOT delete them — the caller acks after processing.
   */
  list(token: string, limit: number): Promise<StoredMessage[]>;
  /** Delete messages with id <= lastAckId (inclusive). Returns count removed. */
  ack(token: string, lastAckId: string): Promise<number>;
  /** Prune messages older than `maxAgeMs` for a token (housekeeping). */
  prune(token: string, maxAgeMs: number): Promise<number>;
}

// ─── In-memory store (default) ───────────────────────────────────────────────

/**
 * Volatile, single-instance store. Fine for local dev and self-hosted Docker
 * where one process owns the receiver. NOT safe across multiple FaaS instances
 * — use a remote KV there (storage.ts).
 */
export class InMemoryMessageStore implements MessageStore {
  private queues = new Map<string, StoredMessage[]>();
  private counters = new Map<string, number>();

  private nextId(token: string): string {
    const n = (this.counters.get(token) ?? 0) + 1;
    this.counters.set(token, n);
    return n.toString(36);
  }

  async push(token: string, msg: StoredMessage): Promise<StoredMessage> {
    const stored: StoredMessage = { ...msg, id: this.nextId(token) };
    let q = this.queues.get(token);
    if (!q) {
      q = [];
      this.queues.set(token, q);
    }
    q.push(stored);
    return stored;
  }

  async list(token: string, limit: number): Promise<StoredMessage[]> {
    const q = this.queues.get(token);
    if (!q) return [];
    return q.slice(0, Math.max(1, limit));
  }

  async ack(token: string, lastAckId: string): Promise<number> {
    const q = this.queues.get(token);
    if (!q) return 0;
    let removed = 0;
    while (q.length > 0 && idLe(q[0].id, lastAckId)) {
      q.shift();
      removed++;
    }
    return removed;
  }

  async prune(token: string, maxAgeMs: number): Promise<number> {
    const q = this.queues.get(token);
    if (!q) return 0;
    const cutoff = Date.now() - maxAgeMs;
    let removed = 0;
    while (q.length > 0 && q[0].timestamp < cutoff) {
      q.shift();
      removed++;
    }
    return removed;
  }
}

/** Compare monotonic ids (base-36 strings) for ack ordering. */
function idLe(a: string, b: string): boolean {
  // Both are base36 monotonic ints from the same counter; numeric comparison
  // is order-preserving. Guard against mixed encodings by comparing as numbers
  // when possible, falling back to lexical.
  const na = parseInt(a, 36);
  const nb = parseInt(b, 36);
  if (!Number.isNaN(na) && !Number.isNaN(nb)) return na <= nb;
  return a <= b;
}

// ─── HMAC verification ───────────────────────────────────────────────────────

/** Timing-safe equality for two strings (constant time on equal length). */
export function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let mismatch = 0;
  for (let i = 0; i < a.length; i++) {
    mismatch |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
  return mismatch === 0;
}

/**
 * HMAC-SHA256 over the raw request body. Used by platforms that sign their
 * webhooks (e.g. generic HMAC configs). Returns the hex/base64 digest.
 *
 * Uses the Web Crypto API (SubtleCrypto) so it works in Workers/Vercel/Node
 * 18+ alike.
 */
export async function computeHmac(
  secret: string,
  body: ArrayBuffer | string,
  encoding: "hex" | "base64" = "hex",
): Promise<string> {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const data = typeof body === "string" ? enc.encode(body) : new Uint8Array(body);
  const sig = new Uint8Array(await crypto.subtle.sign("HMAC", key, data));
  const bytes = new Uint8Array(sig);
  if (encoding === "base64") {
    let bin = "";
    for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
    return btoa(bin);
  }
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

// ─── Token registry ──────────────────────────────────────────────────────────

/**
 * Maps webhook tokens -> { channel, accountId }. Tokens are unguessable secret
 * URL segments: the IM platform posts to /webhook/<token>, and the bridge polls
 * the same path. The binding tells the bridge which channel/account an inbound
 * message belongs to.
 *
 * In-memory by default. For FaaS where the binding must survive cold starts /
 * be visible across instances, back this with the same KV as the message store
 * (see TokenStore in storage.ts).
 */
export class TokenRegistry {
  private bindings = new Map<string, TokenBinding>();

  register(token: string, binding: TokenBinding): void {
    this.bindings.set(token, binding);
  }

  resolve(token: string): TokenBinding | undefined {
    return this.bindings.get(token);
  }

  list(): Array<{ token: string; binding: TokenBinding }> {
    return [...this.bindings.entries()].map(([token, binding]) => ({ token, binding }));
  }

  /** Generate a fresh unguessable token (32 bytes, base64url). */
  static generate(): string {
    const bytes = new Uint8Array(32);
    crypto.getRandomValues(bytes);
    let bin = "";
    for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
    return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  }
}

// ─── Inbound normalization ───────────────────────────────────────────────────

/**
 * Normalize an arbitrary platform webhook body into a StoredMessage. Channels
 * differ widely (Teams, Slack, generic), so we extract a best-effort set of
 * fields and stash the full body in `raw` for any adapter-specific needs.
 *
 * Recognized fields (case-insensitive, multiple aliases):
 *   text/body/message/content
 *   messageId/id/eventId
 *   chatId/conversation/id  (conversation.id preferred)
 *   senderId/from/userId/from.id
 *   senderName/from.name/fromName
 *   type/msgType
 *   timestamp (s or ms; auto-detected)
 */
export function normalizeInbound(
  binding: TokenBinding,
  body: unknown,
): StoredMessage {
  const o = (body && typeof body === "object" ? body : {}) as Record<string, any>;
  const conv = o.conversation ?? o.channel ?? o.chat;
  const from = o.from ?? o.sender ?? o.user ?? o.author;

  const text = pickStr(o, ["text", "body", "message", "content", "payload", "data"]);
  const messageId = pickStr(o, ["messageId", "id", "eventId", "message_id", "ts"]) || `wh-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const chatId = pickStr(conv, ["id", "conversationId", "chatId", "channelId"]) || pickStr(o, ["chatId", "channelId", "conversationId"]) || "";
  const senderId = pickStr(from, ["id", "userId", "user_id", "aadObjectId"]) || pickStr(o, ["senderId", "fromId", "userId", "user_id"]) || "";
  const senderName = pickStr(from, ["name", "displayName", "username", "user_name"]) || pickStr(o, ["senderName", "fromName", "userName"]);
  const msgType = pickStr(o, ["type", "msgType", "messageType"]) || (text ? "text" : "system");
  const replyTo = pickStr(o, ["replyTo", "reply_to", "conversationId"]) || (chatId ? `${binding.channel}:${chatId}` : undefined);

  return {
    id: "", // assigned by store.push
    channel: binding.channel,
    accountId: binding.accountId,
    messageId,
    chatId,
    senderId,
    senderName,
    msgType,
    text: text ?? "",
    timestamp: pickTimestamp(o),
    replyTo,
    mediaUrl: pickStr(o, ["mediaUrl", "media_url", "fileUrl", "imageUrl"]),
    mediaType: pickStr(o, ["mediaType", "mime_type", "contentType"]),
    raw: body,
  };
}

function pickStr(rec: any, keys: string[]): string | undefined {
  if (!rec || typeof rec !== "object") return undefined;
  for (const k of keys) {
    for (const field of Object.keys(rec)) {
      if (field.toLowerCase() === k.toLowerCase()) {
        const v = rec[field];
        if (typeof v === "string" && v.length > 0) return v;
        if (typeof v === "number") return String(v);
      }
    }
  }
  return undefined;
}

function pickTimestamp(o: Record<string, any>): number {
  const v = pickStr(o, ["timestamp", "ts", "time", "createdAt", "created_at", "date"]);
  if (!v) return Date.now();
  const n = Number(v);
  if (!Number.isNaN(n)) {
    // Seconds vs milliseconds heuristic: a 10-digit epoch is seconds.
    if (n < 1e12) return Math.floor(n * 1000);
    return Math.floor(n);
  }
  const d = Date.parse(v);
  return Number.isNaN(d) ? Date.now() : d;
}

// ─── Receiver ────────────────────────────────────────────────────────────────

/**
 * The receiver itself. Holds a store + token registry + config, and exposes a
 * single `handle(req)` that routes a Web Fetch Request to its handler and
 * returns a Web Fetch Response. This is the only entry adapters call.
 *
 * Routes:
 *   POST /webhook/{token}            — IM pushes an inbound event
 *   GET  /webhook/{token}/poll       — bridge drains buffered messages
 *   GET  /webhook/{token}/ack?id=…   — (alt) ack without draining
 *   GET  /healthz                    — liveness
 *   GET  /                           — banner
 *
 * Auth:
 *  - Inbound POSTs are authenticated by the unguessable {token} in the URL,
 *    plus an optional HMAC signature header (when sharedSecret is set).
 *  - Polls are authenticated by the {token} and (when configured) a
 *    `pollBearerToken` presented as `Authorization: Bearer <token>` or
 *    `?token=<token>`. This stops a public URL from leaking buffered DMs.
 */
export class Receiver {
  constructor(
    private store: MessageStore,
    private tokens: TokenRegistry,
    private config: ReceiverConfig = {},
  ) {}

  /** Register a token binding at runtime (used by the CLI / long-lived server). */
  registerToken(token: string, binding: TokenBinding): void {
    this.tokens.register(token, binding);
  }

  async handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";

    if (path === "/healthz") return json({ ok: true });
    if (path === "/") return json({ service: "openclaw-webhook-receiver" });

    // /webhook/{token}  and  /webhook/{token}/{poll|ack}
    const m = path.match(/^\/webhook\/([^/]+)(?:\/(poll|ack))?$/);
    if (!m) return json({ error: "Not found" }, 404);

    const token = decodeURIComponent(m[1]);
    const action = m[2];
    const binding = this.tokens.resolve(token);
    // Resolve BEFORE revealing existence — but 404 either way for unknown
    // tokens so the URL space isn't enumerable.
    if (!binding) return json({ error: "Not found" }, 404);

    if (!action) {
      if (req.method !== "POST") return json({ error: "Method not allowed" }, 405);
      return this.handleInbound(token, binding, req);
    }
    if (action === "poll") {
      if (req.method !== "GET") return json({ error: "Method not allowed" }, 405);
      return this.handlePoll(token, binding, req, url);
    }
    if (action === "ack") {
      if (req.method !== "GET" && req.method !== "POST") return json({ error: "Method not allowed" }, 405);
      return this.handleAck(token, url);
    }
    return json({ error: "Not found" }, 404);
  }

  // ── POST /webhook/{token}: IM -> receiver ─────────────────────────────────

  private async handleInbound(
    token: string,
    binding: TokenBinding,
    req: Request,
  ): Promise<Response> {
    const raw = await req.text();

    // Optional HMAC signature verification (only when a shared secret is set).
    if (this.config.sharedSecret) {
      const sig = readHeader(req, this.config.signatureHeader ?? "x-signature");
      if (!sig) return json({ error: "Missing signature" }, 401);
      const expected = await computeHmac(
        this.config.sharedSecret,
        raw,
        this.config.signatureEncoding ?? "hex",
      );
      // Some platforms prefix the digest (e.g. "sha256=…"); strip it.
      const sigCore = sig.replace(/^sha256=/i, "");
      if (!timingSafeEqual(sigCore.toLowerCase(), expected.toLowerCase())) {
        return json({ error: "Invalid signature" }, 401);
      }
    }

    let parsed: unknown;
    try {
      parsed = raw.length > 0 ? JSON.parse(raw) : {};
    } catch {
      // Non-JSON body (e.g. form-encoded or plain text) — wrap as text.
      parsed = textFallback(raw);
    }

    const msg = normalizeInbound(binding, parsed);
    const stored = await this.store.push(token, msg);

    // Best-effort housekeeping: prune ancient messages so memory is bounded.
    const maxAge = this.config.maxAgeMs ?? 24 * 60 * 60 * 1000;
    try {
      await this.store.prune(token, maxAge);
    } catch {
      // pruning is advisory
    }

    return json({ ok: true, id: stored.id }, 200, { "x-message-id": stored.id });
  }

  // ── GET /webhook/{token}/poll: receiver -> bridge ────────────────────────

  private async handlePoll(
    token: string,
    binding: TokenBinding,
    req: Request,
    url: URL,
  ): Promise<Response> {
    if (!this.authorizePoll(req, url)) {
      return json({ error: "Unauthorized" }, 401);
    }

    const limit = clampInt(url.searchParams.get("limit"), 1, 200, this.config.maxPollBatch ?? 50);
    const ack = url.searchParams.get("ack"); // ack last batch up to this id
    if (ack) {
      try {
        await this.store.ack(token, ack);
      } catch {
        // ack failure shouldn't block delivery of the next batch
      }
    }

    const messages = await this.store.list(token, limit);
    return json({ channel: binding.channel, accountId: binding.accountId, messages });
  }

  // ── GET|POST /webhook/{token}/ack?id=… ───────────────────────────────────

  private async handleAck(token: string, url: URL): Promise<Response> {
    const id = url.searchParams.get("id");
    if (!id) return json({ error: "Missing id" }, 400);
    const removed = await this.store.ack(token, id);
    return json({ ok: true, removed });
  }

  private authorizePoll(req: Request, url: URL): boolean {
    if (!this.config.pollBearerToken) return true; // token-in-URL is the only cred
    const auth = readHeader(req, "authorization") ?? "";
    if (auth.toLowerCase().startsWith("bearer ")) {
      const got = auth.slice(7).trim();
      if (timingSafeEqual(got, this.config.pollBearerToken)) return true;
    }
    const q = url.searchParams.get("token");
    if (q && timingSafeEqual(q, this.config.pollBearerToken)) return true;
    return false;
  }
}

// ─── helpers ─────────────────────────────────────────────────────────────────

function json(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", ...headers },
  });
}

function readHeader(req: Request, name: string): string | undefined {
  // Headers are case-insensitive; this works across platforms.
  const v = req.headers.get(name);
  return v ?? undefined;
}

function clampInt(raw: string | null, min: number, max: number, def: number): number {
  if (raw == null) return def;
  const n = parseInt(raw, 10);
  if (Number.isNaN(n)) return def;
  return Math.max(min, Math.min(max, n));
}

function textFallback(raw: string): unknown {
  // Try form-encoded fields (Teams sometimes posts query strings); else plain text.
  if (raw.includes("=") && !raw.trim().startsWith("{") && !raw.trim().startsWith("[")) {
    const obj: Record<string, string> = {};
    for (const pair of raw.split("&")) {
      const [k, v] = pair.split("=");
      if (k) obj[decodeURIComponent(k)] = decodeURIComponent(v ?? "");
    }
    if (Object.keys(obj).length > 0) return obj;
  }
  return { text: raw };
}
