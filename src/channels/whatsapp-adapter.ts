/**
 * Bridge-native WhatsApp channel adapter.
 *
 * Like TelegramBridgeAdapter, this exists because the `@openclaw/whatsapp`
 * plugin hard-wires the openclaw AI-agent dispatch for inbound
 * (dispatchWhatsAppBufferedBlockDispatcher via runChannelInboundEvent), which
 * never calls the bridge deliver/onDeliver seam -- so inbound never reached
 * WS clients and the agent dispatch failed without a configured provider key
 * ("No API key found for provider openai").
 *
 * This adapter owns BOTH legs by talking to Baileys (WhatsApp Web) directly,
 * routing inbound straight to WS clients:
 *
 *   WS client --[WebSocket]--> Bridge --[whatsapp-adapter]--> WhatsApp Web (Baileys)
 *        <--[inbound_message]--        <--[messages.upsert]--
 *
 * Outbound: send_text -> sock.sendMessage(jid, { text }).
 * Inbound:  messages.upsert (type "notify") -> emitMessage() for each message
 *           not authored by the linked self (fromMe), so the bot's own sends
 *           don't echo back as inbound.
 *
 * Auth: reuses the SAME multi-file auth directory the @openclaw/whatsapp
 * plugin writes (~/.openclaw/credentials/whatsapp/<accountId>, or
 * OPENCLAW_STATE_DIR override), so a session linked via the plugin (or via
 * this adapter's own QR flow) is shared -- no separate link needed.
 *
 * QR login: if the auth dir has no creds, loginWithQrStart() opens a Baileys
 * socket and emits a QR; loginWithQrWait() resolves once connection.update
 * reports "open". This mirrors the plugin's flow but keeps the inbound seam.
 *
 * Only ONE Baileys socket may use a given auth dir at a time, so do not run
 * this adapter and the @openclaw/whatsapp plugin gateway against the same
 * account simultaneously.
 */

import {
  makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  DisconnectReason,
  isJidGroup,
  getContentType,
  type WASocket,
} from "baileys";
import { Boom } from "@hapi/boom";
import { join } from "node:path";
import { homedir } from "node:os";
import { mkdirSync } from "node:fs";
import { randomUUID } from "node:crypto";
import QRCode from "qrcode";
import type {
  NormalizedInboundMessage,
  ChannelStatus,
} from "../protocol/messages.js";
import type {
  ChannelAdapter,
  SendTextParams,
  SendMediaParams,
  SendTypingParams,
  SendResult,
  InboundMessageCallback,
  StatusChangeCallback,
  QrStartResult,
  QrWaitResult,
} from "./channel-adapter.js";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("whatsapp-adapter");

interface AccountHandle {
  accountId: string;
  authDir: string;
  sock: WASocket | undefined;
  abortController: AbortController;
  /** Resolves when the socket reaches "open" (connected). */
  openPromise: Promise<void> | undefined;
  openResolve: (() => void) | undefined;
  /** Pending QR string for the current login attempt. */
  pendingQr: string | undefined;
  /** Self JID once known (used to drop fromMe echoes). */
  selfJid: string | undefined;
  /**
   * Recent outbound message ids, keyed by `${remoteJid}:${messageId}`. Populated
   * on send() so the inbound handler can drop *only* verbatim echoes of messages
   * this process sent (NOT fromMe messages originating from the phone / another
   * linked device, which should flow through as inbound). Mirrors @openclaw/whatsapp's
   * recent-outbound cache; TTL 20 min.
   */
  recentOutbound: Map<string, number>;
}

interface WhatsAppAccountCredentials {
  authDir?: string;
  dmPolicy?: string;
  groupPolicy?: string;
  allowFrom?: unknown[];
}

/** TTL for the recent-outbound echo cache (20 min, matching @openclaw/whatsapp). */
const RECENT_OUTBOUND_TTL_MS = 20 * 60_000;

/**
 * Resolve a LID jid ("23945113804945@lid") to its phone-number jid
 * ("85298193482@s.whatsapp.net"), using the LID↔PN mapping Baileys persists in
 * the auth dir (`lid-mapping-<lidUser>_reverse.json` -> "<pnUser>"). WhatsApp
 * delivers DM senders as LIDs; we surface the canonical PN as senderId /
 * chatId so clients (and the test matcher) see a consistent E.164 identity.
 * Falls back to the original jid when no mapping is known.
 */
function lookupPnForLid(authDir: string, jid: string): string {
  if (typeof jid !== "string" || !jid.endsWith("@lid")) return jid;
  try {
    const lidUser = jid.split("@")[0];
    const baseUser = lidUser?.split(":")[0];
    if (!baseUser) return jid;
    const path = join(authDir, `lid-mapping-${baseUser}_reverse.json`);
    const fs = require("node:fs");
    if (!fs.existsSync(path)) return jid;
    const pnUser = JSON.parse(fs.readFileSync(path, "utf-8"));
    if (typeof pnUser === "string" && pnUser) {
      return `${pnUser}@s.whatsapp.net`;
    }
  } catch {
    // mapping not available yet — keep the LID
  }
  return jid;
}

/** Resolve the auth dir for an account, matching the plugin's layout. */
function resolveAuthDir(accountId: string, cred?: WhatsAppAccountCredentials): string {
  if (cred?.authDir) return cred.authDir;
  const stateDir = process.env.OPENCLAW_STATE_DIR?.trim() || join(homedir() || "/root", ".openclaw");
  return join(stateDir, "credentials", "whatsapp", accountId);
}

function ensureDir(dir: string): void {
  try {
    mkdirSync(dir, { recursive: true });
  } catch {
    // best-effort; Baileys/useMultiFileAuthState will surface clearer errors
  }
}

/** Convert a Baileys message content object into a best-effort text body. */
function extractText(message: any): string {
  if (!message) return "";
  const type = getContentType(message) ?? "";
  const content = message[type];
  if (type === "conversation") return String(message.conversation ?? "");
  if (type === "extendedTextMessage") return String(content?.text ?? "");
  if (type === "imageMessage" || type === "videoMessage" || type === "audioMessage") {
    return String(content?.caption ?? "") || `<media:${type.replace("Message", "")}>`;
  }
  if (type === "documentMessage") return String(content?.caption ?? "") || "<media:document>";
  if (type === "locationMessage") {
    const lat = content?.degreesLatitude;
    const lon = content?.degreesLongitude;
    return lat != null && lon != null ? `<location:${lat},${lon}>` : "<location>";
  }
  if (type === "contactMessage" || type === "contactsArrayMessage") return "<contact>";
  if (type && !type.endsWith("SenderKeyDistributionMessage")) return `<media:${type.replace("Message", "")}>`;
  return "";
}

function sleep(ms: number, abortSignal?: AbortSignal): Promise<void> {
  return new Promise((resolveP) => {
    if (abortSignal?.aborted) return resolveP();
    const timer = setTimeout(resolveP, ms);
    abortSignal?.addEventListener("abort", () => {
      clearTimeout(timer);
      resolveP();
    }, { once: true });
  });
}

/** Is the close error a logged-out (non-retryable) status? */
function isLoggedOut(error: unknown): boolean {
  const statusCode = (error as Boom)?.output?.statusCode ?? (error as any)?.status;
  return statusCode === DisconnectReason.loggedOut;
}

/**
 * Baileys expects a pino-like logger with `level` and `debug/info/warn/error/trace/child`.
 * We route into the bridge logger; silent unless OPENCLAW_WHATSAPP_DEBUG is set.
 */
function makeBaileysLogger(): any {
  const level = process.env.OPENCLAW_WHATSAPP_DEBUG ? "debug" : "silent";
  const noop = () => {};
  const fwd = (lvl: "debug" | "info" | "warn" | "error" | "trace") => (obj: any, msg?: any) => {
    if (level === "silent") return;
    try {
      log[lvl === "trace" ? "debug" : lvl](typeof obj === "string" ? obj : msg ?? obj);
    } catch {
      // ignore
    }
  };
  const logger = {
    level,
    trace: fwd("trace"),
    debug: fwd("debug"),
    info: fwd("info"),
    warn: fwd("warn"),
    error: fwd("error"),
    fatal: fwd("error"),
    child: () => logger,
  };
  return logger;
}

// ─── WhatsAppBridgeAdapter ────────────────────────────────────────────────────

/**
 * Bridge-native WhatsApp adapter.
 *
 * Unlike the bundled @openclaw/whatsapp plugin (which hard-wires embedded-agent
 * dispatch and never surfaces inbound to WS clients), this adapter owns both
 * Baileys legs directly: it polls `messages.upsert` for inbound and calls
 * `sock.sendMessage` for outbound, mirroring how TelegramBridgeAdapter owns its
 * getUpdates long-poll. It reuses the plugin's auth dir so a previously-linked
 * session carries over.
 */
export class WhatsAppBridgeAdapter implements ChannelAdapter {
  public readonly channelId = "whatsapp";
  public readonly label = "WhatsApp (bridge-native)";

  private readonly handles = new Map<string, AccountHandle>();
  private readonly statuses = new Map<string, ChannelStatus>();

  private messageCallbacks = new Set<InboundMessageCallback>();
  private statusCallbacks = new Set<StatusChangeCallback>();

  // ─── ChannelAdapter surface ────────────────────────────────────────────────

  onMessage(cb: InboundMessageCallback): void {
    this.messageCallbacks.add(cb);
  }

  onStatusChange(cb: StatusChangeCallback): void {
    this.statusCallbacks.add(cb);
  }

  listAccounts(): string[] {
    return [...this.handles.keys()];
  }

  listSavedAccountIds(): string[] {
    // Persisted accounts = auth dirs that contain a creds.json
    const ids: string[] = [];
    for (const [accountId, handle] of this.handles) {
      try {
        const credsPath = join(handle.authDir, "creds.json");
        const fs = require("node:fs");
        if (fs.existsSync(credsPath)) ids.push(accountId);
      } catch {
        // ignore
      }
    }
    return ids;
  }

  getStatus(accountId: string): ChannelStatus {
    return (
      this.statuses.get(accountId) ?? {
        channel: "whatsapp",
        accountId,
        connected: false,
        state: "disconnected",
      }
    );
  }

  private updateStatus(accountId: string, patch: Partial<ChannelStatus>): void {
    const prev = this.getStatus(accountId);
    const next: ChannelStatus = { ...prev, ...patch, channel: "whatsapp", accountId };
    this.statuses.set(accountId, next);
    for (const cb of this.statusCallbacks) {
      try {
        cb(next);
      } catch (err) {
        log.warn("status callback threw", { accountId, error: String(err) });
      }
    }
  }

  private requireHandle(accountId: string): AccountHandle {
    const handle = this.handles.get(accountId);
    if (!handle) {
      throw new Error(`WhatsApp account not started: ${accountId}`);
    }
    return handle;
  }

  // ─── Lifecycle: start / stop ───────────────────────────────────────────────

  async start(accountId: string, credentials: Record<string, unknown>): Promise<void> {
    if (this.handles.has(accountId)) {
      log.warn("WhatsApp account already started, ignoring start()", { accountId });
      return;
    }

    const cred = credentials as WhatsAppAccountCredentials;
    const authDir = resolveAuthDir(accountId, cred);
    ensureDir(authDir);

    const handle: AccountHandle = {
      accountId,
      authDir,
      sock: undefined,
      abortController: new AbortController(),
      openPromise: undefined,
      openResolve: undefined,
      pendingQr: undefined,
      selfJid: undefined,
      recentOutbound: new Map(),
    };
    handle.openPromise = new Promise<void>((resolveOpen) => {
      handle.openResolve = resolveOpen;
    });
    this.handles.set(accountId, handle);

    await this.connectSocket(handle);
  }

  /** Create the Baileys socket and wire its event handlers. */
  private async connectSocket(handle: AccountHandle): Promise<void> {
    const { accountId, authDir, abortController } = handle;

    const { state, saveCreds } = await useMultiFileAuthState(authDir);
    const { version } = await fetchLatestBaileysVersion();

    // Reuse the plugin's persisted identity if present (carry-over linkage).
    if (state.creds?.me?.id) {
      handle.selfJid = state.creds.me.id;
    }

    const baileysLogger = makeBaileysLogger();

    const sock = makeWASocket({
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, baileysLogger) },
      version,
      printQRInTerminal: false,
      browser: ["openclaw", "cli", "1.0.0"],
      markOnlineOnConnect: false,
      logger: baileysLogger,
      abortSignal: abortController instanceof AbortController ? undefined : undefined,
    } as any);
    handle.sock = sock;

    // Persist credentials whenever they update (keep session link fresh).
    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("connection.update", (update: any) => {
      const { connection, qr, lastDisconnect, receivedPendingNotifications } = update || {};

      if (qr) {
        handle.pendingQr = qr;
        log.info("QR code emitted; awaiting scan", { accountId });
      }

      if (connection === "open") {
        const me = (sock as any).user;
        if (me?.id) handle.selfJid = me.id;
        this.updateStatus(accountId, {
          connected: true,
          state: "connected",
          detail: receivedPendingNotifications ? "connected" : "connected (syncing)",
          lastError: undefined,
        });
        log.info("WhatsApp socket open", { accountId, selfJid: handle.selfJid });
        handle.openResolve?.();
      } else if (connection === "connecting") {
        this.updateStatus(accountId, { connected: false, state: "reconnecting", detail: "connecting" });
      } else if (connection === "close") {
        const shouldReconnect =
          lastDisconnect && !isLoggedOut(lastDisconnect.error);
        this.updateStatus(accountId, {
          connected: false,
          state: shouldReconnect ? "reconnecting" : "disconnected",
          detail: shouldReconnect ? "reconnecting" : "logged out",
          lastError: lastDisconnect?.error ? String(lastDisconnect.error) : undefined,
        });
        if (shouldReconnect && !abortController.signal.aborted) {
          // Reconnect with a fresh socket bound to the same handle.
          log.warn("WhatsApp closed, reconnecting", { accountId, error: String(lastDisconnect?.error) });
          setImmediate(() => {
            if (!abortController.signal.aborted) void this.connectSocket(handle);
          });
        } else {
          log.error("WhatsApp closed (logged out / aborted), not reconnecting", { accountId });
        }
      }
    });

    sock.ev.on("messages.upsert", (upsert: any) => {
      if (upsert?.type !== "notify") return;
      for (const env of upsert.messages || []) {
        this.handleInbound(handle, env);
      }
    });
  }

  async stop(accountId: string): Promise<void> {
    const handle = this.handles.get(accountId);
    if (!handle) return;
    handle.abortController.abort();
    try {
      await handle.sock?.end?.(undefined);
    } catch (err) {
      log.warn("error closing socket", { accountId, error: String(err) });
    }
    handle.sock = undefined;
    this.updateStatus(accountId, { connected: false, state: "disconnected", detail: "stopped" });
    this.handles.delete(accountId);
  }

  async stopAll(): Promise<void> {
    const ids = [...this.handles.keys()];
    await Promise.all(ids.map((id) => this.stop(id)));
  }

  // ─── Outbound: sendText / sendMedia / sendTyping ───────────────────────────

  /**
   * Wait for the socket to be open, with backoff. Baileys opens its listener
   * asynchronously after start(), so the first outbound can race a transient
   * "No active WhatsApp Web listener" error — retry until ack or deadline.
   */
  private async waitForReady(handle: AccountHandle, timeoutMs = 60_000): Promise<WASocket> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (handle.abortController.signal.aborted) {
        throw new Error(`WhatsApp account ${handle.accountId} was stopped`);
      }
      const sock = handle.sock;
      if (sock?.user) return sock;
      // Drain the open promise, but cap via deadline so a never-opening
      // socket doesn't hang forever.
      if (handle.openPromise) {
        const raced = await Promise.race([
          handle.openPromise,
          sleep(2_000, handle.abortController.signal),
        ]);
        if (handle.sock?.user) return handle.sock;
      }
      await sleep(1_000, handle.abortController.signal);
    }
    throw new Error(`WhatsApp socket for ${handle.accountId} did not open within ${timeoutMs}ms`);
  }

  async sendText(params: SendTextParams): Promise<SendResult> {
    const accountId = params.accountId ?? "default";
    const handle = this.requireHandle(accountId);
    const sock = await this.waitForReady(handle);

    const to = this.normalizeTarget(params.to);
    try {
      const result = await sock.sendMessage(to, { text: params.text });
      this.updateStatus(accountId, { lastOutboundAt: Date.now() });
      const messageId =
        (result as any)?.key?.id ??
        randomUUID();
      this.rememberOutbound(handle, to, messageId);
      return { messageId, chatId: to };
    } catch (err: any) {
      const msg = String(err?.message ?? err);
      // Transient listener race: retry once after a short settle.
      if (/no active.*listener|not connected| timed out/i.test(msg)) {
        log.warn("transient send error, retrying", { accountId, error: msg });
        await sleep(3_000, handle.abortController.signal);
        const retry = await sock.sendMessage(to, { text: params.text });
        this.updateStatus(accountId, { lastOutboundAt: Date.now() });
        const retryId = (retry as any)?.key?.id ?? randomUUID();
        this.rememberOutbound(handle, to, retryId);
        return { messageId: retryId, chatId: to };
      }
      throw err;
    }
  }

  /** Record an outbound message id so its echo can be dropped on the inbound side. */
  private rememberOutbound(handle: AccountHandle, remoteJid: string, messageId: string): void {
    if (!messageId) return;
    handle.recentOutbound.set(`${remoteJid}:${messageId}`, Date.now());
    // Opportunistic GC: cap at 5000 entries, drop oldest-expired.
    if (handle.recentOutbound.size > 5000) {
      const now = Date.now();
      for (const [k, ts] of handle.recentOutbound) {
        if (now - ts > RECENT_OUTBOUND_TTL_MS) handle.recentOutbound.delete(k);
      }
    }
  }

  /**
   * Resolve a LID jid to its canonical phone-number jid for a stable E.164
   * identity. Prefers the PN Baileys attaches alongside the LID
   * (`remoteJidAlt`), then the persisted LID→PN mapping file, then the raw jid.
   */
  private resolvePnJid(
    handle: AccountHandle,
    jid: string,
    remoteJidAlt?: string,
  ): string {
    if (typeof jid !== "string" || !jid.endsWith("@lid")) return jid;
    if (typeof remoteJidAlt === "string" && remoteJidAlt.endsWith("@s.whatsapp.net")) {
      return remoteJidAlt;
    }
    return lookupPnForLid(handle.authDir, jid);
  }

  /** Is this inbound message a verbatim echo of a message we just sent? */
  private isOutboundEcho(handle: AccountHandle, remoteJid: string, messageId: string): boolean {
    const key = `${remoteJid}:${messageId}`;
    const sentAt = handle.recentOutbound.get(key);
    if (sentAt == null) return false;
    if (Date.now() - sentAt > RECENT_OUTBOUND_TTL_MS) {
      handle.recentOutbound.delete(key);
      return false;
    }
    return true;
  }

  async sendMedia(params: SendMediaParams): Promise<SendResult> {
    const accountId = params.accountId ?? "default";
    const handle = this.requireHandle(accountId);
    const sock = await this.waitForReady(handle);
    const to = this.normalizeTarget(params.to);

    // Fetch the media bytes from the URL/path, then send as document (generic,
    // works for any media type without sniffing mime specifics here).
    const fs = await import("node:fs/promises");
    let buffer: Buffer;
    if (/^https?:\/\//i.test(params.mediaUrl)) {
      const res = await globalThis.fetch!(params.mediaUrl);
      if (!res.ok) throw new Error(`failed to fetch media (${res.status})`);
      buffer = Buffer.from(await res.arrayBuffer());
    } else {
      buffer = await fs.readFile(params.mediaUrl);
    }

    const result = await sock.sendMessage(to, {
      document: buffer,
      fileName: params.mediaUrl.split("/").pop() || "file",
      caption: params.text,
      mimetype: params.mediaType,
    } as any);
    this.updateStatus(accountId, { lastOutboundAt: Date.now() });
    const messageId = (result as any)?.key?.id ?? randomUUID();
    this.rememberOutbound(handle, to, messageId);
    return { messageId, chatId: to };
  }

  async sendTyping(params: SendTypingParams): Promise<void> {
    const accountId = params.accountId ?? "default";
    const handle = this.requireHandle(accountId);
    const sock = handle.sock;
    if (!sock) return;
    const to = this.normalizeTarget(params.to);
    try {
      if (params.typing) {
        await sock.sendPresenceUpdate("composing", to);
      } else {
        await sock.sendPresenceUpdate("paused", to);
      }
    } catch (err) {
      log.warn("sendTyping failed", { accountId, error: String(err) });
    }
  }

  /** Normalize a target id to a WhatsApp JID if it isn't one already. */
  private normalizeTarget(to: string): string {
    if (/@s\.whatsapp\.net$/i.test(to) || /@g\.us$/i.test(to)) return to;
    // Bare E.164 -> individual JID.
    const digits = to.replace(/[^\d]/g, "");
    return `${digits}@s.whatsapp.net`;
  }

  // ─── QR login ──────────────────────────────────────────────────────────────

  async loginWithQrStart(params: { accountId?: string; force?: boolean }): Promise<QrStartResult> {
    const accountId = params.accountId ?? "default";
    // If a handle already exists and is linked (selfJid known), do not
    // re-issue a QR — it would force-logout the existing session. The caller
    // should instead rely on start() resuming the linked creds.
    let handle = this.handles.get(accountId);
    if (!handle) {
      await this.start(accountId, {});
      handle = this.handles.get(accountId);
    }
    if (!handle) {
      return { message: "failed to start account for QR login" };
    }

    // If already linked, surface that rather than destroying the link.
    if (handle.selfJid && !params.force) {
      return {
        message: `Account ${accountId} is already linked (${handle.selfJid}); no QR needed.`,
        qrDataUrl: undefined,
      };
    }

    if (params.force) {
      this.updateStatus(accountId, { connected: false, state: "disconnected", detail: "force relink requested" });
      handle.selfJid = undefined;
    }

    // Wait briefly for Baileys to emit a QR (it does so when unlinked).
    const qr = await this.waitForQr(handle, 30_000);
    if (!qr) {
      return { message: "No QR emitted — the session may already be linked. Try without force." };
    }
    const qrDataUrl = await QRCode.toDataURL(qr);
    return { qrDataUrl, message: "Scan the QR code with WhatsApp → Linked Devices." };
  }

  async loginWithQrWait(params: {
    accountId?: string;
    sessionKey?: string;
    timeoutMs?: number;
  }): Promise<QrWaitResult> {
    const accountId = params.accountId ?? "default";
    const handle = this.handles.get(accountId);
    if (!handle) {
      return { connected: false, message: `Account ${accountId} not started` };
    }

    const timeoutMs = params.timeoutMs ?? 120_000;
    const deadline = Date.now() + timeoutMs;
    // Poll the handle's status until connected (QR scanned) or timeout.
    while (Date.now() < deadline) {
      if (handle.abortController.signal.aborted) {
        return { connected: false, message: "aborted" };
      }
      const status = this.getStatus(accountId);
      if (status.connected) {
        return {
          connected: true,
          message: `WhatsApp login succeeded (${handle.selfJid ?? accountId})`,
          accountId,
        };
      }
      await sleep(2_000, handle.abortController.signal);
    }
    // Refresh QR if one is pending so the client can display a fresh code.
    let qrDataUrl: string | undefined;
    if (handle.pendingQr) {
      qrDataUrl = await QRCode.toDataURL(handle.pendingQr);
    }
    return { connected: false, message: "QR login timed out", qrDataUrl };
  }

  private async waitForQr(handle: AccountHandle, timeoutMs: number): Promise<string | undefined> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (handle.pendingQr) {
        const qr = handle.pendingQr;
        handle.pendingQr = undefined;
        return qr;
      }
      if (this.getStatus(handle.accountId).connected) return undefined;
      await sleep(1_000, handle.abortController.signal);
    }
    return undefined;
  }

  // ─── Inbound routing ───────────────────────────────────────────────────────

  private handleInbound(handle: AccountHandle, env: any): void {
    try {
      const key = env?.key;
      if (!key) return;

      const rawChatId: string = key.remoteJid ?? key.participant ?? "";
      if (!rawChatId) return;

      // WhatsApp delivers DM senders/chats as LIDs (Linked Identities). For a
      // stable E.164 identity we prefer the PN that Baileys attaches alongside
      // the LID (`key.remoteJidAlt`), then fall back to the persisted LID→PN
      // mapping file, then the raw LID. Group jids are left as-is.
      const chatId: string = isJidGroup(rawChatId)
        ? rawChatId
        : this.resolvePnJid(handle, rawChatId, key.remoteJidAlt);

      // Echo suppression (matches @openclaw/whatsapp): a fromMe message is only
      // dropped if it is a verbatim echo of a message this process sent recently
      // (tracked via the recent-outbound cache). A fromMe message that
      // originated from the phone / another linked device — which this process
      // did NOT send — has no cache entry and flows through as inbound. This is
      // what allows a self-chat round-trip (phone -> bot) to be observed.
      if (key.fromMe && key.id && this.isOutboundEcho(handle, chatId, key.id)) {
        log.debug("dropping outbound echo", { accountId: handle.accountId, messageId: key.id });
        return;
      }

      const rawSender: string =
        (isJidGroup(rawChatId) ? key.participant : key.remoteJid) ?? rawChatId;
      const senderId: string = isJidGroup(rawChatId)
        ? rawSender
        : this.resolvePnJid(handle, rawSender, key.remoteJidAlt);

      const message = env?.message ?? {};
      const text = extractText(message);
      const contentType = getContentType(message);

      // Skip pure presence/receipt events with no consumable content.
      if (!text && !message?.audioMessage && !message?.imageMessage && !message?.videoMessage && !message?.documentMessage && !message?.locationMessage) {
        return;
      }

      const msgType = this.classifyMsgType(contentType);

      const inbound: NormalizedInboundMessage = {
        channel: "whatsapp",
        accountId: handle.accountId,
        messageId: key.id ?? randomUUID(),
        chatId,
        senderId: senderId ?? chatId,
        replyTo: chatId,
        msgType,
        text,
        timestamp: (env?.messageTimestamp ? Number(env.messageTimestamp) * 1000 : Date.now()),
        wasEncrypted: true,
        replyToMessageId: message?.extendedTextMessage?.contextInfo?.stanzaId,
        raw: env,
      };

      this.emitMessage(inbound);
      this.updateStatus(handle.accountId, { lastInboundAt: Date.now() });
    } catch (err) {
      log.warn("failed to normalize inbound message", { accountId: handle.accountId, error: String(err) });
    }
  }

  private classifyMsgType(contentType: string | undefined): string {
    switch (contentType) {
      case "conversation":
      case "extendedTextMessage":
        return "text";
      case "imageMessage":
        return "image";
      case "videoMessage":
        return "video";
      case "audioMessage":
        return "voice";
      case "documentMessage":
        return "file";
      case "locationMessage":
        return "text";
      default:
        return "text";
    }
  }

  private emitMessage(msg: NormalizedInboundMessage): void {
    for (const cb of this.messageCallbacks) {
      try {
        cb(msg);
      } catch (err) {
        log.warn("message callback threw", { accountId: msg.accountId, error: String(err) });
      }
    }
  }
}
