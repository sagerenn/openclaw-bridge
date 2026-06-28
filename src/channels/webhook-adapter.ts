/**
 * Bridge-native webhook channel adapter.
 *
 * The counterpart to the standalone webhook-receiver (../../webhook-receiver/).
 * Channels whose IM platform can only push *outbound webhooks* (e.g. MS Teams
 * outgoing webhooks, Slack, generic webhook bots) cannot hold a long-lived
 * inbound connection back to the bridge. Instead, the platform POSTs inbound
 * events to the standalone receiver, and THIS adapter polls that receiver:
 *
 *   IM --[POST]--> webhook-receiver --[poll]--> WebhookBridgeAdapter --[WS]--> client
 *
 * The adapter is registered under the *logical* channel id (e.g. "msteams") so
 * that `send_text` envelopes addressed to that channel route here. Each account
 * carries the receiver's poll URL (+ optional bearer) and a poll interval.
 *
 *   ws client -> bridge -> WebhookBridgeAdapter --(optional outboundUrl)--> IM
 *   IM -> webhook-receiver <- (poll) <- WebhookBridgeAdapter -> ws client
 *
 * Outbound is optional: if the account configures `outboundUrl`, send_text /
 * send_media POST a normalized payload to it (a relay that turns the payload
 * into the platform's send-message API call). Without `outboundUrl`, the
 * channel is inbound-only and send_* throws — the webhook receiver is a
 * one-way inbound buffer by design.
 */

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

const log = rootLogger.child("webhook-adapter");

interface WebhookAccountCredentials {
  /** Full poll URL on the receiver, e.g. https://host/api/webhook/<token>/poll */
  pollUrl?: string;
  /** Optional bearer presented to the receiver (WH_POLL_TOKEN). */
  bearer?: string;
  /** Poll cadence in ms (default 3000). */
  pollIntervalMs?: number;
  /**
   * Optional relay URL the adapter POSTs outbound send_text/send_media payloads
   * to. The relay is responsible for translating them into the IM platform's
   * send-message API. Without it, the channel is inbound-only.
   */
  outboundUrl?: string;
  /** Optional bearer for the outbound relay. */
  outboundBearer?: string;
  /** Channel id to register under (default "webhook"). Lets one adapter serve
   * multiple logical channels — e.g. configure accounts under "msteams". */
  channel?: string;
}

interface AccountHandle {
  accountId: string;
  channelId: string;
  pollUrl: string;
  bearer?: string;
  outboundUrl?: string;
  outboundBearer?: string;
  abortController: AbortController;
  pollTimer?: ReturnType<typeof setTimeout>;
  /** The id of the last message delivered to WS clients (ack cursor). */
  lastSeenId: string | null;
}

const DEFAULT_POLL_MS = 3000;

/**
 * A single adapter instance can serve ONE logical channel id (set at
 * construction). The bridge registers one per channel that needs webhook
 * inbound — see registerWebhookAdapters() at the bottom of this file.
 */
export class WebhookBridgeAdapter implements ChannelAdapter {
  readonly channelId: string;
  readonly label: string;

  private handles = new Map<string, AccountHandle>();
  private statuses = new Map<string, ChannelStatus>();
  private messageCallbacks: InboundMessageCallback[] = [];
  private statusCallbacks: StatusChangeCallback[] = [];

  constructor(channelId = "webhook") {
    this.channelId = channelId;
    this.label = `Webhook (${channelId})`;
  }

  onMessage(callback: InboundMessageCallback): void {
    this.messageCallbacks.push(callback);
  }

  onStatusChange(callback: StatusChangeCallback): void {
    this.statusCallbacks.push(callback);
  }

  listAccounts(): string[] {
    return [...this.handles.keys()];
  }

  listSavedAccountIds(): string[] {
    return [];
  }

  getStatus(accountId: string): ChannelStatus {
    return this.statuses.get(accountId) ?? {
      channel: this.channelId,
      accountId,
      connected: false,
      state: "disconnected",
    };
  }

  private updateStatus(accountId: string, state: ChannelStatus["state"], detail?: string, lastError?: string): void {
    const status: ChannelStatus = {
      ...this.getStatus(accountId),
      channel: this.channelId,
      accountId,
      connected: state === "connected",
      state,
      ...(detail !== undefined ? { detail } : {}),
      ...(lastError !== undefined ? { lastError } : {}),
    };
    this.statuses.set(accountId, status);
    for (const cb of this.statusCallbacks) cb(status);
  }

  async start(accountId: string, credentials: Record<string, unknown>): Promise<void> {
    if (this.handles.has(accountId)) {
      log.warn("Account already started", { channelId: this.channelId, accountId });
      return;
    }
    const cred = credentials as WebhookAccountCredentials;
    const pollUrl = (cred.pollUrl ?? "").trim();
    if (!pollUrl) {
      throw new Error(
        `webhook channel "${this.channelId}" account "${accountId}" is missing pollUrl ` +
        `(set channels.${this.channelId}.accounts.${accountId}.pollUrl to the receiver's ` +
        `/webhook/<token>/poll URL).`,
      );
    }

    const handle: AccountHandle = {
      accountId,
      channelId: this.channelId,
      pollUrl,
      bearer: cred.bearer,
      outboundUrl: cred.outboundUrl,
      outboundBearer: cred.outboundBearer,
      abortController: new AbortController(),
      lastSeenId: null,
    };
    this.handles.set(accountId, handle);

    this.updateStatus(accountId, "connected", `Polling ${pollUrl}`);
    log.info("Webhook account started", { channelId: this.channelId, accountId, pollUrl });

    // Kick the poll loop immediately, then on an interval.
    this.schedulePoll(handle, 0);
  }

  async stop(accountId: string): Promise<void> {
    const handle = this.handles.get(accountId);
    if (!handle) return;
    handle.abortController.abort();
    if (handle.pollTimer) clearTimeout(handle.pollTimer);
    this.handles.delete(accountId);
    this.updateStatus(accountId, "disconnected", "Stopped");
    log.info("Webhook account stopped", { channelId: this.channelId, accountId });
  }

  async stopAll(): Promise<void> {
    await Promise.all([...this.handles.keys()].map((id) => this.stop(id)));
  }

  async sendText(params: SendTextParams): Promise<SendResult> {
    const handle = this.requireHandle(params.accountId);
    if (!handle.outboundUrl) {
      throw new Error(
        `webhook channel "${this.channelId}" is inbound-only ` +
        `(no outboundUrl configured for account ${handle.accountId}).`,
      );
    }
    const body = {
      kind: "send_text",
      to: params.to,
      text: params.text,
      replyToMessageId: params.replyToMessageId,
      accountId: handle.accountId,
    };
    const res = await this.postOutbound(handle, body);
    this.markOutbound(handle.accountId);
    return {
      messageId: String(res?.messageId ?? res?.id ?? ""),
      chatId: res?.chatId != null ? String(res.chatId) : params.to,
    };
  }

  async sendMedia(params: SendMediaParams): Promise<SendResult> {
    const handle = this.requireHandle(params.accountId);
    if (!handle.outboundUrl) {
      throw new Error(
        `webhook channel "${this.channelId}" is inbound-only ` +
        `(no outboundUrl configured for account ${handle.accountId}).`,
      );
    }
    const body = {
      kind: "send_media",
      to: params.to,
      mediaUrl: params.mediaUrl,
      text: params.text,
      mediaType: params.mediaType,
      accountId: handle.accountId,
    };
    const res = await this.postOutbound(handle, body);
    this.markOutbound(handle.accountId);
    return {
      messageId: String(res?.messageId ?? res?.id ?? ""),
      chatId: res?.chatId != null ? String(res.chatId) : params.to,
    };
  }

  async sendTyping(_params: SendTypingParams): Promise<void> {
    // Typing indicators are not meaningful for a polled webhook channel.
  }

  // --- QR login is not applicable to webhook channels. ----------------------

  async loginWithQrStart(_params: { accountId?: string; force?: boolean }): Promise<QrStartResult> {
    throw new Error("QR login is not supported by the webhook adapter");
  }

  async loginWithQrWait(_params: { accountId?: string; sessionKey?: string; timeoutMs?: number }): Promise<QrWaitResult> {
    throw new Error("QR login is not supported by the webhook adapter");
  }

  // --- Internals -----------------------------------------------------------

  private requireHandle(accountId?: string): AccountHandle {
    const id = accountId ?? this.handles.keys().next().value;
    const handle = id ? this.handles.get(id) : undefined;
    if (!handle) {
      throw new Error(`No started webhook account (requested: ${accountId ?? "<none>"})`);
    }
    return handle;
  }

  private markOutbound(accountId: string): void {
    const status = this.getStatus(accountId);
    this.statuses.set(accountId, { ...status, lastOutboundAt: Date.now() });
  }

  private emitMessage(msg: NormalizedInboundMessage): void {
    const status = this.getStatus(msg.accountId);
    this.statuses.set(msg.accountId, { ...status, lastInboundAt: Date.now() });
    for (const cb of this.messageCallbacks) cb(msg);
  }

  /** Schedule the next poll; honors the abort signal so stop() halts cleanly. */
  private schedulePoll(handle: AccountHandle, delayMs: number): void {
    if (handle.abortController.signal.aborted) return;
    handle.pollTimer = setTimeout(() => {
      this.pollOnce(handle)
        .catch((err) => {
          if (!handle.abortController.signal.aborted) {
            log.warn("Poll error (will retry)", { channelId: handle.channelId, accountId: handle.accountId, error: String(err) });
            this.updateStatus(handle.accountId, "reconnecting", `Poll error: ${err}`);
          }
        })
        .finally(() => {
          if (!handle.abortController.signal.aborted) {
            // Re-read interval from a fresh lookup in case config changed.
            const h = this.handles.get(handle.accountId);
            this.schedulePoll(handle, h ? 0 : 0);
          }
        });
    }, delayMs);
  }

  /** One poll cycle: GET the receiver, emit any new messages, ack the batch. */
  private async pollOnce(handle: AccountHandle): Promise<void> {
    const url = new URL(handle.pollUrl);
    if (handle.lastSeenId) url.searchParams.set("ack", handle.lastSeenId);

    const res = await fetch(url, {
      method: "GET",
      headers: {
        ...(handle.bearer ? { authorization: `Bearer ${handle.bearer}` } : {}),
        accept: "application/json",
      },
      signal: handle.abortController.signal,
    });

    if (!res.ok) {
      const detail = await res.text().catch(() => "");
      throw new Error(`poll HTTP ${res.status}: ${detail}`);
    }

    const data = (await res.json()) as {
      messages?: Array<Record<string, unknown>>;
    };
    const messages = data.messages ?? [];
    if (messages.length === 0) return;

    let newest: string | null = handle.lastSeenId;
    for (const m of messages) {
      const id = typeof m.id === "string" ? m.id : undefined;
      if (id && (!newest || idAfter(id, newest))) newest = id;
      this.emitMessage(toNormalized(handle, m));
    }
    handle.lastSeenId = newest;
    log.debug("Webhook poll drained", { channelId: handle.channelId, accountId: handle.accountId, count: messages.length });
  }

  /** POST an outbound payload to the relay. */
  private async postOutbound(handle: AccountHandle, body: unknown): Promise<any> {
    const res = await fetch(handle.outboundUrl!, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(handle.outboundBearer ? { authorization: `Bearer ${handle.outboundBearer}` } : {}),
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`outbound relay HTTP ${res.status}: ${await res.text().catch(() => "")}`);
    }
    return await res.json().catch(() => ({}));
  }
}

// ─── helpers ─────────────────────────────────────────────────────────────────

/** Convert a receiver poll message into the bridge's NormalizedInboundMessage. */
function toNormalized(handle: AccountHandle, m: Record<string, unknown>): NormalizedInboundMessage {
  const str = (k: string): string | undefined => {
    const v = m[k];
    return typeof v === "string" ? v : typeof v === "number" ? String(v) : undefined;
  };
  const text = str("text") ?? "";
  return {
    channel: handle.channelId,
    accountId: handle.accountId,
    messageId: str("messageId") ?? str("id") ?? `wh-${Date.now()}`,
    chatId: str("chatId") ?? "",
    senderId: str("senderId") ?? "",
    replyTo: str("replyTo") ?? str("chatId"),
    senderName: str("senderName"),
    msgType: str("msgType") ?? (text ? "text" : "system"),
    text,
    timestamp: typeof m.timestamp === "number" ? m.timestamp : Date.now(),
    mediaUrl: str("mediaUrl"),
    mediaType: str("mediaType"),
    raw: m.raw ?? m,
  };
}

/** Compare two receiver message ids (base-36-ish) for "a comes after b". */
function idAfter(a: string, b: string): boolean {
  const na = parseInt(a, 36);
  const nb = parseInt(b, 36);
  if (!Number.isNaN(na) && !Number.isNaN(nb)) return na > nb;
  return a > b;
}

// ─── Registration ─────────────────────────────────────────────────────────────

/**
 * Discover webhook channels from the bridge config and register an adapter for
 * each. A channel is treated as a webhook channel when its config section sets
 * `webhook: true` OR any of its accounts carries a `pollUrl`. This lets the
 * same generic adapter serve multiple logical channels (msteams, slack, …) by
 * mounting one adapter per channel id.
 *
 * Returns the registered adapters so the caller can register them on the
 * ChannelManager after the plugin loaders (so a webhook channel wins its id
 * slot, mirroring how TelegramBridgeAdapter overrides the bundled telegram
 * plugin).
 */
export function registerWebhookAdapters(
  config: { channels?: Record<string, any> },
  manager: { registerAdapter: (a: ChannelAdapter) => void },
): WebhookBridgeAdapter[] {
  const registered: WebhookBridgeAdapter[] = [];
  const channels = config.channels ?? {};

  for (const [channelId, section] of Object.entries(channels)) {
    if (!section || typeof section !== "object") continue;
    const isWebhook = section.webhook === true || accountsHavePollUrl(section.accounts);
    if (!isWebhook) continue;

    const adapter = new WebhookBridgeAdapter(channelId);
    manager.registerAdapter(adapter);
    registered.push(adapter);
    log.info("Registered webhook adapter for channel", { channelId });
  }

  return registered;
}

function accountsHavePollUrl(accounts: unknown): boolean {
  if (!accounts || typeof accounts !== "object") return false;
  for (const acc of Object.values(accounts as Record<string, any>)) {
    if (acc && typeof acc.pollUrl === "string" && acc.pollUrl.length > 0) return true;
  }
  return false;
}
