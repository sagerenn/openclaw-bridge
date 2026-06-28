/**
 * Bridge-native Telegram channel adapter.
 *
 * Unlike OpenClawChannelAdapter (which drives a bundled openclaw channel
 * plugin through its gateway + reply pipeline), this adapter talks to the
 * Telegram Bot API directly over HTTP. It exists because the bundled
 * `telegram` plugin hard-wires the real openclaw AI-agent dispatch for
 * inbound (dispatchReplyWithBufferedBlockDispatcher), which never calls the
 * bridge deliver/onDeliver seam -- so inbound never reached WS clients and
 * the agent dispatch failed ("Missing API key for provider openai").
 *
 * This adapter reuses the Telegram Bot API itself (the same endpoints the
 * openclaw plugin uses) and the bridge ChannelAdapter framework, owning BOTH
 * legs so inbound is routed straight to WS clients:
 *
 *   WS client --[WebSocket]--> Bridge --[telegram-adapter]--> Telegram Bot API
 *        <--[inbound_message]--        <--[getUpdates long-poll]--
 *
 * Outbound: send_text -> POST /bot<token>/sendMessage.
 * Inbound:  a single getUpdates long-poll loop per account -> emitMessage().
 *
 * One bot token supports only ONE active getUpdates consumer, so this adapter
 * must be the sole telegram poller for a token (do not also run the bundled
 * telegram plugin gateway against the same token).
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

const log = rootLogger.child("telegram-adapter");

interface AccountHandle {
  accountId: string;
  token: string;
  apiRoot: string;
  abortController: AbortController;
  lastUpdateId: number;
}

interface TelegramAccountCredentials {
  botToken?: string;
  token?: string;
  apiRoot?: string;
  dmPolicy?: string;
  groupPolicy?: string;
  allowFrom?: unknown[];
}

const DEFAULT_API_ROOT = "https://api.telegram.org";
// Telegram getUpdates long-poll timeout (seconds). The HTTP request itself
// uses this + a margin so the server can hold the connection open.
const LONG_POLL_TIMEOUT_SEC = 30;
const POLL_REQUEST_MARGIN_MS = 5000;
const RECONNECT_DELAY_MS = 2000;

function resolveToken(cred: TelegramAccountCredentials): string {
  const token = (cred.botToken ?? cred.token ?? "").trim();
  if (!token) {
    throw new Error(
      "Telegram account is missing botToken (set channels.telegram.accounts.<id>.botToken).",
    );
  }
  return token;
}

function resolveApiRoot(cred: TelegramAccountCredentials): string {
  const root = (cred.apiRoot ?? DEFAULT_API_ROOT).trim().replace(/\/$/, "");
  return root || DEFAULT_API_ROOT;
}

export class TelegramBridgeAdapter implements ChannelAdapter {
  readonly channelId = "telegram";
  readonly label = "Telegram (bridge-native)";

  private handles = new Map<string, AccountHandle>();
  private statuses = new Map<string, ChannelStatus>();
  private messageCallbacks: InboundMessageCallback[] = [];
  private statusCallbacks: StatusChangeCallback[] = [];

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
    if (state === "connected") {
      status.lastInboundAt = this.getStatus(accountId).lastInboundAt;
      status.lastOutboundAt = this.getStatus(accountId).lastOutboundAt;
    }
    this.statuses.set(accountId, status);
    for (const cb of this.statusCallbacks) cb(status);
  }

  async start(accountId: string, credentials: Record<string, unknown>): Promise<void> {
    if (this.handles.has(accountId)) {
      log.warn("Account already started", { channelId: this.channelId, accountId });
      return;
    }
    const cred = credentials as TelegramAccountCredentials;
    const token = resolveToken(cred);
    const apiRoot = resolveApiRoot(cred);
    const abortController = new AbortController();

    const handle: AccountHandle = { accountId, token, apiRoot, abortController, lastUpdateId: 0 };
    this.handles.set(accountId, handle);
    this.updateStatus(accountId, "reconnecting", "Starting...");

    // Validate the token and confirm the bot identity before polling.
    try {
      const me = await this.botApi(handle, "getMe", {});
      log.info("Telegram bot connected", { accountId, username: me.result?.username, id: me.result?.id });
    } catch (err: any) {
      this.updateStatus(accountId, "error", `getMe failed: ${err}`, String(err));
      throw err;
    }

    // Drop any webhook so getUpdates (long-poll) works; a registered webhook
    // would otherwise cause getUpdates to 409-conflict.
    try {
      await this.botApi(handle, "deleteWebhook", { drop_pending_updates: false });
    } catch (err: any) {
      log.warn("deleteWebhook failed (continuing)", { accountId, error: String(err) });
    }

    this.updateStatus(accountId, "connected", "Polling");
    // Fire-and-forget the long-poll loop; it runs until stop().
    this.pollLoop(handle).catch((err) => {
      if (!abortController.signal.aborted) {
        log.error("Telegram poll loop exited unexpectedly", { accountId, error: String(err) });
        this.updateStatus(accountId, "error", `Poll loop exited: ${err}`, String(err));
      }
    });
  }

  async stop(accountId: string): Promise<void> {
    const handle = this.handles.get(accountId);
    if (!handle) return;
    handle.abortController.abort();
    this.handles.delete(accountId);
    this.updateStatus(accountId, "disconnected", "Stopped");
    log.info("Telegram account stopped", { accountId });
  }

  async stopAll(): Promise<void> {
    await Promise.all([...this.handles.keys()].map((id) => this.stop(id)));
  }

  async sendText(params: SendTextParams): Promise<SendResult> {
    const handle = this.requireHandle(params.accountId);
    const body: Record<string, unknown> = {
      chat_id: params.to,
      text: params.text,
    };
    if (params.replyToMessageId) body.reply_to_message_id = params.replyToMessageId;
    const res = await this.botApi(handle, "sendMessage", body);
    const result = res.result ?? {};
    this.markOutbound(handle.accountId);
    return {
      messageId: String(result.message_id ?? ""),
      chatId: result.chat?.id != null ? String(result.chat.id) : params.to,
    };
  }

  async sendMedia(_params: SendMediaParams): Promise<SendResult> {
    throw new Error("sendMedia is not supported by the bridge-native telegram adapter");
  }

  async sendTyping(params: SendTypingParams): Promise<void> {
    const handle = this.requireHandle(params.accountId);
    try {
      await this.botApi(handle, "sendChatAction", { chat_id: params.to, action: "typing" });
    } catch (err: any) {
      log.debug("sendTyping failed", { accountId: params.accountId, error: String(err) });
    }
  }

  // --- QR login is not applicable to Telegram bots. ------------------------

  async loginWithQrStart(_params: { accountId?: string; force?: boolean }): Promise<QrStartResult> {
    throw new Error("QR login is not supported by the telegram adapter");
  }

  async loginWithQrWait(_params: { accountId?: string; sessionKey?: string; timeoutMs?: number }): Promise<QrWaitResult> {
    throw new Error("QR login is not supported by the telegram adapter");
  }

  // --- Internals -----------------------------------------------------------

  private requireHandle(accountId?: string): AccountHandle {
    const id = accountId ?? this.handles.keys().next().value;
    const handle = id ? this.handles.get(id) : undefined;
    if (!handle) {
      throw new Error(`No started telegram account (requested: ${accountId ?? "<none>"})`);
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

  /** POST a Bot API method call; returns the parsed `result` object. */
  private async botApi(
    handle: AccountHandle,
    method: string,
    body: Record<string, unknown>,
    timeoutMs?: number,
  ): Promise<any> {
    const url = `${handle.apiRoot}/bot${handle.token}/${method}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs ?? 60000);
    try {
      const signal = handle.abortController.signal.aborted
        ? handle.abortController.signal
        : controller.signal;
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal,
      });
      const json = (await res.json()) as any;
      if (!json.ok) {
        throw new Error(`Telegram ${method} failed: ${json.error_code} ${json.description ?? JSON.stringify(json)}`);
      }
      return json;
    } finally {
      clearTimeout(timer);
    }
  }

  /** The single getUpdates long-poll loop for an account. */
  private async pollLoop(handle: AccountHandle): Promise<void> {
    const { accountId, abortController } = handle;
    while (!abortController.signal.aborted) {
      try {
        const res = await this.botApi(
          handle,
          "getUpdates",
          {
            offset: handle.lastUpdateId + 1,
            timeout: LONG_POLL_TIMEOUT_SEC,
            allowed_updates: ["message", "edited_message", "channel_post", "callback_query"],
          },
          (LONG_POLL_TIMEOUT_SEC * 1000) + POLL_REQUEST_MARGIN_MS,
        );
        const updates: any[] = res.result ?? [];
        for (const update of updates) {
          if (typeof update.update_id === "number" && update.update_id > handle.lastUpdateId) {
            handle.lastUpdateId = update.update_id;
          }
          this.handleUpdate(handle, update);
        }
      } catch (err: any) {
        if (abortController.signal.aborted) return;
        // AbortError from our own timeout margin is recoverable; just loop.
        log.warn("getUpdates error (reconnecting)", { accountId, error: String(err) });
        this.updateStatus(accountId, "reconnecting", `Poll error: ${err}`);
        await sleep(RECONNECT_DELAY_MS, abortController.signal);
        if (!abortController.signal.aborted) this.updateStatus(accountId, "connected", "Polling");
      }
    }
  }

  /** Convert a raw Telegram update into a NormalizedInboundMessage. */
  private handleUpdate(handle: AccountHandle, update: any): void {
    const msg = update.message ?? update.edited_message ?? update.channel_post;
    if (!msg) return; // callback_query etc. not surfaced as inbound text here.
    const text: string = msg.text ?? msg.caption ?? "";
    if (!text && !msg.media) {
      // Non-text payloads without media are ignored by this minimal adapter.
    }
    const chatId = msg.chat?.id != null ? String(msg.chat.id) : "";
    const senderId = msg.from?.id != null ? String(msg.from.id) : chatId;
    const isPrivate = msg.chat?.type === "private";
    // For private chats the reply target is the chat id; for groups it is the
    // group chat id. Clients echo replyTo as send_text.to.
    const replyTo = chatId;
    const normalized: NormalizedInboundMessage = {
      channel: this.channelId,
      accountId: handle.accountId,
      messageId: String(msg.message_id ?? update.update_id ?? Date.now()),
      chatId,
      senderId,
      replyTo,
      senderName: msg.from?.first_name ?? msg.from?.username ?? msg.chat?.title,
      msgType: msg.text ? "text" : "system",
      text,
      timestamp: (msg.date ? msg.date * 1000 : Date.now()),
      raw: msg,
    };
    log.info("Telegram inbound delivered", { accountId: handle.accountId, chatId, senderId, text });
    this.emitMessage(normalized);
  }
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
