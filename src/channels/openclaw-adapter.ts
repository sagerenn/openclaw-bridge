/**
 * OpenClawChannelAdapter — a ChannelAdapter implementation that drives
 * ANY openclaw channel plugin through its standard ChannelPlugin interface.
 *
 * Instead of plugin-specific transport strategies, this adapter:
 * 1. Loads the plugin's ChannelPlugin object (via register() or direct export)
 * 2. Calls gateway.startAccount(ctx) to start backend connections
 * 3. Calls gateway.stopAccount(ctx) to stop them
 * 4. Calls outbound.sendText() / outbound.sendMedia() for outbound messages
 * 5. Intercepts the deliver() callback in the reply pipeline to capture
 *    inbound messages and route them to WS clients
 *
 * This is fully generic — no plugin-specific code required.
 */

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
import type { NormalizedInboundMessage, ChannelStatus } from "../protocol/messages.js";
import {
  buildGatewayContext,
  buildPluginApi,
  type DeliverInterceptor,
} from "./runtime-shim.js";
import { toQrImageDataUrl } from "./qr-util.js";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("openclaw-adapter");

// ─── Account Handle ──────────────────────────────────────────────────────────

interface AccountHandle {
  accountId: string;
  abortController: AbortController;
  connected: boolean;
}

// ─── Adapter ─────────────────────────────────────────────────────────────────

export class OpenClawChannelAdapter implements ChannelAdapter {
  readonly channelId: string;
  readonly label: string;

  private plugin: Record<string, any>;
  private handles = new Map<string, AccountHandle>();
  private statuses = new Map<string, ChannelStatus>();
  private messageCallbacks: InboundMessageCallback[] = [];
  private statusCallbacks: StatusChangeCallback[] = [];

  constructor(plugin: Record<string, any>) {
    this.plugin = plugin;
    this.channelId = plugin.id ?? "unknown";
    this.label = plugin.meta?.label ?? plugin.meta?.selectionLabel ?? this.channelId;
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

  getStatus(accountId: string): ChannelStatus {
    return this.statuses.get(accountId) ?? {
      channel: this.channelId,
      accountId,
      connected: false,
      state: "disconnected",
    };
  }

  async start(accountId: string, credentials: Record<string, unknown>): Promise<void> {
    if (this.handles.has(accountId)) {
      log.warn("Account already started", { channelId: this.channelId, accountId });
      return;
    }

    this.updateStatus(accountId, "reconnecting", "Starting...");

    const abortController = new AbortController();

    // Build the deliver interceptor — this captures inbound messages
    // from the plugin's reply pipeline and routes them to WS clients
    const onDeliver: DeliverInterceptor = (payload) => {
      this.emitDeliverAsInbound(accountId, payload);
    };

    // Build the gateway context with our runtime shim
    const ctx = buildGatewayContext(
      this.channelId,
      accountId,
      credentials,
      abortController.signal,
      onDeliver,
    );

    const handle: AccountHandle = {
      accountId,
      abortController,
      connected: false,
    };

    this.handles.set(accountId, handle);

    // Call the plugin's gateway.startAccount(ctx)
    // NOTE: Some plugins (e.g., liangzimixin) block startAccount() until
    // the abort signal fires. We fire-and-forget the gateway start and
    // mark the account as connected once the plugin logs readiness.
    const gateway = this.plugin.gateway;
    if (gateway?.startAccount) {
      log.info("Calling gateway.startAccount()", { channelId: this.channelId, accountId });

      // Fire-and-forget: startAccount blocks until abort for long-running gateways
      gateway.startAccount(ctx).then(() => {
        handle.connected = true;
        this.updateStatus(accountId, "connected", "Plugin gateway started");
        log.info("Account started via plugin gateway", { channelId: this.channelId, accountId });
      }).catch((err: any) => {
        if (abortController.signal.aborted) {
          // Normal shutdown — don't treat as error
          this.updateStatus(accountId, "disconnected", "Stopped");
        } else {
          this.updateStatus(accountId, "error", `Start failed: ${err}`, String(err));
          log.error("gateway.startAccount() failed", { channelId: this.channelId, accountId, error: String(err) });
        }
      });

      // Mark as connected optimistically — the plugin's gateway has been invoked
      // and will establish the backend connection. The status will be updated
      // when the promise resolves or the plugin signals readiness.
      handle.connected = true;
      this.updateStatus(accountId, "connected", "Plugin gateway started");
    } else {
      log.warn("Plugin has no gateway.startAccount() — cannot start backend connection", {
        channelId: this.channelId,
        accountId,
      });
      this.updateStatus(accountId, "error", "No gateway.startAccount()");
    }
  }

  async stop(accountId: string): Promise<void> {
    const handle = this.handles.get(accountId);
    if (!handle) return;

    // Abort the signal first — this tells the plugin's gateway to stop
    handle.abortController.abort();

    try {
      // Call the plugin's gateway.stopAccount(ctx)
      const gateway = this.plugin.gateway;
      if (gateway?.stopAccount) {
        await gateway.stopAccount({
          accountId,
          cfg: {},
          account: { accountId },
          abortSignal: handle.abortController.signal,
        });
      }
    } catch (err) {
      log.warn("Error stopping plugin gateway", { channelId: this.channelId, accountId, error: String(err) });
    }

    handle.connected = false;
    this.handles.delete(accountId);
    this.updateStatus(accountId, "disconnected", "Stopped");
  }

  async stopAll(): Promise<void> {
    for (const accountId of this.handles.keys()) {
      await this.stop(accountId);
    }
  }

  async sendText(params: SendTextParams): Promise<SendResult> {
    const accountId = params.accountId ?? "default";
    const handle = this.handles.get(accountId);
    if (!handle || !handle.connected) {
      throw new Error(`${this.channelId} account not started: ${accountId}`);
    }

    const outbound = this.plugin.outbound;
    if (!outbound?.sendText) {
      throw new Error(`Plugin ${this.channelId} does not support outbound.sendText()`);
    }

    try {
      const result = await outbound.sendText({
        cfg: {},
        to: params.to,
        text: params.text,
        accountId,
        replyToMessageId: params.replyToMessageId,
        contextToken: params.contextToken,
      });

      this.statuses.set(`${this.channelId}:${accountId}`, {
        ...this.getStatus(accountId),
        lastOutboundAt: Date.now(),
      });

      return {
        messageId: result?.messageId ?? result?.id ?? "",
        chatId: result?.chatId,
      };
    } catch (err) {
      log.error("sendText failed", { channelId: this.channelId, accountId, error: String(err) });
      throw err;
    }
  }

  async sendMedia(params: SendMediaParams): Promise<SendResult> {
    const accountId = params.accountId ?? "default";
    const handle = this.handles.get(accountId);
    if (!handle || !handle.connected) {
      throw new Error(`${this.channelId} account not started: ${accountId}`);
    }

    const outbound = this.plugin.outbound;
    if (!outbound?.sendMedia) {
      throw new Error(`Plugin ${this.channelId} does not support outbound.sendMedia()`);
    }

    try {
      const result = await outbound.sendMedia({
        cfg: {},
        to: params.to,
        mediaUrl: params.mediaUrl,
        text: params.text,
        mediaType: params.mediaType,
        accountId,
        contextToken: params.contextToken,
      });

      this.statuses.set(`${this.channelId}:${accountId}`, {
        ...this.getStatus(accountId),
        lastOutboundAt: Date.now(),
      });

      return {
        messageId: result?.messageId ?? result?.id ?? "",
        chatId: result?.chatId,
      };
    } catch (err) {
      log.error("sendMedia failed", { channelId: this.channelId, accountId, error: String(err) });
      throw err;
    }
  }

  async sendTyping(params: SendTypingParams): Promise<void> {
    const accountId = params.accountId ?? "default";
    const handle = this.handles.get(accountId);
    if (!handle || !handle.connected) return;

    // Typing is optional — not all plugins support it
    const outbound = this.plugin.outbound;
    if (outbound?.sendTyping) {
      try {
        await outbound.sendTyping({
          cfg: {},
          to: params.to,
          typing: params.typing,
          accountId,
        });
      } catch (err) {
        log.debug("sendTyping failed (non-critical)", { channelId: this.channelId, accountId, error: String(err) });
      }
    }
  }

  async loginWithQrStart(params: { accountId?: string; force?: boolean }): Promise<QrStartResult> {
    const gateway = this.plugin.gateway;
    if (!gateway?.loginWithQrStart) {
      throw new Error(`Plugin ${this.channelId} does not support QR login (no gateway.loginWithQrStart)`);
    }

    log.info("Calling gateway.loginWithQrStart()", { channelId: this.channelId, accountId: params.accountId });

    try {
      const result = await gateway.loginWithQrStart({
        accountId: params.accountId,
        force: params.force,
      });

      // Plugins may return the QR *value* (the URL/text to scan) rather than an
      // image data URL (e.g. weixin's qrcodeUrl is a plain https login URL).
      // Normalize it into a PNG data URL so the browser can render it directly.
      const qrDataUrl = await toQrImageDataUrl(result.qrDataUrl ?? result.qrcodeUrl);

      return {
        qrDataUrl,
        message: result.message ?? "QR code generated",
        sessionKey: result.sessionKey,
      };
    } catch (err) {
      log.error("loginWithQrStart failed", { channelId: this.channelId, error: String(err) });
      throw err;
    }
  }

  async loginWithQrWait(params: { accountId?: string; sessionKey?: string; timeoutMs?: number }): Promise<QrWaitResult> {
    const gateway = this.plugin.gateway;
    if (!gateway?.loginWithQrWait) {
      throw new Error(`Plugin ${this.channelId} does not support QR login wait (no gateway.loginWithQrWait)`);
    }

    log.info("Calling gateway.loginWithQrWait()", {
      channelId: this.channelId,
      accountId: params.accountId,
      hasSessionKey: !!params.sessionKey,
    });

    try {
      const result = await gateway.loginWithQrWait({
        accountId: params.accountId,
        timeoutMs: params.timeoutMs,
        sessionKey: params.sessionKey,
      });

      if (result.connected) {
        log.info("QR login succeeded", {
          channelId: this.channelId,
          accountId: result.accountId ?? params.accountId,
        });
      }

      // A refreshed QR may come back as a plain URL/text — normalize it too.
      const qrDataUrl = await toQrImageDataUrl(result.qrDataUrl ?? result.qrcodeUrl);

      return {
        connected: result.connected ?? false,
        message: result.message ?? "",
        accountId: result.accountId,
        qrDataUrl,
      };
    } catch (err) {
      log.error("loginWithQrWait failed", { channelId: this.channelId, error: String(err) });
      throw err;
    }
  }

  /**
   * Emit a normalized inbound message to all registered callbacks.
   * Called when the plugin's deliver() callback fires.
   */
  private emitDeliverAsInbound(accountId: string, payload: {
    text?: string;
    mediaUrl?: string;
    mediaUrls?: string[];
    inboundContext?: unknown;
  }): void {
    const ctx = payload.inboundContext as any;

    const normalized: NormalizedInboundMessage = {
      channel: this.channelId,
      accountId,
      messageId: ctx?.messageId ?? ctx?.id ?? `deliver-${Date.now()}`,
      chatId: ctx?.chatId ?? ctx?.peer ?? ctx?.from ?? "",
      senderId: ctx?.senderId ?? ctx?.from ?? "",
      senderName: ctx?.senderName ?? ctx?.fromName,
      msgType: payload.mediaUrl || payload.mediaUrls?.length ? "media" : "text",
      text: payload.text ?? "",
      timestamp: ctx?.timestamp ?? Date.now(),
      mediaUrl: payload.mediaUrl ?? payload.mediaUrls?.[0],
      mediaType: payload.mediaUrl ? "image" : undefined,
      raw: ctx,
    };

    this.statuses.set(`${this.channelId}:${accountId}`, {
      ...this.getStatus(accountId),
      lastInboundAt: Date.now(),
    });

    for (const cb of this.messageCallbacks) cb(normalized);
  }

  /**
   * Emit a normalized inbound message from raw plugin data.
   * Used when the plugin pushes messages through its own mechanism
   * (e.g., liangzimixin's messagePipe) rather than the reply pipeline.
   */
  emitMessage(msg: NormalizedInboundMessage): void {
    this.statuses.set(`${this.channelId}:${msg.accountId}`, {
      ...this.getStatus(msg.accountId),
      lastInboundAt: Date.now(),
    });
    for (const cb of this.messageCallbacks) cb(msg);
  }

  private updateStatus(accountId: string, state: ChannelStatus["state"], detail?: string, lastError?: string): void {
    const status: ChannelStatus = {
      channel: this.channelId,
      accountId,
      connected: state === "connected",
      state,
      detail,
      lastError,
    };
    this.statuses.set(`${this.channelId}:${accountId}`, status);
    for (const cb of this.statusCallbacks) cb(status);
  }
}
