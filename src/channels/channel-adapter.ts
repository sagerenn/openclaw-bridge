/**
 * ChannelAdapter interface — the contract between the bridge server
 * and any backend channel. Adapters wrap an openclaw ChannelPlugin
 * and drive it through the standard plugin interface (gateway, outbound).
 *
 * No plugin-specific code lives here — the adapter is fully generic
 * and works with ANY openclaw channel plugin.
 */

import type {
  NormalizedInboundMessage,
  ChannelStatus,
} from "../protocol/messages.js";

// ─── Adapter Interface ───────────────────────────────────────────────────────

export interface ChannelAdapter {
  /** Unique channel identifier (e.g. "liangzimixin", "openclaw-weixin") */
  readonly channelId: string;

  /** Human-readable label */
  readonly label: string;

  /**
   * Initialize the adapter: validate config, acquire tokens, establish
   * backend connections. Called once per account at server startup.
   */
  start(accountId: string, credentials: Record<string, unknown>): Promise<void>;

  /**
   * Gracefully shut down an account: close connections, cancel timers, flush state.
   */
  stop(accountId: string): Promise<void>;

  /**
   * Shut down the entire adapter (all accounts).
   */
  stopAll(): Promise<void>;

  /**
   * Send a text message to a recipient on this channel.
   */
  sendText(params: SendTextParams): Promise<SendResult>;

  /**
   * Send a media message to a recipient on this channel.
   */
  sendMedia(params: SendMediaParams): Promise<SendResult>;

  /**
   * Send a typing indicator.
   */
  sendTyping(params: SendTypingParams): Promise<void>;

  /**
   * Register a callback for inbound messages.
   */
  onMessage(callback: InboundMessageCallback): void;

  /**
   * Register a callback for connection status changes.
   */
  onStatusChange(callback: StatusChangeCallback): void;

  /**
   * Current connection status for an account.
   */
  getStatus(accountId: string): ChannelStatus;

  /**
   * List configured account IDs for this channel.
   */
  listAccounts(): string[];

  /**
   * List account IDs the plugin itself knows about — e.g. accounts whose
   * credentials were persisted by a QR-login flow (openclaw-weixin stores
   * these in its own state dir, not in the bridge config). Used to auto-resume
   * such accounts on boot. Returns [] when the plugin exposes no discovery.
   */
  listSavedAccountIds(): string[];

  /**
   * Start a QR code login flow for an account.
   * Returns a QR code image and session key for polling.
   * Only supported by plugins that implement gateway.loginWithQrStart().
   */
  loginWithQrStart(params: { accountId?: string; force?: boolean }): Promise<QrStartResult>;

  /**
   * Wait for a QR code login to complete (polling).
   * Pass the sessionKey from loginWithQrStart().
   * Only supported by plugins that implement gateway.loginWithQrWait().
   */
  loginWithQrWait(params: { accountId?: string; sessionKey?: string; timeoutMs?: number }): Promise<QrWaitResult>;
}

// ─── Parameter Types ─────────────────────────────────────────────────────────

export interface SendTextParams {
  to: string;
  text: string;
  accountId?: string;
  replyToMessageId?: string;
  contextToken?: string;
}

export interface SendMediaParams {
  to: string;
  mediaUrl: string;
  text?: string;
  mediaType?: string;
  accountId?: string;
  contextToken?: string;
}

export interface SendTypingParams {
  to: string;
  typing: boolean;
  accountId?: string;
  contextToken?: string;
}

export interface SendResult {
  messageId: string;
  chatId?: string;
}

// ─── QR Login Types ──────────────────────────────────────────────────────────

export interface QrStartResult {
  /** Data URL of the QR code image (e.g. data:image/png;base64,...) */
  qrDataUrl?: string;
  /** Human-readable status message */
  message: string;
  /** Session key to pass back to loginWithQrWait (plugin-specific) */
  sessionKey?: string;
}

export interface QrWaitResult {
  /** Whether the login completed successfully */
  connected: boolean;
  /** Human-readable status message */
  message: string;
  /** The account ID assigned by the plugin (may differ from requested ID) */
  accountId?: string;
  /** Updated QR data URL if the QR was refreshed during waiting */
  qrDataUrl?: string;
}

// ─── Callback Types ──────────────────────────────────────────────────────────

export type InboundMessageCallback = (msg: NormalizedInboundMessage) => void;
export type StatusChangeCallback = (status: ChannelStatus) => void;
