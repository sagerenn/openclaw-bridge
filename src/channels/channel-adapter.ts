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

// ─── Callback Types ──────────────────────────────────────────────────────────

export type InboundMessageCallback = (msg: NormalizedInboundMessage) => void;
export type StatusChangeCallback = (status: ChannelStatus) => void;
