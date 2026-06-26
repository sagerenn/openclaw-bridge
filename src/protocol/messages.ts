/**
 * Bridge protocol message types and envelope definitions.
 * Defines the wire protocol between WS clients and the bridge server.
 */

// ─── Envelope ────────────────────────────────────────────────────────────────

export interface BridgeEnvelope {
  /** Protocol version — currently 1 */
  v: 1;
  /** Correlation ID — echoed in responses */
  id: string;
  /** Message type discriminator */
  type: BridgeMessageType;
  /** Channel this message pertains to (e.g. "liangzimixin", "weixin") */
  channel: string;
  /** Account ID within the channel (optional, defaults to "default") */
  accountId?: string;
  /** Payload — shape depends on `type` */
  payload: unknown;
  /** Server timestamp (ms since epoch) — set by server on inbound */
  ts?: number;
  /** Error info — only present on error responses */
  error?: BridgeError;
}

export interface BridgeError {
  code: string;
  message: string;
}

// ─── Message Types ───────────────────────────────────────────────────────────

export enum BridgeMessageType {
  // Client -> Server
  SEND_TEXT = "send_text",
  SEND_MEDIA = "send_media",
  SEND_TYPING = "send_typing",
  SUBSCRIBE = "subscribe",
  UNSUBSCRIBE = "unsubscribe",
  LIST_CHANNELS = "list_channels",
  PING = "ping",

  // Server -> Client
  INBOUND_MESSAGE = "inbound_message",
  CHANNEL_STATUS = "channel_status",
  CHANNELS_LIST = "channels_list",
  SEND_ACK = "send_ack",
  SEND_ERROR = "send_error",
  PONG = "pong",
  WELCOME = "welcome",
}

// ─── Client -> Server Payloads ───────────────────────────────────────────────

export interface SendTextPayload {
  /** Recipient user ID (channel-native format) */
  to: string;
  /** Text content */
  text: string;
  /** Optional reply-to message ID */
  replyToMessageId?: string;
  /** Weixin: context_token to echo back (if known) */
  contextToken?: string;
}

export interface SendMediaPayload {
  to: string;
  /** URL or local path to media */
  mediaUrl: string;
  /** Optional caption text */
  text?: string;
  /** MIME type hint */
  mediaType?: string;
  contextToken?: string;
}

export interface SendTypingPayload {
  to: string;
  /** true = typing, false = cancel */
  typing: boolean;
}

export interface SubscribePayload {
  /** Channel to subscribe to */
  channel: string;
  /** Account ID (defaults to "default") */
  accountId?: string;
  /** Optional: only receive messages from these senders */
  filter?: { fromUserIds?: string[] };
}

export interface ListChannelsPayload {
  /** If true, include detailed status per account */
  verbose?: boolean;
}

// ─── Server -> Client Payloads ───────────────────────────────────────────────

export interface InboundMessagePayload {
  /** Channel-native message ID */
  messageId: string;
  /** Conversation/chat ID */
  chatId: string;
  /** Sender user ID */
  senderId: string;
  /** Sender display name (if available) */
  senderName?: string;
  /** Message type: "text" | "markdown" | "image" | "file" | "voice" | "video" | "system" */
  msgType: string;
  /** Text content (already decrypted/decoded by the adapter) */
  text: string;
  /** Timestamp (ms since epoch) */
  timestamp: number;
  /** Whether the original message was encrypted */
  wasEncrypted?: boolean;
  /** Reply-to message ID */
  replyToMessageId?: string;
  /** Media URL (if media message, already resolved) */
  mediaUrl?: string;
  mediaType?: string;
  /** Weixin-specific: context_token that must be echoed in replies */
  contextToken?: string;
  /** Raw channel-specific payload for advanced use */
  raw?: unknown;
}

export interface ChannelStatusPayload {
  /** "connected" | "disconnected" | "reconnecting" | "error" */
  status: string;
  /** Human-readable detail */
  detail?: string;
  /** Last error (if status is "error") */
  error?: string;
}

export interface ChannelsListPayload {
  /** Map of channel ID -> channel info */
  channels: Record<string, {
    /** Adapter label / display name */
    label: string;
    /** Accounts for this channel */
    accounts: Record<string, {
      /** "connected" | "disconnected" | "reconnecting" | "error" */
      status: string;
      /** Human-readable detail */
      detail?: string;
      /** Last error (if status is "error") */
      error?: string;
    }>;
  }>;
}

export interface SendAckPayload {
  /** The correlation ID from the original send request */
  requestId: string;
  /** Channel-assigned message ID (if available) */
  messageId?: string;
}

export interface SendErrorPayload {
  requestId: string;
  code: string;
  message: string;
}

export interface WelcomePayload {
  /** Server version */
  version: string;
  /** Available channels and their current status */
  channels: Record<string, { status: string; accounts: string[] }>;
}

// ─── Normalized Inbound Message (internal) ───────────────────────────────────

export interface NormalizedInboundMessage {
  /** Channel that produced this message */
  channel: string;
  /** Account ID within the channel */
  accountId: string;
  /** Channel-native message ID */
  messageId: string;
  /** Conversation/chat ID */
  chatId: string;
  /** Sender user ID */
  senderId: string;
  /** Sender display name */
  senderName?: string;
  /** Message type */
  msgType: string;
  /** Text content (decrypted/decoded) */
  text: string;
  /** Timestamp (ms since epoch) */
  timestamp: number;
  /** Whether the original was encrypted */
  wasEncrypted?: boolean;
  /** Reply-to message ID */
  replyToMessageId?: string;
  /** Media URL */
  mediaUrl?: string;
  mediaType?: string;
  /** Weixin context_token */
  contextToken?: string;
  /** Raw channel-specific payload */
  raw?: unknown;
}

// ─── Channel Status (internal) ───────────────────────────────────────────────

export interface ChannelStatus {
  channel: string;
  accountId: string;
  connected: boolean;
  state: "connected" | "disconnected" | "reconnecting" | "error";
  detail?: string;
  lastError?: string;
  lastInboundAt?: number;
  lastOutboundAt?: number;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

let _correlationCounter = 0;

export function makeCorrelationId(): string {
  return `br-${Date.now().toString(36)}-${(++_correlationCounter).toString(36)}`;
}

export function makeEnvelope(
  type: BridgeMessageType,
  channel: string,
  payload: unknown,
  opts?: { id?: string; accountId?: string; error?: BridgeError }
): BridgeEnvelope {
  return {
    v: 1,
    id: opts?.id ?? makeCorrelationId(),
    type,
    channel,
    accountId: opts?.accountId,
    payload,
    ts: Date.now(),
    error: opts?.error,
  };
}
