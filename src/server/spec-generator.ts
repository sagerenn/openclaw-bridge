/**
 * API specification generators.
 *
 * Builds the AsyncAPI (WebSocket API) and OpenAPI (HTTP API) documents
 * programmatically at request time from the bridge's own protocol definitions,
 * rather than from static files — so the specs never drift from the code.
 *
 * Each generator accepts the live server context (effective host/port/path and
 * configured spec-URL overrides) and returns a plain JS object that the HTTP
 * handler JSON-serializes for the caller.
 */

import type { BridgeConfig } from "../config/schema.js";
import {
  BridgeMessageType,
  BRIDGE_MESSAGE_DIRECTIONS,
  BRIDGE_MESSAGE_TITLES,
  BRIDGE_MESSAGE_PAYLOAD_SCHEMAS,
} from "../protocol/messages.js";
import { HTTP_ROUTES, type HttpRouteParam } from "./http-routes.js";

// ─── Shared metadata ─────────────────────────────────────────────────────────

const BRIDGE_TITLE_WS = "OpenClaw Bridge WebSocket API";
const BRIDGE_TITLE_HTTP = "OpenClaw Bridge HTTP API";
const BRIDGE_VERSION = "1.0.0";

/**
 * Build a base URL for the given scheme given the effective server config.
 * Used for the `servers` block of each spec.
 */
function baseUrl(config: BridgeConfig, scheme: "ws" | "http"): string {
  const host = config.server.host ?? "0.0.0.0";
  const port = config.server.port ?? 9300;
  return `${scheme}://${host}:${port}`;
}

// ─── Payload JSON Schemas (from src/protocol/messages.ts) ────────────────────
// Reused by the AsyncAPI spec; structured to mirror the runtime payload types.

const envelopeSchema = {
  type: "object",
  description: "Wire envelope wrapping every WebSocket message.",
  required: ["v", "id", "type", "channel", "payload"],
  properties: {
    v: { type: "integer", const: 1, description: "Protocol version" },
    id: { type: "string", description: "Correlation id; echoed in responses" },
    type: { type: "string", description: "Message type discriminator" },
    channel: { type: "string", description: "Channel id, or '*' for broadcast/channel-agnostic messages" },
    accountId: { type: "string", description: "Account id within the channel (defaults to 'default')" },
    payload: { description: "Type-specific payload object" },
    ts: { type: "integer", description: "Server timestamp (ms since epoch), set on server messages" },
    error: { $ref: "#/components/schemas/BridgeError" },
  },
};

const schemas = {
  Envelope: envelopeSchema,
  BridgeError: {
    type: "object",
    properties: { code: { type: "string" }, message: { type: "string" } },
  },
  SendTextPayload: {
    type: "object",
    required: ["to", "text"],
    properties: {
      to: { type: "string", description: "Recipient user id (channel-native format)" },
      text: { type: "string" },
      replyToMessageId: { type: "string" },
      contextToken: { type: "string" },
    },
  },
  SendMediaPayload: {
    type: "object",
    required: ["to", "mediaUrl"],
    properties: {
      to: { type: "string" },
      mediaUrl: { type: "string", description: "URL or local path to media" },
      text: { type: "string", description: "Optional caption" },
      mediaType: { type: "string", description: "MIME type hint" },
      contextToken: { type: "string" },
    },
  },
  SendTypingPayload: {
    type: "object",
    required: ["to", "typing"],
    properties: { to: { type: "string" }, typing: { type: "boolean" } },
  },
  SubscribePayload: {
    type: "object",
    required: ["channel"],
    properties: {
      channel: { type: "string" },
      accountId: { type: "string" },
      filter: {
        type: "object",
        properties: { fromUserIds: { type: "array", items: { type: "string" } } },
      },
    },
  },
  ListChannelsPayload: {
    type: "object",
    properties: { verbose: { type: "boolean" } },
  },
  QrStartPayload: {
    type: "object",
    properties: { accountId: { type: "string" }, force: { type: "boolean" } },
  },
  QrWaitPayload: {
    type: "object",
    properties: {
      accountId: { type: "string" },
      sessionKey: { type: "string" },
      timeoutMs: { type: "integer" },
    },
  },
  WelcomePayload: {
    type: "object",
    required: ["version", "channels"],
    properties: {
      version: { type: "string" },
      asyncApiSpecUrl: { type: "string", description: "URL to the AsyncAPI spec, if configured" },
      openApiSpecUrl: { type: "string", description: "URL to the OpenAPI spec, if configured" },
      channels: {
        type: "object",
        additionalProperties: {
          type: "object",
          properties: {
            status: { type: "string" },
            accounts: { type: "array", items: { type: "string" } },
          },
        },
      },
    },
  },
  InboundMessagePayload: {
    type: "object",
    required: ["messageId", "chatId", "senderId", "msgType", "text", "timestamp"],
    properties: {
      messageId: { type: "string" },
      chatId: { type: "string" },
      senderId: { type: "string" },
      senderName: { type: "string" },
      msgType: { type: "string", description: "text|markdown|image|file|voice|video|system" },
      text: { type: "string" },
      timestamp: { type: "integer" },
      wasEncrypted: { type: "boolean" },
      replyToMessageId: { type: "string" },
      mediaUrl: { type: "string" },
      mediaType: { type: "string" },
      contextToken: { type: "string" },
      raw: { description: "Raw channel-specific payload" },
    },
  },
  ChannelStatusPayload: {
    type: "object",
    required: ["status"],
    properties: {
      status: { type: "string", description: "connected|disconnected|reconnecting|error" },
      detail: { type: "string" },
      error: { type: "string" },
    },
  },
  ChannelsListPayload: {
    type: "object",
    required: ["channels"],
    properties: {
      channels: {
        type: "object",
        additionalProperties: {
          type: "object",
          properties: {
            label: { type: "string" },
            accounts: {
              type: "object",
              additionalProperties: {
                type: "object",
                properties: {
                  status: { type: "string" },
                  detail: { type: "string" },
                  error: { type: "string" },
                },
              },
            },
          },
        },
      },
    },
  },
  SendAckPayload: {
    type: "object",
    required: ["requestId"],
    properties: { requestId: { type: "string" }, messageId: { type: "string" } },
  },
  SendErrorPayload: {
    type: "object",
    required: ["requestId", "code", "message"],
    properties: { requestId: { type: "string" }, code: { type: "string" }, message: { type: "string" } },
  },
  QrResultPayload: {
    type: "object",
    required: ["message"],
    properties: {
      connected: { type: "boolean" },
      qrDataUrl: { type: "string" },
      message: { type: "string" },
      sessionKey: { type: "string" },
      accountId: { type: "string" },
    },
  },
} as const;

// ─── AsyncAPI message generation ─────────────────────────────────────────────
//
// Messages are derived directly from the `BridgeMessageType` enum and its
// descriptor maps (BRIDGE_MESSAGE_DIRECTIONS / _TITLES / _PAYLOAD_SCHEMAS) in
// src/protocol/messages.ts — the single source of truth. Adding a new message
// type to the enum (and its descriptors) is the ONLY change needed; the spec
// generated at /spec/asyncapi.json reflects it automatically.

const EMPTY_SCHEMA = { type: "object", properties: {} };

/** Build the AsyncAPI `messages` object by iterating the live enum. */
function buildAsyncMessages(): Record<string, unknown> {
  const messages: Record<string, unknown> = {};
  for (const type of Object.values(BridgeMessageType)) {
    const schemaName = BRIDGE_MESSAGE_PAYLOAD_SCHEMAS[type];
    const direction = BRIDGE_MESSAGE_DIRECTIONS[type];
    const summary =
      direction === "client-to-server"
        ? `Client -> Server. ${BRIDGE_MESSAGE_TITLES[type]}.`
        : `Server -> Client. ${BRIDGE_MESSAGE_TITLES[type]}.`;
    messages[type] = {
      name: type,
      title: BRIDGE_MESSAGE_TITLES[type],
      summary,
      payload: schemaName
        ? { $ref: `#/components/schemas/${schemaName}` }
        : EMPTY_SCHEMA,
    };
  }
  return messages;
}

/**
 * Generate the AsyncAPI 2.6.0 document for the WebSocket API. Built live from
 * the protocol enum so it never goes stale.
 */
export function generateAsyncApi(config: BridgeConfig, opts: {
  asyncApiSpecUrl?: string;
  openApiSpecUrl?: string;
}): Record<string, unknown> {
  const path = config.server.path ?? "/bridge";
  const wsBase = baseUrl(config, "ws");
  const messages = buildAsyncMessages();

  return {
    asyncapi: "2.6.0",
    info: {
      title: BRIDGE_TITLE_WS,
      version: BRIDGE_VERSION,
      description:
        "Bidirectional WebSocket protocol bridging WS clients to backend IM channels via openclaw channel plugins. All messages are JSON envelopes with a correlation id (`id`) echoed in responses. Connect to the configured server path (default `/bridge`).",
    },
    servers: {
      bridge: {
        url: `${wsBase}${path}`,
        protocol: "ws",
        description: "WebSocket endpoint. Replace host/port/path with your bridge config.",
      },
    },
    defaultContentType: "application/json",
    channels: {
      bridge: {
        address: path,
        description:
          "Single bidirectional channel carrying all envelope messages. The `type` field discriminates the message.",
        messages,
      },
    },
    components: {
      schemas: {
        ...schemas,
        Empty: { type: "object", properties: {} },
      },
      messages,
    },
    "x-spec-urls": {
      asyncApiSpecUrl: opts.asyncApiSpecUrl ?? null,
      openApiSpecUrl: opts.openApiSpecUrl ?? null,
    },
  };
}

// ─── OpenAPI generator ───────────────────────────────────────────────────────

/**
 * Generate the OpenAPI 3.1.0 document for the HTTP API. Built live from the
 * bridge's HTTP route set so it stays in sync with the code.
 */
export function generateOpenApi(config: BridgeConfig, opts: {
  asyncApiSpecUrl?: string;
  openApiSpecUrl?: string;
}): Record<string, unknown> {
  const httpBase = baseUrl(config, "http");

  const toParam = (p: HttpRouteParam) => ({
    name: p.name,
    in: p.in,
    required: p.required,
    description: p.description,
    schema: p.schema,
  });

  // Build the `paths` object entirely from HTTP_ROUTES — adding a route to the
  // table is the only change needed; it becomes live and appears here.
  const paths: Record<string, unknown> = {};
  const paramsByName: Record<string, unknown> = {};
  for (const route of HTTP_ROUTES) {
    paths[route.path] = {
      [route.method.toLowerCase()]: {
        tags: route.tags,
        summary: route.summary,
        description: route.description,
        operationId: route.operationId,
        parameters: route.params.map(toParam),
        responses: route.responses,
      },
    };
    for (const p of route.params) {
      if (p.in === "path" && !(p.name in paramsByName)) {
        paramsByName[p.name] = toParam(p);
      }
    }
  }

  return {
    openapi: "3.1.0",
    info: {
      title: BRIDGE_TITLE_HTTP,
      version: BRIDGE_VERSION,
      description:
        "HTTP endpoints exposed by the OpenClaw Bridge alongside its WebSocket server. Covers QR-code login flows for plugins that support it (e.g. WeChat) and machine-readable API spec discovery. Generated live from the bridge's route table.",
    },
    servers: [
      {
        url: httpBase,
        description: "HTTP base. Replace host/port with your bridge config.",
      },
    ],
    "x-spec-urls": {
      asyncApiSpecUrl: opts.asyncApiSpecUrl ?? null,
      openApiSpecUrl: opts.openApiSpecUrl ?? null,
    },
    paths,
    components: {
      parameters: paramsByName,
      responses: {
        Error: {
          description: "Error response",
          content: { "application/json": { schema: { $ref: "#/components/schemas/Error" } } },
        },
      },
      schemas: {
        Error: {
          type: "object",
          properties: { error: { type: "string" }, message: { type: "string" } },
        },
        QrStartResponse: {
          type: "object",
          required: ["message"],
          properties: {
            qrDataUrl: { type: "string", description: "PNG data URL of the QR code" },
            message: { type: "string" },
            sessionKey: { type: "string", description: "Pass to /qr/status" },
          },
        },
        QrWaitResponse: {
          type: "object",
          required: ["message"],
          properties: {
            connected: { type: "boolean", description: "Whether the login completed" },
            message: { type: "string" },
            accountId: { type: "string", description: "Account id assigned by the plugin on success" },
            qrDataUrl: { type: "string", description: "Refreshed QR code, if the server refreshed it" },
          },
        },
      },
    },
  };
}



