/**
 * BridgeServer — the core WebSocket server that bridges clients to channel backends.
 * Also exposes HTTP API routes for plugin operations (e.g. QR code login).
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { WebSocketServer, WebSocket } from "ws";
import type { ChannelManager } from "../channels/channel-manager.js";
import type { ClientRegistry } from "./client-registry.js";
import { ClientConnection } from "./client-connection.js";
import type { BridgeConfig } from "../config/schema.js";
import {
  BridgeMessageType,
  makeEnvelope,
  makeCorrelationId,
  type BridgeEnvelope,
  type SendTextPayload,
  type SendMediaPayload,
  type SendTypingPayload,
  type SubscribePayload,
  type InboundMessagePayload,
  type ChannelStatusPayload,
  type ChannelsListPayload,
  type SendAckPayload,
  type SendErrorPayload,
  type WelcomePayload,
  type NormalizedInboundMessage,
  type QrStartPayload,
  type QrWaitPayload,
  type QrResultPayload,
} from "../protocol/messages.js";
import { rootLogger } from "../util/logger.js";
import { matchHttpRoute } from "./http-routes.js";
import { generateAsyncApi, generateOpenApi } from "./spec-generator.js";

const log = rootLogger.child("server");

// ─── API spec definitions ────────────────────────────────────────────────────
// Machine-readable specs for the WebSocket API (AsyncAPI) and HTTP API (OpenAPI),
// generated on demand from the protocol definitions and served from /spec/* so
// clients can discover the protocol programmatically.
const ASYNC_API_PATH = "/spec/asyncapi.json";
const OPEN_API_PATH = "/spec/openapi.json";

/** Resolve the advertised URL for a spec, honoring any explicit config override. */
function resolveSpecUrl(override: string | undefined, defaultPath: string): string | undefined {
  return override ?? defaultPath;
}

export class BridgeServer {
  private httpServer: Server;
  private wss: WebSocketServer;
  private channelManager: ChannelManager;
  private clientRegistry: ClientRegistry;
  private config: BridgeConfig;
  private heartbeatInterval?: ReturnType<typeof setInterval>;

  constructor(
    config: BridgeConfig,
    channelManager: ChannelManager,
    clientRegistry: ClientRegistry
  ) {
    this.config = config;
    this.channelManager = channelManager;
    this.clientRegistry = clientRegistry;

    this.httpServer = createServer();
    this.wss = new WebSocketServer({
      server: this.httpServer,
      path: config.server.path ?? "/bridge",
      maxPayload: config.server.maxMessageSize ?? 10 * 1024 * 1024,
    });
  }

  /** Start the WS server and begin accepting connections */
  async start(): Promise<void> {
    // Wire channel events to client broadcasting
    this.channelManager.onMessage((msg) => this.routeInboundMessage(msg));
    this.channelManager.onStatusChange((status) => this.routeStatusChange(status));

    // Handle new WS connections
    this.wss.on("connection", (ws, req) => this.handleConnection(ws, req));

    // Handle HTTP API requests (for QR code and other plugin operations)
    this.httpServer.on("request", (req, res) => this.handleHttpRequest(req, res));

    // Start listening
    const host = this.config.server.host ?? "0.0.0.0";
    const port = this.config.server.port ?? 9300;

    await new Promise<void>((resolve, reject) => {
      this.httpServer.once("error", reject);
      this.httpServer.listen(port, host, () => {
        this.httpServer.removeListener("error", reject);
        resolve();
      });
    });

    // Start client heartbeat check
    const heartbeatMs = this.config.server.clientHeartbeatMs ?? 30000;
    this.heartbeatInterval = setInterval(() => this.checkClientHeartbeats(), heartbeatMs);

    log.info("Bridge server listening", { host, port, path: this.config.server.path ?? "/bridge" });
  }

  /** Gracefully shut down */
  async stop(): Promise<void> {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = undefined;
    }

    // Close all client connections
    for (const client of this.clientRegistry.getAll()) {
      client.close(1001, "server shutdown");
    }

    // Stop all channel adapters
    await this.channelManager.stopAll();

    // Close the WS server
    await new Promise<void>((resolve) => {
      this.wss.close(() => resolve());
    });

    // Close the HTTP server
    await new Promise<void>((resolve) => {
      this.httpServer.close(() => resolve());
    });

    log.info("Bridge server stopped");
  }

  /** Handle a new client WS connection */
  private handleConnection(ws: WebSocket, req: IncomingMessage): void {
    const maxClients = this.config.server.maxClients ?? 100;
    if (this.clientRegistry.size >= maxClients) {
      ws.close(1013, "max clients reached");
      return;
    }

    const client = new ClientConnection(ws);
    this.clientRegistry.register(client);

    log.info("Client connected", { clientId: client.id, ip: req.socket.remoteAddress });

    // Send welcome message
    const welcomePayload = this.buildWelcomePayload();
    client.send(makeEnvelope(BridgeMessageType.WELCOME, "*", welcomePayload));

    // Handle incoming messages
    ws.on("message", (data) => {
      try {
        const envelope: BridgeEnvelope = JSON.parse(data.toString());
        this.handleClientMessage(client, envelope);
      } catch (err) {
        log.warn("Invalid message from client", { clientId: client.id, error: String(err) });
      }
    });

    // Handle disconnect
    ws.on("close", (code, reason) => {
      log.info("Client disconnected", { clientId: client.id, code, reason: reason.toString() });
      this.clientRegistry.unregister(client.id);
    });

    ws.on("error", (err) => {
      log.warn("Client error", { clientId: client.id, error: String(err) });
      this.clientRegistry.unregister(client.id);
    });

    // Set up pong tracking for heartbeat
    ws.on("pong", () => {
      (ws as any)._lastPong = Date.now();
    });
  }

  /** Handle a message from a client */
  private handleClientMessage(client: ClientConnection, envelope: BridgeEnvelope): void {
    switch (envelope.type) {
      case BridgeMessageType.SEND_TEXT:
        this.handleSendText(client, envelope);
        break;
      case BridgeMessageType.SEND_MEDIA:
        this.handleSendMedia(client, envelope);
        break;
      case BridgeMessageType.SEND_TYPING:
        this.handleSendTyping(client, envelope);
        break;
      case BridgeMessageType.SUBSCRIBE:
        this.handleSubscribe(client, envelope);
        break;
      case BridgeMessageType.UNSUBSCRIBE:
        this.handleUnsubscribe(client, envelope);
        break;
      case BridgeMessageType.LIST_CHANNELS:
        this.handleListChannels(client, envelope);
        break;
      case BridgeMessageType.PING:
        client.send(makeEnvelope(BridgeMessageType.PONG, "*", {}, { id: envelope.id }));
        break;
      case BridgeMessageType.QR_START:
        this.handleQrStart(client, envelope);
        break;
      case BridgeMessageType.QR_WAIT:
        this.handleQrWait(client, envelope);
        break;
      default:
        log.warn("Unknown message type from client", { clientId: client.id, type: envelope.type });
    }
  }

  /** Handle SEND_TEXT from a client */
  private async handleSendText(client: ClientConnection, envelope: BridgeEnvelope): Promise<void> {
    const payload = envelope.payload as SendTextPayload;
    const channelId = envelope.channel;
    const accountId = envelope.accountId ?? "default";

    const adapter = this.channelManager.getAdapter(channelId);
    if (!adapter) {
      this.sendError(client, envelope.id, channelId, "unknown_channel", `No adapter for channel: ${channelId}`);
      return;
    }

    try {
      const result = await adapter.sendText({
        to: payload.to,
        text: payload.text,
        accountId,
        replyToMessageId: payload.replyToMessageId,
        contextToken: payload.contextToken,
      });

      const ack: SendAckPayload = { requestId: envelope.id, messageId: result.messageId };
      client.send(makeEnvelope(BridgeMessageType.SEND_ACK, channelId, ack, { accountId }));
    } catch (err) {
      this.sendError(client, envelope.id, channelId, "send_failed", String(err));
    }
  }

  /** Handle SEND_MEDIA from a client */
  private async handleSendMedia(client: ClientConnection, envelope: BridgeEnvelope): Promise<void> {
    const payload = envelope.payload as SendMediaPayload;
    const channelId = envelope.channel;
    const accountId = envelope.accountId ?? "default";

    const adapter = this.channelManager.getAdapter(channelId);
    if (!adapter) {
      this.sendError(client, envelope.id, channelId, "unknown_channel", `No adapter for channel: ${channelId}`);
      return;
    }

    try {
      const result = await adapter.sendMedia({
        to: payload.to,
        mediaUrl: payload.mediaUrl,
        text: payload.text,
        mediaType: payload.mediaType,
        accountId,
        contextToken: payload.contextToken,
      });

      const ack: SendAckPayload = { requestId: envelope.id, messageId: result.messageId };
      client.send(makeEnvelope(BridgeMessageType.SEND_ACK, channelId, ack, { accountId }));
    } catch (err) {
      this.sendError(client, envelope.id, channelId, "send_failed", String(err));
    }
  }

  /** Handle SEND_TYPING from a client */
  private async handleSendTyping(client: ClientConnection, envelope: BridgeEnvelope): Promise<void> {
    const payload = envelope.payload as SendTypingPayload;
    const channelId = envelope.channel;
    const accountId = envelope.accountId ?? "default";

    const adapter = this.channelManager.getAdapter(channelId);
    if (!adapter) return;

    try {
      await adapter.sendTyping({
        to: payload.to,
        typing: payload.typing,
        accountId,
      });
    } catch (err) {
      log.warn("Send typing failed", { channelId, accountId, error: String(err) });
    }
  }

  /** Handle SUBSCRIBE from a client */
  private handleSubscribe(client: ClientConnection, envelope: BridgeEnvelope): void {
    const payload = envelope.payload as SubscribePayload;
    const channel = payload.channel ?? envelope.channel;
    const accountId = payload.accountId ?? envelope.accountId ?? "default";
    const key = `${channel}:${accountId}`;

    client.addSubscription(key, payload.filter);

    // Send current channel status
    const status = this.channelManager.getStatus(channel, accountId);
    const statusPayload: ChannelStatusPayload = {
      status: status.state,
      detail: status.detail,
      error: status.lastError,
    };
    client.send(makeEnvelope(BridgeMessageType.CHANNEL_STATUS, channel, statusPayload, { accountId }));

    log.info("Client subscribed", { clientId: client.id, channel, accountId });
  }

  /** Handle UNSUBSCRIBE from a client */
  private handleUnsubscribe(client: ClientConnection, envelope: BridgeEnvelope): void {
    const payload = envelope.payload as SubscribePayload;
    const channel = payload.channel ?? envelope.channel;
    const accountId = payload.accountId ?? envelope.accountId ?? "default";
    const key = `${channel}:${accountId}`;

    client.removeSubscription(key);
    log.info("Client unsubscribed", { clientId: client.id, channel, accountId });
  }

  /** Handle LIST_CHANNELS from a client — returns all channels, accounts, and their status */
  private handleListChannels(client: ClientConnection, envelope: BridgeEnvelope): void {
    const channels: ChannelsListPayload["channels"] = {};

    for (const adapter of this.channelManager.getAllAdapters()) {
      const accounts = adapter.listAccounts();
      const accountStatuses: ChannelsListPayload["channels"][string]["accounts"] = {};

      for (const accId of accounts) {
        const status = this.channelManager.getStatus(adapter.channelId, accId);
        accountStatuses[accId] = {
          status: status.state,
          detail: status.detail,
          error: status.lastError,
        };
      }

      // Also include accounts from config that haven't been started yet
      const channelConfig = (this.config as any).channels?.[adapter.channelId];
      if (channelConfig?.accounts) {
        for (const accId of Object.keys(channelConfig.accounts)) {
          if (!accountStatuses[accId]) {
            const status = this.channelManager.getStatus(adapter.channelId, accId);
            accountStatuses[accId] = {
              status: status.state,
              detail: status.detail,
              error: status.lastError,
            };
          }
        }
      }

      channels[adapter.channelId] = {
        label: adapter.label,
        accounts: accountStatuses,
      };
    }

    const payload: ChannelsListPayload = { channels };
    client.send(makeEnvelope(BridgeMessageType.CHANNELS_LIST, "*", payload, { id: envelope.id }));
  }

  /** Handle QR_START from a WS client */
  private async handleQrStart(client: ClientConnection, envelope: BridgeEnvelope): Promise<void> {
    const payload = envelope.payload as QrStartPayload;
    const channelId = envelope.channel;
    const accountId = payload.accountId ?? envelope.accountId;

    try {
      const result = await this.channelManager.loginWithQrStart(channelId, {
        accountId,
        force: payload.force,
      });

      const qrPayload: QrResultPayload = {
        qrDataUrl: result.qrDataUrl,
        message: result.message,
        sessionKey: result.sessionKey,
      };
      client.send(makeEnvelope(BridgeMessageType.QR_RESULT, channelId, qrPayload, { accountId }));
    } catch (err) {
      const qrPayload: QrResultPayload = {
        message: `QR start failed: ${String(err)}`,
      };
      client.send(makeEnvelope(BridgeMessageType.QR_RESULT, channelId, qrPayload, { accountId }));
    }
  }

  /** Handle QR_WAIT from a WS client */
  private async handleQrWait(client: ClientConnection, envelope: BridgeEnvelope): Promise<void> {
    const payload = envelope.payload as QrWaitPayload;
    const channelId = envelope.channel;
    const accountId = payload.accountId ?? envelope.accountId;

    try {
      const result = await this.channelManager.loginWithQrWait(channelId, {
        accountId,
        sessionKey: payload.sessionKey,
        timeoutMs: payload.timeoutMs,
      });

      const qrPayload: QrResultPayload = {
        connected: result.connected,
        message: result.message,
        accountId: result.accountId,
        qrDataUrl: result.qrDataUrl,
      };
      client.send(makeEnvelope(BridgeMessageType.QR_RESULT, channelId, qrPayload, { accountId }));
    } catch (err) {
      const qrPayload: QrResultPayload = {
        message: `QR wait failed: ${String(err)}`,
      };
      client.send(makeEnvelope(BridgeMessageType.QR_RESULT, channelId, qrPayload, { accountId }));
    }
  }

  // ─── HTTP API ────────────────────────────────────────────────────────────────

  /**
   * Handle HTTP API requests. Routes are defined in `http-routes.ts` (the
   * single source of truth shared with the OpenAPI spec generator), so adding
   * a route there automatically makes it live AND documented at
   * /spec/openapi.json — no separate spec update needed.
   */
  private handleHttpRequest(req: IncomingMessage, res: ServerResponse): void {
    const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
    const pathname = url.pathname;

    const matched = matchHttpRoute(req.method ?? "GET", pathname);
    if (!matched) {
      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Not found" }));
      return;
    }

    const { route, pathParams } = matched;
    const channelId = pathParams.channelId;
    const accountId = pathParams.accountId;

    switch (route.handler) {
      case "qr-html":
        this.handleQrHttpRequest(channelId, accountId, url, req, res);
        break;
      case "qr-json":
        this.handleQrJsonHttpRequest(channelId, accountId, url, req, res);
        break;
      case "qr-status":
        this.handleQrStatusHttpRequest(channelId, accountId, url, req, res);
        break;
      case "spec-asyncapi":
        this.serveAsyncApiSpec(res);
        break;
      case "spec-openapi":
        this.serveOpenApiSpec(res);
        break;
      default:
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: `Unhandled route: ${route.path}` }));
    }
  }

  /**
   * GET /spec/asyncapi.json — the WebSocket API spec, generated live from the
   * protocol message enum so it always reflects the current API.
   */
  private serveAsyncApiSpec(res: ServerResponse): void {
    try {
      const spec = generateAsyncApi(this.config, {
        asyncApiSpecUrl: this.resolveAsyncApiSpecUrl(),
        openApiSpecUrl: this.resolveOpenApiSpecUrl(),
      });
      res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
      res.end(JSON.stringify(spec, null, 2));
    } catch (err) {
      log.error("Failed to generate AsyncAPI spec", { error: String(err) });
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(err) }));
    }
  }

  /**
   * GET /spec/openapi.json — the HTTP API spec, generated live from the route
   * table so it always reflects the current API.
   */
  private serveOpenApiSpec(res: ServerResponse): void {
    try {
      const spec = generateOpenApi(this.config, {
        asyncApiSpecUrl: this.resolveAsyncApiSpecUrl(),
        openApiSpecUrl: this.resolveOpenApiSpecUrl(),
      });
      res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
      res.end(JSON.stringify(spec, null, 2));
    } catch (err) {
      log.error("Failed to generate OpenAPI spec", { error: String(err) });
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(err) }));
    }
  }

  /** Effective AsyncAPI spec URL (config override or the served default). */
  private resolveAsyncApiSpecUrl(): string | undefined {
    return resolveSpecUrl(this.config.server.asyncApiSpecUrl, ASYNC_API_PATH);
  }

  /** Effective OpenAPI spec URL (config override or the served default). */
  private resolveOpenApiSpecUrl(): string | undefined {
    return resolveSpecUrl(this.config.server.openApiSpecUrl, OPEN_API_PATH);
  }

  /**
   * GET /plugin/:channelId/:accountId/qr
   * Start QR login and return the QR code as an HTML page with the image embedded.
   * This is the "scan-friendly" endpoint — opens in a browser.
   */
  private async handleQrHttpRequest(
    channelId: string,
    accountId: string,
    url: URL,
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<void> {
    const force = url.searchParams.get("force") === "true";

    try {
      const result = await this.channelManager.loginWithQrStart(channelId, {
        accountId,
        force,
      });

      if (!result.qrDataUrl) {
        res.writeHead(502, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "No QR code available", message: result.message }));
        return;
      }

      // Return an HTML page that displays the QR code and auto-polls for status
      const html = buildQrPageHtml(channelId, accountId, result.qrDataUrl, result.message, result.sessionKey);
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(html);
    } catch (err) {
      log.error("QR HTTP start failed", { channelId, accountId, error: String(err) });
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(err) }));
    }
  }

  /**
   * GET /plugin/:channelId/:accountId/qr/json
   * Start QR login and return the result as JSON (for programmatic use).
   */
  private async handleQrJsonHttpRequest(
    channelId: string,
    accountId: string,
    url: URL,
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<void> {
    const force = url.searchParams.get("force") === "true";

    try {
      const result = await this.channelManager.loginWithQrStart(channelId, {
        accountId,
        force,
      });

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        qrDataUrl: result.qrDataUrl,
        message: result.message,
        sessionKey: result.sessionKey,
      }));
    } catch (err) {
      log.error("QR JSON start failed", { channelId, accountId, error: String(err) });
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(err) }));
    }
  }

  /**
   * GET /plugin/:channelId/:accountId/qr/status
   * Poll the QR login status. Pass ?sessionKey=xxx from the start response.
   * This is a long-poll endpoint — it blocks until the login completes or times out.
   */
  private async handleQrStatusHttpRequest(
    channelId: string,
    accountId: string,
    url: URL,
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<void> {
    const sessionKey = url.searchParams.get("sessionKey") ?? undefined;
    const timeoutMs = parseInt(url.searchParams.get("timeoutMs") ?? "120000", 10);

    try {
      const result = await this.channelManager.loginWithQrWait(channelId, {
        accountId,
        sessionKey,
        timeoutMs,
      });

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        connected: result.connected,
        message: result.message,
        accountId: result.accountId,
        qrDataUrl: result.qrDataUrl,
      }));
    } catch (err) {
      log.error("QR status poll failed", { channelId, accountId, error: String(err) });
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(err) }));
    }
  }

  /** Route an inbound message from a channel to subscribed clients */
  private routeInboundMessage(msg: NormalizedInboundMessage): void {
    const payload: InboundMessagePayload = {
      messageId: msg.messageId,
      chatId: msg.chatId,
      senderId: msg.senderId,
      senderName: msg.senderName,
      msgType: msg.msgType,
      text: msg.text,
      timestamp: msg.timestamp,
      wasEncrypted: msg.wasEncrypted,
      replyToMessageId: msg.replyToMessageId,
      mediaUrl: msg.mediaUrl,
      mediaType: msg.mediaType,
      contextToken: msg.contextToken,
      raw: msg.raw,
    };

    const envelope = makeEnvelope(
      BridgeMessageType.INBOUND_MESSAGE,
      msg.channel,
      payload,
      { accountId: msg.accountId }
    );

    this.clientRegistry.broadcast(msg.channel, msg.accountId, envelope, msg.senderId);
  }

  /** Route a channel status change to subscribed clients */
  private routeStatusChange(status: import("../protocol/messages.js").ChannelStatus): void {
    const payload: ChannelStatusPayload = {
      status: status.state,
      detail: status.detail,
      error: status.lastError,
    };

    const envelope = makeEnvelope(
      BridgeMessageType.CHANNEL_STATUS,
      status.channel,
      payload,
      { accountId: status.accountId }
    );

    this.clientRegistry.broadcast(status.channel, status.accountId, envelope);
  }

  /** Build the welcome payload with current channel status */
  private buildWelcomePayload(): WelcomePayload {
    const channels: WelcomePayload["channels"] = {};
    for (const adapter of this.channelManager.getAllAdapters()) {
      const accounts = adapter.listAccounts();
      const statuses = accounts.map((accId) => this.channelManager.getStatus(adapter.channelId, accId));
      channels[adapter.channelId] = {
        status: statuses.length > 0 ? statuses[0]?.state ?? "disconnected" : "disconnected",
        accounts,
      };
    }
    return {
      version: "1.0.0",
      channels,
      asyncApiSpecUrl: this.resolveAsyncApiSpecUrl(),
      openApiSpecUrl: this.resolveOpenApiSpecUrl(),
    };
  }

  /** Send an error envelope to a client */
  private sendError(
    client: ClientConnection,
    requestId: string,
    channel: string,
    code: string,
    message: string
  ): void {
    const payload: SendErrorPayload = { requestId, code, message };
    client.send(
      makeEnvelope(BridgeMessageType.SEND_ERROR, channel, payload, {
        id: requestId,
      })
    );
  }

  /** Check client heartbeats — terminate stale connections */
  private checkClientHeartbeats(): void {
    const now = Date.now();
    const timeout = (this.config.server.clientHeartbeatMs ?? 30000) * 2;

    for (const client of this.clientRegistry.getAll()) {
      const ws = client.socket as any;
      if (ws.readyState !== WebSocket.OPEN) continue;

      // Ping the client
      ws.ping();

      // Check if the client has responded to a previous ping
      if (ws._lastPong && now - ws._lastPong > timeout) {
        log.warn("Terminating stale client", { clientId: client.id });
        ws.terminate();
        this.clientRegistry.unregister(client.id);
      }
    }
  }
}

// ─── QR Page HTML ─────────────────────────────────────────────────────────────

/**
 * Build an HTML page that displays a QR code and auto-polls for login status.
 * This provides a browser-friendly way to scan QR codes for plugin auth.
 */
function buildQrPageHtml(
  channelId: string,
  accountId: string,
  qrDataUrl: string,
  message: string,
  sessionKey?: string,
): string {
  const escapedSessionKey = sessionKey
    ? JSON.stringify(sessionKey).replace(/</g, "\\u003c")
    : '""';
  const escapedChannelId = channelId.replace(/</g, "&lt;");
  const escapedAccountId = accountId.replace(/</g, "&lt;");
  const escapedMessage = message.replace(/</g, "&lt;").replace(/"/g, "&quot;");

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>QR Login — ${escapedChannelId}/${escapedAccountId}</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #0f172a; color: #e2e8f0;
    display: flex; flex-direction: column; align-items: center; justify-content: center;
    min-height: 100vh; padding: 2rem;
  }
  .card {
    background: #1e293b; border-radius: 16px; padding: 2rem;
    box-shadow: 0 25px 50px -12px rgba(0,0,0,0.5);
    max-width: 420px; width: 100%; text-align: center;
  }
  h1 { font-size: 1.25rem; margin-bottom: 0.5rem; color: #f1f5f9; }
  .subtitle { font-size: 0.875rem; color: #94a3b8; margin-bottom: 1.5rem; }
  .qr-container {
    background: #fff; border-radius: 12px; padding: 16px;
    display: inline-block; margin-bottom: 1rem;
  }
  .qr-container img { display: block; width: 256px; height: 256px; }
  .status {
    font-size: 0.875rem; padding: 0.75rem 1rem; border-radius: 8px;
    margin-top: 1rem;
  }
  .status.waiting { background: #1e3a5f; color: #93c5fd; }
  .status.success { background: #14532d; color: #86efac; }
  .status.error { background: #7f1d1d; color: #fca5a5; }
  .spinner {
    display: inline-block; width: 16px; height: 16px;
    border: 2px solid #93c5fd; border-top-color: transparent;
    border-radius: 50%; animation: spin 1s linear infinite;
    vertical-align: middle; margin-right: 6px;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
  .meta { font-size: 0.75rem; color: #64748b; margin-top: 1.5rem; }
</style>
</head>
<body>
<div class="card">
  <h1>Scan QR Code to Login</h1>
  <p class="subtitle">${escapedChannelId} / ${escapedAccountId}</p>
  <div class="qr-container">
    <img src="${qrDataUrl}" alt="QR Code" id="qr-img">
  </div>
  <div id="status" class="status waiting">
    <span class="spinner"></span> Waiting for scan...
  </div>
  <p class="meta">Message: ${escapedMessage}</p>
</div>
<script>
(function() {
  var channelId = ${JSON.stringify(channelId).replace(/</g, "\\u003c")};
  var accountId = ${JSON.stringify(accountId).replace(/</g, "\\u003c")};
  var sessionKey = ${escapedSessionKey};
  var statusEl = document.getElementById("status");
  var qrImg = document.getElementById("qr-img");
  var pollInterval;

  function poll() {
    var url = "/plugin/" + encodeURIComponent(channelId) + "/" + encodeURIComponent(accountId) + "/qr/status?timeoutMs=30000";
    if (sessionKey) url += "&sessionKey=" + encodeURIComponent(sessionKey);

    fetch(url)
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.connected) {
          statusEl.className = "status success";
          statusEl.innerHTML = "\\u2705 Connected! Account: " + (data.accountId || accountId);
          clearInterval(pollInterval);
        } else if (data.qrDataUrl) {
          // QR was refreshed — update the image
          qrImg.src = data.qrDataUrl;
          statusEl.className = "status waiting";
          statusEl.innerHTML = '<span class="spinner"></span> QR refreshed, waiting for scan...';
        } else {
          statusEl.className = "status waiting";
          statusEl.innerHTML = '<span class="spinner"></span> ' + (data.message || "Waiting for scan...");
        }
      })
      .catch(function(err) {
        statusEl.className = "status error";
        statusEl.innerHTML = "\\u26a0\\ufe0f Poll error: " + err.message + " (retrying...)";
      });
  }

  // Start polling every 5 seconds
  pollInterval = setInterval(poll, 5000);
  // Initial poll after 2 seconds
  setTimeout(poll, 2000);
})();
</script>
</body>
</html>`;
}
