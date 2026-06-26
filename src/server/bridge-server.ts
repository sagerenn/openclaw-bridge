/**
 * BridgeServer — the core WebSocket server that bridges clients to channel backends.
 */

import { createServer, type IncomingMessage, type Server } from "node:http";
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
  type SendAckPayload,
  type SendErrorPayload,
  type WelcomePayload,
  type NormalizedInboundMessage,
} from "../protocol/messages.js";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("server");

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
      case BridgeMessageType.PING:
        client.send(makeEnvelope(BridgeMessageType.PONG, "*", {}, { id: envelope.id }));
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
    return { version: "1.0.0", channels };
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
