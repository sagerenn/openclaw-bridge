/**
 * ClientConnection — represents a single WS client connected to the bridge.
 */

import WebSocket from "ws";
import type { BridgeEnvelope } from "../protocol/messages.js";

export class ClientConnection {
  readonly id: string;
  private ws: WebSocket;
  private subscriptions = new Set<string>(); // "channel:accountId" keys
  private filters = new Map<string, { fromUserIds?: string[] }>(); // key -> filter

  constructor(ws: WebSocket) {
    this.id = `client-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    this.ws = ws;
  }

  /** Send a typed envelope to this client */
  send(envelope: BridgeEnvelope): void {
    if (this.ws.readyState !== WebSocket.OPEN) return;
    try {
      this.ws.send(JSON.stringify(envelope));
    } catch {
      // Client disconnected during send — ignore
    }
  }

  /** Close the connection */
  close(code?: number, reason?: string): void {
    if (this.ws.readyState === WebSocket.OPEN) {
      this.ws.close(code ?? 1001, reason ?? "server shutdown");
    }
  }

  /** Check if the WS is still open */
  get isConnected(): boolean {
    return this.ws.readyState === WebSocket.OPEN;
  }

  /** Add a subscription */
  addSubscription(key: string, filter?: { fromUserIds?: string[] }): void {
    this.subscriptions.add(key);
    if (filter) this.filters.set(key, filter);
  }

  /** Remove a subscription */
  removeSubscription(key: string): void {
    this.subscriptions.delete(key);
    this.filters.delete(key);
  }

  /** Check if subscribed to a channel:account */
  hasSubscription(key: string): boolean {
    return this.subscriptions.has(key);
  }

  /** Get all subscription keys */
  getSubscriptionKeys(): string[] {
    return [...this.subscriptions];
  }

  /** Check if this client should receive a message from a given sender */
  shouldReceiveMessage(key: string, senderId: string): boolean {
    if (!this.subscriptions.has(key)) return false;
    const filter = this.filters.get(key);
    if (!filter?.fromUserIds?.length) return true;
    return filter.fromUserIds.includes(senderId);
  }

  /** Get the underlying WebSocket */
  get socket(): WebSocket {
    return this.ws;
  }
}
