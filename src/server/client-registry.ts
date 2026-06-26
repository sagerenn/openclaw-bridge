/**
 * ClientRegistry — tracks all connected WS clients and their subscriptions.
 */

import type { ClientConnection } from "./client-connection.js";
import type { BridgeEnvelope } from "../protocol/messages.js";

export class ClientRegistry {
  private clients = new Map<string, ClientConnection>();

  /** Register a new client */
  register(client: ClientConnection): void {
    this.clients.set(client.id, client);
  }

  /** Remove a client (on disconnect) */
  unregister(clientId: string): void {
    this.clients.delete(clientId);
  }

  /** Get a client by ID */
  get(clientId: string): ClientConnection | undefined {
    return this.clients.get(clientId);
  }

  /** Get all connected clients */
  getAll(): ClientConnection[] {
    return [...this.clients.values()];
  }

  /** Get number of connected clients */
  get size(): number {
    return this.clients.size;
  }

  /** Get all clients subscribed to a given channel account */
  getSubscribers(channel: string, accountId: string): ClientConnection[] {
    const key = `${channel}:${accountId}`;
    return [...this.clients.values()].filter((c) => c.hasSubscription(key));
  }

  /**
   * Send a message to all clients subscribed to a channel account,
   * respecting per-client sender filters.
   */
  broadcast(
    channel: string,
    accountId: string,
    envelope: BridgeEnvelope,
    senderId?: string
  ): void {
    const key = `${channel}:${accountId}`;
    for (const client of this.clients.values()) {
      if (!client.isConnected) continue;
      if (!client.hasSubscription(key)) continue;
      if (senderId && !client.shouldReceiveMessage(key, senderId)) continue;
      client.send(envelope);
    }
  }

  /** Send to a specific client */
  sendTo(clientId: string, envelope: BridgeEnvelope): boolean {
    const client = this.clients.get(clientId);
    if (!client || !client.isConnected) return false;
    client.send(envelope);
    return true;
  }
}
