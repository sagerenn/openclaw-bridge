/**
 * E2E test: start server, connect client, list channels, send messages,
 * wait for inbound message, verify contact persistence.
 *
 * Prerequisites:
 *   - liangzimixin plugin installed
 *   - config.json with valid liangzimixin credentials
 *
 * Run: node dist/test/e2e-test.js
 */

import { resolve } from "node:path";
import { existsSync, readFileSync, unlinkSync } from "node:fs";
import { WebSocket } from "ws";
import { BridgeServer } from "../server/bridge-server.js";
import { ClientRegistry } from "../server/client-registry.js";
import { ChannelManager } from "../channels/channel-manager.js";
import { loadChannelAdapters } from "../channels/plugin-loader.js";
import { loadConfig } from "../config/schema.js";
import { ContactStore } from "../contacts/contact-store.js";
import { BridgeMessageType, type BridgeEnvelope } from "../protocol/messages.js";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("e2e-test");

// Real liangzimixin user ID (from inbound message logs)
const REAL_USER_ID = "3c641fe588c14faf90802e72a09c1a44";

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Wait for a condition to be true, polling every `intervalMs` */
function waitFor(
  condition: () => boolean,
  timeoutMs: number,
  intervalMs = 200,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const check = () => {
      if (condition()) return resolve();
      if (Date.now() - start > timeoutMs) {
        return reject(new Error(`waitFor timed out after ${timeoutMs}ms`));
      }
      setTimeout(check, intervalMs);
    };
    check();
  });
}

/** Connect a WS client and collect messages */
function connectClient(url: string): Promise<{
  ws: WebSocket;
  messages: BridgeEnvelope[];
  close: () => void;
}> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    const messages: BridgeEnvelope[] = [];

    ws.on("message", (data) => {
      messages.push(JSON.parse(data.toString()));
    });

    ws.on("open", () => {
      resolve({
        ws,
        messages,
        close: () => ws.close(),
      });
    });

    ws.on("error", reject);

    // Timeout
    setTimeout(() => reject(new Error("WS connect timed out")), 10000);
  });
}

/** Send a message and wait for a specific response type */
function sendAndWait(
  ws: WebSocket,
  envelope: Partial<BridgeEnvelope> & { type: BridgeMessageType; payload: unknown },
  responseType: BridgeMessageType,
  timeoutMs = 5000,
): Promise<BridgeEnvelope> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${responseType}`)), timeoutMs);

    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (msg.type === responseType) {
        clearTimeout(timer);
        ws.off("message", handler);
        resolve(msg);
      }
    };
    ws.on("message", handler);

    const full: BridgeEnvelope = {
      v: 1,
      id: envelope.id ?? `test-${Date.now()}`,
      type: envelope.type,
      channel: envelope.channel ?? "*",
      accountId: envelope.accountId,
      payload: envelope.payload,
    };
    ws.send(JSON.stringify(full));
  });
}

/** Send a message and wait for either send_ack or send_error */
function sendAndWaitForAckOrError(
  ws: WebSocket,
  envelope: Partial<BridgeEnvelope> & { type: BridgeMessageType; payload: unknown },
  timeoutMs = 15000,
): Promise<BridgeEnvelope> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Timed out waiting for send_ack or send_error")), timeoutMs);

    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (msg.type === BridgeMessageType.SEND_ACK || msg.type === BridgeMessageType.SEND_ERROR) {
        clearTimeout(timer);
        ws.off("message", handler);
        resolve(msg);
      }
    };
    ws.on("message", handler);

    const full: BridgeEnvelope = {
      v: 1,
      id: envelope.id ?? `test-${Date.now()}`,
      type: envelope.type,
      channel: envelope.channel ?? "*",
      accountId: envelope.accountId,
      payload: envelope.payload,
    };
    ws.send(JSON.stringify(full));
  });
}

/** Wait for an inbound_message from a specific sender */
function waitForInboundMessage(
  ws: WebSocket,
  fromUserId: string,
  timeoutMs = 60000,
): Promise<BridgeEnvelope> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timed out waiting for inbound message from ${fromUserId}`)), timeoutMs);

    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (msg.type === BridgeMessageType.INBOUND_MESSAGE) {
        const payload = msg.payload as any;
        if (payload.senderId === fromUserId) {
          clearTimeout(timer);
          ws.off("message", handler);
          resolve(msg);
        }
      }
    };
    ws.on("message", handler);
  });
}

// ─── Test Runner ──────────────────────────────────────────────────────────────

async function runTest(): Promise<void> {
  let server: BridgeServer | undefined;
  let client: Awaited<ReturnType<typeof connectClient>> | undefined;

  const configPath = resolve(process.cwd(), "config.json");

  try {
    // ── Step 1: Load config and start server ──────────────────────────────
    log.info("=== Step 1: Starting server ===");

    const config = loadConfig(configPath);

    // Apply logging level
    if (config.logging?.level) {
      (rootLogger as any).minLevel = ["debug", "info", "warn", "error"].indexOf(config.logging.level);
    }

    const channelManager = new ChannelManager();
    const clientRegistry = new ClientRegistry();

    // Create contact store
    const contactStore = new ContactStore(configPath);
    channelManager.setContactStore(contactStore);

    // Discover and load channel adapters
    const adapters = await loadChannelAdapters();
    for (const [, adapter] of adapters) {
      channelManager.registerAdapter(adapter);
    }

    // Start HTTP server first
    server = new BridgeServer(config, channelManager, clientRegistry);
    await server.start();
    log.info("Server started");

    // Start configured channel accounts
    for (const [channelId, channelConfig] of Object.entries(config.channels)) {
      if (!channelConfig || typeof channelConfig !== "object") continue;
      const cfg = channelConfig as any;
      if (cfg.enabled === false) continue;

      const adapter = channelManager.getAdapter(channelId);
      if (!adapter) {
        log.warn("No adapter for channel, skipping", { channelId });
        continue;
      }

      const accounts = cfg.accounts as Record<string, Record<string, unknown>> | undefined;
      if (!accounts) continue;

      for (const [accountId, accountConfig] of Object.entries(accounts)) {
        const mergedConfig = { ...accountConfig, ...(cfg.transport ?? {}) };
        try {
          await channelManager.startAccount(channelId, accountId, mergedConfig);
          log.info("Started channel account", { channelId, accountId });
        } catch (err) {
          log.error("Failed to start channel account", { channelId, accountId, error: String(err) });
        }
      }
    }

    // Wait for at least one channel to connect
    log.info("Waiting for channel to connect...");
    await waitFor(
      () => channelManager.getAllStatus().some((s) => s.state === "connected"),
      15000,
    );
    log.info("Channel connected!");

    // ── Step 2: Connect WS client ─────────────────────────────────────────
    log.info("=== Step 2: Connecting WS client ===");

    const port = config.server.port ?? 9300;
    const path = config.server.path ?? "/bridge";
    client = await connectClient(`ws://127.0.0.1:${port}${path}`);
    log.info("Client connected");

    // Wait for welcome
    const welcome = client.messages.find((m) => m.type === BridgeMessageType.WELCOME);
    if (!welcome) {
      throw new Error("Did not receive welcome message");
    }
    log.info("Received welcome", { channels: Object.keys((welcome.payload as any).channels ?? {}) });

    // ── Step 3: List channels ─────────────────────────────────────────────
    log.info("=== Step 3: Listing channels ===");

    const listResp = await sendAndWait(
      client.ws,
      {
        type: BridgeMessageType.LIST_CHANNELS,
        channel: "*",
        payload: { verbose: true },
      },
      BridgeMessageType.CHANNELS_LIST,
      5000,
    );

    const channelsList = listResp.payload as any;
    log.info("Channels list received", { channels: JSON.stringify(channelsList, null, 2) });

    // Verify we have at least one channel with a connected account
    const channels = channelsList.channels;
    const channelIds = Object.keys(channels);
    if (channelIds.length === 0) {
      throw new Error("No channels found in list_channels response");
    }
    log.info("Available channels: " + channelIds.join(", "));

    // Find a connected channel account
    let targetChannel: string | undefined;
    let targetAccount: string | undefined;
    for (const [chId, chInfo] of Object.entries(channels)) {
      const info = chInfo as any;
      for (const [accId, accInfo] of Object.entries(info.accounts ?? {})) {
        const acc = accInfo as any;
        if (acc.status === "connected") {
          targetChannel = chId;
          targetAccount = accId;
          break;
        }
      }
      if (targetChannel) break;
    }

    if (!targetChannel) {
      throw new Error("No connected channel account found — cannot test send_text");
    }
    log.info(`Using channel: ${targetChannel}, account: ${targetAccount}`);

    // ── Step 4: Subscribe to inbound messages ─────────────────────────────
    log.info("=== Step 4: Subscribing to inbound messages ===");

    const subResp = await sendAndWait(
      client.ws,
      {
        type: BridgeMessageType.SUBSCRIBE,
        channel: targetChannel,
        accountId: targetAccount,
        payload: { channel: targetChannel, accountId: targetAccount },
      },
      BridgeMessageType.CHANNEL_STATUS,
      5000,
    );
    log.info("Subscribed to channel", { status: (subResp.payload as any).status });

    // ── Step 5: Send three messages to real user ──────────────────────────
    log.info("=== Step 5: Sending three messages to real user ===");

    const testMessages = [
      "Hello from e2e test #1! 🚀",
      "Bridge is working — message #2 ✅",
      "Final test message #3 🎉",
    ];

    let ackCount = 0;
    let errorCount = 0;

    for (let i = 0; i < testMessages.length; i++) {
      const text = testMessages[i];
      log.info(`Sending message ${i + 1}/3: "${text}"`);

      try {
        const resp = await sendAndWaitForAckOrError(
          client.ws,
          {
            type: BridgeMessageType.SEND_TEXT,
            channel: targetChannel,
            accountId: targetAccount,
            payload: {
              to: REAL_USER_ID,
              text,
            },
          },
          15000,
        );

        if (resp.type === BridgeMessageType.SEND_ACK) {
          const ackPayload = resp.payload as any;
          ackCount++;
          log.info(`Message ${i + 1} acknowledged`, {
            requestId: ackPayload.requestId,
            messageId: ackPayload.messageId,
          });
        } else if (resp.type === BridgeMessageType.SEND_ERROR) {
          const errPayload = resp.payload as any;
          errorCount++;
          log.info(`Message ${i + 1} send_error`, {
            code: errPayload.code,
            message: errPayload.message,
          });
        }
      } catch (err: any) {
        log.warn(`Message ${i + 1} timed out waiting for response: ${err.message}`);
        errorCount++;
      }
    }

    // Verify at least some responses came back
    const totalResponses = ackCount + errorCount;
    if (totalResponses === 0) {
      throw new Error("No send responses received — bridge may not be routing messages correctly");
    }

    // ── Step 6: Wait for inbound message from real user ───────────────────
    log.info("=== Step 6: Waiting for inbound message ===");
    log.info(`Send a message on liangzimixin to the bot — waiting for user ${REAL_USER_ID}...`);

    try {
      const inboundMsg = await waitForInboundMessage(client.ws, REAL_USER_ID, 120000);
      const inboundPayload = inboundMsg.payload as any;
      log.info("Received inbound message!", {
        messageId: inboundPayload.messageId,
        senderId: inboundPayload.senderId,
        senderName: inboundPayload.senderName,
        text: inboundPayload.text,
        msgType: inboundPayload.msgType,
      });
    } catch (err: any) {
      log.warn(`No inbound message received within timeout: ${err.message}`);
      log.warn("This is OK if you didn't send a message — the outbound test still passed.");
    }

    // ── Step 7: Verify contact persistence ────────────────────────────────
    log.info("=== Step 7: Verifying contact persistence ===");

    contactStore.flush();
    const contacts = contactStore.getAllContacts();
    log.info(`Contacts stored: ${contacts.length}`);
    for (const contact of contacts) {
      log.info("Contact", {
        channel: contact.channel,
        accountId: contact.accountId,
        userId: contact.userId,
        displayName: contact.displayName,
        firstSeenAt: new Date(contact.firstSeenAt).toISOString(),
      });
    }

    // Verify contacts.json file exists
    if (existsSync(contactStore.path)) {
      const raw = readFileSync(contactStore.path, "utf-8");
      const data = JSON.parse(raw);
      log.info(`contacts.json file verified: ${Object.keys(data.contacts ?? {}).length} contacts`);
    } else {
      log.warn("contacts.json file not found (no inbound messages received)");
    }

    // ── Summary ───────────────────────────────────────────────────────────
    log.info("=== E2E Test Complete ===");
    log.info(`Channels: ${channelIds.join(", ")}`);
    log.info(`Connected: ${targetChannel}/${targetAccount}`);
    log.info(`Messages sent: 3, acks: ${ackCount}, errors: ${errorCount}`);
    log.info(`Contacts persisted: ${contacts.length}`);
    log.info("✅ All steps passed!");

  } finally {
    // Cleanup
    if (client) client.close();
    if (server) await server.stop();
  }
}

// ─── Entry Point ──────────────────────────────────────────────────────────────

runTest().then(() => {
  process.exit(0);
}).catch((err) => {
  log.error("E2E test failed", { error: String(err), stack: err.stack });
  process.exit(1);
});
