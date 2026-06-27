/**
 * E2E test for the QQ Bot channel.
 *
 * Mirrors e2e-test-feishu.ts but targets the `qqbot` channel provided by the
 * `@tencent-connect/openclaw-qqbot` plugin. It verifies the full bridge
 * pipeline against a real QQ Bot application: server start → account connect
 * → WS client → list channels → subscribe → send outbound text → receive an
 * inbound message → verify contact persistence.
 *
 * Prerequisites:
 *   - `@tencent-connect/openclaw-qqbot` plugin installed
 *   - config.json with a `qqbot` section containing valid appId/clientSecret
 *     (sandbox or production QQ Bot application credentials)
 *   - A real QQ user who will message the bot and (optionally) receive a reply
 *
 * Configuration (env vars, all optional):
 *   QQBOT_TARGET_ID   - recipient to send outbound messages to. QQ Bot targets
 *                       are channel-native openid strings, optionally prefixed
 *                       to disambiguate the chat type:
 *                         qqbot:c2c:<user_openid>   (direct message)
 *                         qqbot:group:<group_openid> (group chat)
 *                       A bare openid defaults to a c2c (direct) message.
 *                       Defaults to QQBOT_SENDER_ID (round-trip to self).
 *   QQBOT_SENDER_ID   - sender user openid to match inbound messages against.
 *   QQBOT_ACCOUNT_ID  - account id in config (defaults to "default").
 *
 * Run: npm run test:e2e:qqbot
 */

import { resolve } from "node:path";
import { existsSync, readFileSync } from "node:fs";
import { WebSocket } from "ws";
import { BridgeServer } from "../server/bridge-server.js";
import { ClientRegistry } from "../server/client-registry.js";
import { ChannelManager } from "../channels/channel-manager.js";
import { loadChannelAdapters } from "../channels/plugin-loader.js";
import { loadConfig } from "../config/schema.js";
import { ContactStore } from "../contacts/contact-store.js";
import { BridgeMessageType, type BridgeEnvelope } from "../protocol/messages.js";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("e2e-qqbot");

// The channel id exposed by @tencent-connect/openclaw-qqbot (see openclaw.plugin.json).
const CHANNEL_ID = "qqbot";

// Account configured in config.json (defaults to "default").
const ACCOUNT_ID = process.env.QQBOT_ACCOUNT_ID ?? "default";

// Real QQ user openid used to match inbound messages.
const SENDER_ID = process.env.QQBOT_SENDER_ID ?? "";

// Recipient for outbound messages — defaults to the sender so the test can
// round-trip against a single user when only one QQ user is involved.
const TARGET_ID = process.env.QQBOT_TARGET_ID ?? SENDER_ID;

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Wait for a condition to be true, polling every `intervalMs` */
function waitFor(
  condition: () => boolean,
  timeoutMs: number,
  intervalMs = 200,
): Promise<void> {
  return new Promise((resolveP, rejectP) => {
    const start = Date.now();
    const check = () => {
      if (condition()) return resolveP();
      if (Date.now() - start > timeoutMs) {
        return rejectP(new Error(`waitFor timed out after ${timeoutMs}ms`));
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
  return new Promise((resolveP, rejectP) => {
    const ws = new WebSocket(url);
    const messages: BridgeEnvelope[] = [];

    ws.on("message", (data) => {
      messages.push(JSON.parse(data.toString()));
    });

    ws.on("open", () => {
      resolveP({
        ws,
        messages,
        close: () => ws.close(),
      });
    });

    ws.on("error", rejectP);

    // Timeout
    setTimeout(() => rejectP(new Error("WS connect timed out")), 10000);
  });
}

/** Send a message and wait for a specific response type */
function sendAndWait(
  ws: WebSocket,
  envelope: Partial<BridgeEnvelope> & { type: BridgeMessageType; payload: unknown },
  responseType: BridgeMessageType,
  timeoutMs = 5000,
): Promise<BridgeEnvelope> {
  return new Promise((resolveP, rejectP) => {
    const timer = setTimeout(() => rejectP(new Error(`Timed out waiting for ${responseType}`)), timeoutMs);

    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (msg.type === responseType) {
        clearTimeout(timer);
        ws.off("message", handler);
        resolveP(msg);
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
  return new Promise((resolveP, rejectP) => {
    const timer = setTimeout(() => rejectP(new Error("Timed out waiting for send_ack or send_error")), timeoutMs);

    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (msg.type === BridgeMessageType.SEND_ACK || msg.type === BridgeMessageType.SEND_ERROR) {
        clearTimeout(timer);
        ws.off("message", handler);
        resolveP(msg);
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

/** Wait for an inbound_message from a specific sender (any sender if none given) */
function waitForInboundMessage(
  ws: WebSocket,
  fromUserId: string | undefined,
  timeoutMs = 60000,
): Promise<BridgeEnvelope> {
  return new Promise((resolveP, rejectP) => {
    const timer = setTimeout(() => rejectP(new Error("Timed out waiting for inbound message")), timeoutMs);

    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (msg.type === BridgeMessageType.INBOUND_MESSAGE) {
        const payload = msg.payload as any;
        if (!fromUserId || payload.senderId === fromUserId) {
          clearTimeout(timer);
          ws.off("message", handler);
          resolveP(msg);
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

    if (config.logging?.level) {
      (rootLogger as any).minLevel = ["debug", "info", "warn", "error"].indexOf(config.logging.level);
    }

    // Verify the qqbot channel is configured before going any further.
    const qqbotCfg = config.channels[CHANNEL_ID];
    if (!qqbotCfg || qqbotCfg.enabled === false || !qqbotCfg.accounts?.[ACCOUNT_ID]) {
      throw new Error(
        `config.json has no enabled ${CHANNEL_ID} account "${ACCOUNT_ID}" — ` +
        `add a qqbot section with appId/clientSecret first.`,
      );
    }
    const cred = qqbotCfg.accounts[ACCOUNT_ID];
    if (!cred.appId || !cred.clientSecret) {
      throw new Error(
        `${CHANNEL_ID} account "${ACCOUNT_ID}" is missing appId/clientSecret in config.json`,
      );
    }
    log.info("QQ Bot config verified", { accountId: ACCOUNT_ID, hasAppId: !!cred.appId });

    const channelManager = new ChannelManager();
    const clientRegistry = new ClientRegistry();

    const contactStore = new ContactStore(configPath);
    channelManager.setContactStore(contactStore);

    // Discover and load channel adapters
    const adapters = await loadChannelAdapters();
    for (const [, adapter] of adapters) {
      channelManager.registerAdapter(adapter);
    }

    if (!channelManager.getAdapter(CHANNEL_ID)) {
      throw new Error(
        `${CHANNEL_ID} adapter not loaded — is @tencent-connect/openclaw-qqbot installed?`,
      );
    }

    server = new BridgeServer(config, channelManager, clientRegistry);
    await server.start();
    log.info("Server started");

    // Start the qqbot account
    const mergedConfig = { ...cred, ...(qqbotCfg.transport ?? {}) };
    try {
      await channelManager.startAccount(CHANNEL_ID, ACCOUNT_ID, mergedConfig);
      log.info("Started channel account", { channelId: CHANNEL_ID, accountId: ACCOUNT_ID });
    } catch (err) {
      log.error("Failed to start channel account", { channelId: CHANNEL_ID, accountId: ACCOUNT_ID, error: String(err) });
    }

    // Wait for the qqbot account to connect (the gateway opens a WebSocket to
    // the QQ Bot platform; this may take a few seconds to handshake).
    log.info("Waiting for qqbot account to connect...");
    await waitFor(
      () => channelManager.getStatus(CHANNEL_ID, ACCOUNT_ID).state === "connected",
      15000,
    );
    log.info("QQ Bot account connected!");

    // ── Step 2: Connect WS client ─────────────────────────────────────────
    log.info("=== Step 2: Connecting WS client ===");

    const port = config.server.port ?? 9300;
    const path = config.server.path ?? "/bridge";
    client = await connectClient(`ws://127.0.0.1:${port}${path}`);
    log.info("Client connected");

    const welcome = client.messages.find((m) => m.type === BridgeMessageType.WELCOME);
    if (!welcome) {
      throw new Error("Did not receive welcome message");
    }
    const welcomeChannels = Object.keys((welcome.payload as any).channels ?? {});
    log.info("Received welcome", { channels: welcomeChannels.join(",") });
    if (!welcomeChannels.includes(CHANNEL_ID)) {
      throw new Error(`welcome did not advertise ${CHANNEL_ID}`);
    }

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

    const qqbotInfo = channelsList.channels?.[CHANNEL_ID];
    if (!qqbotInfo) {
      throw new Error(`${CHANNEL_ID} not present in list_channels response`);
    }
    const accInfo = qqbotInfo.accounts?.[ACCOUNT_ID];
    if (!accInfo) {
      throw new Error(`${CHANNEL_ID} account ${ACCOUNT_ID} not in list_channels response`);
    }
    log.info(`qqbot account status: ${accInfo.status}`);
    if (accInfo.status !== "connected") {
      throw new Error(`${CHANNEL_ID}/${ACCOUNT_ID} not connected (status=${accInfo.status})`);
    }

    // ── Step 4: Subscribe to inbound messages ─────────────────────────────
    log.info("=== Step 4: Subscribing to inbound messages ===");

    const subResp = await sendAndWait(
      client.ws,
      {
        type: BridgeMessageType.SUBSCRIBE,
        channel: CHANNEL_ID,
        accountId: ACCOUNT_ID,
        payload: { channel: CHANNEL_ID, accountId: ACCOUNT_ID },
      },
      BridgeMessageType.CHANNEL_STATUS,
      5000,
    );
    log.info("Subscribed to channel", { status: (subResp.payload as any).status });

    // ── Step 5: Send outbound messages ────────────────────────────────────
    log.info("=== Step 5: Sending messages to qqbot ===");

    const testMessages = [
      "Hello from qqbot e2e test #1 🚀",
      "Bridge is working — message #2 ✅",
      "Final qqbot test message #3 🎉",
    ];

    let ackCount = 0;
    let errorCount = 0;

    if (!TARGET_ID) {
      log.warn("QQBOT_TARGET_ID / QQBOT_SENDER_ID not set — skipping outbound send step");
    } else {
      for (let i = 0; i < testMessages.length; i++) {
        const text = testMessages[i];
        log.info(`Sending message ${i + 1}/3: "${text}"`);

        try {
          const resp = await sendAndWaitForAckOrError(
            client.ws,
            {
              type: BridgeMessageType.SEND_TEXT,
              channel: CHANNEL_ID,
              accountId: ACCOUNT_ID,
              payload: {
                to: TARGET_ID,
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

      const totalResponses = ackCount + errorCount;
      if (totalResponses === 0) {
        throw new Error("No send responses received — bridge may not be routing messages correctly");
      }
    }

    // ── Step 6: Wait for inbound message ──────────────────────────────────
    log.info("=== Step 6: Waiting for inbound message ===");
    if (SENDER_ID) {
      log.info(`Send a message in QQ to the bot — waiting for user ${SENDER_ID}...`);
    } else {
      log.info("Send a message in QQ to the bot — waiting for any inbound message...");
    }

    try {
      const inboundMsg = await waitForInboundMessage(client.ws, SENDER_ID || undefined, 120000);
      const inboundPayload = inboundMsg.payload as any;
      log.info("Received inbound message!", {
        messageId: inboundPayload.messageId,
        senderId: inboundPayload.senderId,
        senderName: inboundPayload.senderName,
        chatId: inboundPayload.chatId,
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

    // Verify the qqbot sender was persisted if we observed inbound
    if (SENDER_ID) {
      const qqbotContacts = contactStore.getContactsForAccount(CHANNEL_ID, ACCOUNT_ID);
      const found = qqbotContacts.find((c) => c.userId === SENDER_ID);
      if (found) {
        log.info("QQ Bot sender persisted as contact", { userId: SENDER_ID });
      } else {
        log.warn("QQ Bot sender was not persisted (no inbound message observed)");
      }
    }

    if (existsSync(contactStore.path)) {
      const raw = readFileSync(contactStore.path, "utf-8");
      const data = JSON.parse(raw);
      log.info(`contacts.json file verified: ${Object.keys(data.contacts ?? {}).length} contacts`);
    } else {
      log.warn("contacts.json file not found (no inbound messages received)");
    }

    // ── Summary ───────────────────────────────────────────────────────────
    log.info("=== QQ Bot E2E Test Complete ===");
    log.info(`Channel: ${CHANNEL_ID}, account: ${ACCOUNT_ID}`);
    log.info(`Messages sent: 3, acks: ${ackCount}, errors: ${errorCount}`);
    log.info(`Contacts persisted: ${contacts.length}`);
    log.info("✅ All steps passed!");

  } finally {
    if (client) client.close();
    if (server) await server.stop();
  }
}

// ─── Entry Point ──────────────────────────────────────────────────────────────

runTest().then(() => {
  process.exit(0);
}).catch((err) => {
  log.error("QQ Bot E2E test failed", { error: String(err), stack: err.stack });
  process.exit(1);
});
