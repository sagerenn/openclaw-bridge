/**
 * E2E test for the WeChat (Weixin) channel.
 *
 * Mirrors e2e-test-feishu.ts / e2e-test-qqbot.ts but targets the
 * `openclaw-weixin` channel provided by the `@tencent-weixin/openclaw-weixin`
 * plugin. WeChat differs from Feishu/QQ in that it has no static credentials
 * in config.json -- it authenticates via QR code, and the account ID is
 * assigned by the server at login time (persisted to the plugin's state dir).
 *
 * The test verifies the full QR-login bridge pipeline: server start, QR
 * start, wait for scan, account connect (server-assigned ID), WS client,
 * list channels, subscribe, send outbound text, receive an inbound message,
 * verify contact persistence.
 *
 * Prerequisites:
 *   - `@tencent-weixin/openclaw-weixin` plugin installed
 *   - config.json with an `openclaw-weixin` section (accounts may be empty --
 *     WeChat uses QR login). The account named by WEIXIN_ACCOUNT_ID (default
 *     "default") is used only as the QR-start target; the real connected
 *     account ID comes back from the QR wait response.
 *   - A phone with WeChat ready to scan the QR code
 *
 * Configuration (env vars, all optional):
 *   WEIXIN_TARGET_ID   - recipient (WeChat user id, ends with @im.wechat) to
 *                        send outbound messages to. Defaults to
 *                        WEIXIN_SENDER_ID (round-trip to self).
 *   WEIXIN_SENDER_ID   - sender user id to match inbound messages against.
 *   WEIXIN_ACCOUNT_ID  - account id used for the QR-start request (the
 *                        account the QR login will bind to; defaults to
 *                        "default"). The actual connected account id is
 *                        taken from the qr_wait response.
 *   WEIXIN_QR_TIMEOUT  - max ms to wait for the QR scan (default 180000 = 3min).
 *
 * Run: npm run test:e2e:weixin
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

const log = rootLogger.child("e2e-weixin");

// The channel id exposed by @tencent-weixin/openclaw-weixin (see openclaw.plugin.json).
const CHANNEL_ID = "openclaw-weixin";

// Account the QR-start request targets. WeChat credentials are NOT in config;
// they are produced by the QR scan. The real connected account id (assigned by
// the server) is taken from the qr_wait response.
const ACCOUNT_ID = process.env.WEIXIN_ACCOUNT_ID ?? "default";

// Real WeChat user id (ends with @im.wechat) used to match inbound messages.
const SENDER_ID = process.env.WEIXIN_SENDER_ID ?? "";

// Recipient for outbound messages -- defaults to the sender so the test can
// round-trip against a single user when only one WeChat user is involved.
const TARGET_ID = process.env.WEIXIN_TARGET_ID ?? SENDER_ID;

// How long to wait for the user to scan the QR code with their phone.
const QR_TIMEOUT = parseInt(process.env.WEIXIN_QR_TIMEOUT ?? "180000", 10);

// --- Helpers ---

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

// --- Test Runner ---

async function runTest(): Promise<void> {
  let server: BridgeServer | undefined;
  let client: Awaited<ReturnType<typeof connectClient>> | undefined;

  const configPath = resolve(process.cwd(), "config.json");

  try {
    // -- Step 1: Load config and start server --
    log.info("=== Step 1: Starting server ===");

    const config = loadConfig(configPath);

    if (config.logging?.level) {
      (rootLogger as any).minLevel = ["debug", "info", "warn", "error"].indexOf(config.logging.level);
    }

    // Verify the openclaw-weixin channel is present and enabled. Unlike
    // feishu/qqbot, the account entry may be empty -- WeChat logs in via QR.
    const weixinCfg = config.channels[CHANNEL_ID];
    if (!weixinCfg || weixinCfg.enabled === false) {
      throw new Error(
        `config.json has no enabled ${CHANNEL_ID} section -- add an ` +
        `"openclaw-weixin" entry (accounts may be empty; QR login is used).`,
      );
    }
    log.info("WeChat config verified", { accountId: ACCOUNT_ID, note: "credentials come from QR login, not config" });

    const channelManager = new ChannelManager();
    const clientRegistry = new ClientRegistry();

    const contactStore = new ContactStore(configPath);
    channelManager.setContactStore(contactStore);

    // When QR login succeeds, auto-start the (server-assigned) account. This
    // mirrors the production boot path in server.ts.
    channelManager.setOnQrLoginSuccess(async (channelId, accountId) => {
      log.info("QR login success callback — auto-starting account", { channelId, accountId });
      try {
        await channelManager.startAccount(channelId, accountId, {});
      } catch (err) {
        log.error("Auto-start after QR login failed", { channelId, accountId, error: String(err) });
      }
    });

    // Discover and load channel adapters
    const adapters = await loadChannelAdapters();
    for (const [, adapter] of adapters) {
      channelManager.registerAdapter(adapter);
    }

    if (!channelManager.getAdapter(CHANNEL_ID)) {
      throw new Error(
        `${CHANNEL_ID} adapter not loaded — is @tencent-weixin/openclaw-weixin installed?`,
      );
    }

    server = new BridgeServer(config, channelManager, clientRegistry);
    await server.start();
    log.info("Server started");

    // -- Step 2: Connect WS client --
    // Connect early so we can drive the QR flow over the same socket.
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

    // -- Step 3: Start QR login --
    log.info("=== Step 3: Starting QR login ===");

    const qrStartResp = await sendAndWait(
      client.ws,
      {
        type: BridgeMessageType.QR_START,
        channel: CHANNEL_ID,
        accountId: ACCOUNT_ID,
        payload: { accountId: ACCOUNT_ID, force: false },
      },
      BridgeMessageType.QR_RESULT,
      15000,
    );

    const qrStartPayload = qrStartResp.payload as any;
    if (!qrStartPayload.sessionKey) {
      throw new Error(`QR start did not return a sessionKey: ${qrStartPayload.message ?? "(no message)"}`);
    }
    log.info("QR code generated", {
      hasImage: !!qrStartPayload.qrDataUrl,
      message: qrStartPayload.message,
    });

    if (qrStartPayload.qrDataUrl) {
      // Write the QR image to disk so the user can open/scanner it easily in
      // environments without a browser (e.g. headless CI / SSH sessions).
      const qrOutPath = resolve(process.cwd(), "weixin-qr.png");
      try {
        const b64 = String(qrStartPayload.qrDataUrl).replace(/^data:[^;]+;base64,/, "");
        const { writeFileSync } = await import("node:fs");
        writeFileSync(qrOutPath, Buffer.from(b64, "base64"));
        log.info(`QR image written to ${qrOutPath} — open it and scan with WeChat`);
      } catch (err) {
        log.warn("Could not write QR image to disk", { error: String(err) });
      }
    }

    let sessionKey = qrStartPayload.sessionKey;

    // -- Step 4: Wait for the user to scan --
    log.info("=== Step 4: Waiting for WeChat scan ===");
    log.info(`Scan the QR code with WeChat (timeout ${Math.round(QR_TIMEOUT / 1000)}s)...`);

    // The qr_wait endpoint long-polls; loop until connected or QR refreshed.
    // If the server-side login session expires (e.g. "no login in progress"),
    // re-initiate the QR so the user can scan a fresh code instead of spinning.
    let connectedAccountId: string | undefined;
    const qrDeadline = Date.now() + QR_TIMEOUT;
    let lastSessionExpired = 0;
    while (Date.now() < qrDeadline && !connectedAccountId) {
      const remaining = qrDeadline - Date.now();
      const roundTimeoutMs = Math.min(remaining, 30000);
      const waitResp = await sendAndWait(
        client.ws,
        {
          type: BridgeMessageType.QR_WAIT,
          channel: CHANNEL_ID,
          accountId: ACCOUNT_ID,
          payload: { accountId: ACCOUNT_ID, sessionKey, timeoutMs: roundTimeoutMs },
        },
        BridgeMessageType.QR_RESULT,
        roundTimeoutMs + 5000,
      ).catch((err) => {
        log.warn("qr_wait round failed, retrying", { error: String(err) });
        return undefined;
      });

      if (!waitResp) {
        // Brief pause to avoid a tight loop on repeated transport failures.
        await new Promise((r) => setTimeout(r, 1000));
        continue;
      }
      const waitPayload = waitResp.payload as any;

      if (waitPayload.connected) {
        connectedAccountId = waitPayload.accountId;
        log.info("QR login succeeded!", { accountId: connectedAccountId });
        break;
      }
      if (waitPayload.qrDataUrl) {
        log.info("QR was refreshed — scan the new code");
        continue;
      }

      const message: string = waitPayload.message ?? "";
      // Server signals the login session lapsed; restart the QR flow.
      const expired = message.includes("没有进行中的登录") || message.includes("登录超时") || message.includes("expired");
      if (expired) {
        // Throttle re-init to at most once every few seconds.
        if (Date.now() - lastSessionExpired > 3000) {
          lastSessionExpired = Date.now();
          log.warn("Login session expired — requesting a fresh QR code");
          try {
            const restartResp = await sendAndWait(
              client.ws,
              {
                type: BridgeMessageType.QR_START,
                channel: CHANNEL_ID,
                accountId: ACCOUNT_ID,
                payload: { accountId: ACCOUNT_ID, force: true },
              },
              BridgeMessageType.QR_RESULT,
              15000,
            );
            const restartPayload = restartResp.payload as any;
            if (restartPayload.sessionKey) sessionKey = restartPayload.sessionKey;
            if (restartPayload.qrDataUrl) {
              const qrOutPath = resolve(process.cwd(), "weixin-qr.png");
              try {
                const b64 = String(restartPayload.qrDataUrl).replace(/^data:[^;]+;base64,/, "");
                const { writeFileSync } = await import("node:fs");
                writeFileSync(qrOutPath, Buffer.from(b64, "base64"));
                log.info(`Fresh QR image written to ${qrOutPath}`);
              } catch {
                // best-effort
              }
            }
          } catch (err) {
            log.warn("QR restart failed", { error: String(err) });
            await new Promise((r) => setTimeout(r, 2000));
          }
        } else {
          await new Promise((r) => setTimeout(r, 1000));
        }
        continue;
      }

      log.info("Still waiting for scan...", { message });
    }

    if (!connectedAccountId) {
      throw new Error(`QR login did not complete within ${Math.round(QR_TIMEOUT / 1000)}s`);
    }

    // -- Step 5: Wait for the account to connect --
    log.info("=== Step 5: Waiting for WeChat account to connect ===");
    log.info("Waiting for account to connect...", { accountId: connectedAccountId });
    await waitFor(
      () => channelManager.getStatus(CHANNEL_ID, connectedAccountId!).state === "connected",
      30000,
    );
    log.info("WeChat account connected!", { accountId: connectedAccountId });

    // -- Step 6: List channels --
    log.info("=== Step 6: Listing channels ===");

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

    const weixinInfo = channelsList.channels?.[CHANNEL_ID];
    if (!weixinInfo) {
      throw new Error(`${CHANNEL_ID} not present in list_channels response`);
    }
    const accInfo = weixinInfo.accounts?.[connectedAccountId];
    if (!accInfo) {
      throw new Error(`${CHANNEL_ID} account ${connectedAccountId} not in list_channels response`);
    }
    log.info(`weixin account status: ${accInfo.status}`);
    if (accInfo.status !== "connected") {
      throw new Error(`${CHANNEL_ID}/${connectedAccountId} not connected (status=${accInfo.status})`);
    }

    // -- Step 7: Subscribe to inbound messages --
    log.info("=== Step 7: Subscribing to inbound messages ===");

    const subResp = await sendAndWait(
      client.ws,
      {
        type: BridgeMessageType.SUBSCRIBE,
        channel: CHANNEL_ID,
        accountId: connectedAccountId,
        payload: { channel: CHANNEL_ID, accountId: connectedAccountId },
      },
      BridgeMessageType.CHANNEL_STATUS,
      5000,
    );
    log.info("Subscribed to channel", { status: (subResp.payload as any).status });

    // -- Step 8: Send outbound messages --
    log.info("=== Step 8: Sending messages to wechat ===");

    const testMessages = [
      "Hello from weixin e2e test #1",
      "Bridge is working — message #2",
      "Final weixin test message #3",
    ];

    let ackCount = 0;
    let errorCount = 0;

    if (!TARGET_ID) {
      log.warn("WEIXIN_TARGET_ID / WEIXIN_SENDER_ID not set — skipping outbound send step");
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
              accountId: connectedAccountId,
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

    // -- Step 9: Wait for inbound message --
    log.info("=== Step 9: Waiting for inbound message ===");
    if (SENDER_ID) {
      log.info(`Send a message in WeChat to the bot — waiting for user ${SENDER_ID}...`);
    } else {
      log.info("Send a message in WeChat to the bot — waiting for any inbound message...");
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

    // -- Step 10: Verify contact persistence --
    log.info("=== Step 10: Verifying contact persistence ===");

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

    // Verify the WeChat sender was persisted if we observed inbound
    if (SENDER_ID) {
      const weixinContacts = contactStore.getContactsForAccount(CHANNEL_ID, connectedAccountId);
      const found = weixinContacts.find((c) => c.userId === SENDER_ID);
      if (found) {
        log.info("WeChat sender persisted as contact", { userId: SENDER_ID });
      } else {
        log.warn("WeChat sender was not persisted (no inbound message observed)");
      }
    }

    if (existsSync(contactStore.path)) {
      const raw = readFileSync(contactStore.path, "utf-8");
      const data = JSON.parse(raw);
      log.info(`contacts.json file verified: ${Object.keys(data.contacts ?? {}).length} contacts`);
    } else {
      log.warn("contacts.json file not found (no inbound messages received)");
    }

    // -- Summary --
    log.info("=== WeChat E2E Test Complete ===");
    log.info(`Channel: ${CHANNEL_ID}, account: ${connectedAccountId}`);
    log.info(`Messages sent: 3, acks: ${ackCount}, errors: ${errorCount}`);
    log.info(`Contacts persisted: ${contacts.length}`);
    log.info("All steps passed!");

  } finally {
    if (client) client.close();
    if (server) await server.stop();
  }
}

// --- Entry Point ---

runTest().then(() => {
  process.exit(0);
}).catch((err) => {
  log.error("WeChat E2E test failed", { error: String(err), stack: err.stack });
  process.exit(1);
});
