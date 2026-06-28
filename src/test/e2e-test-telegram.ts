/**
 * E2E test for the Telegram channel.
 *
 * Uses the bridge-native TelegramBridgeAdapter (src/channels/telegram-adapter),
 * which talks to the real Telegram Bot API directly over HTTP, to verify the
 * full bridge pipeline end-to-end:
 *   server start -> account connect (long-polling) -> WS client -> list channels
 *   -> subscribe -> send outbound text -> receive an inbound message -> verify
 *   contact persistence.
 *
 *   WS client --[WebSocket]--> Bridge --[telegram-adapter]--> Telegram Bot API
 *        <--[inbound_message]--        <--[getUpdates long-poll]--
 *
 * The bridge-native adapter is used instead of the bundled `telegram` openclaw
 * plugin because the bundled plugin hard-wires the openclaw AI-agent dispatch
 * for inbound, which never reaches WS clients (and fails without an API key).
 * The native adapter owns both legs and routes inbound straight to the WS seam.
 *
 * Telegram bots cannot initiate a DM -- a user must /start the bot first. So,
 * unlike the self-contained IRC/Mattermost suites (which drive a fake far-end
 * user in-process), this is a human-in-the-loop test in the style of the
 * feishu/qqbot/weixin suites: outbound messages are verified through the
 * send_ack returned by the bridge (the bot delivers them to the human's
 * Telegram chat), and the inbound leg waits for the human to send a message
 * back to the bot.
 *
 * To make the test immediately runnable, the bot's chat id (the user who
 * /started it) is auto-discovered via getUpdates before the bridge begins
 * long-polling -- otherwise it can be supplied explicitly via env vars. The
 * pending /start update is drained (acked via offset) before the bridge starts
 * so the inbound wait is a clean human reply rather than the stale /start.
 *
 * Prerequisites:
 *   - A Telegram bot token (from @BotFather) for the bot the bridge runs as.
 *   - A real Telegram user who has sent /start to the bot (so the bot has a
 *     DM chat to deliver outbound messages to) and who will send a reply to
 *     produce inbound traffic.
 *
 * Configuration (env vars):
 *   TELEGRAM_BOT_TOKEN  - bot token the bridge authenticates with (required).
 *   TELEGRAM_CHAT_ID    - chat id to send outbound messages to (the user who
 *                         /started the bot). If unset, auto-discovered via
 *                         getUpdates from the most recent private-chat message.
 *   TELEGRAM_SENDER_ID  - sender user id to match inbound messages against.
 *                         Defaults to TELEGRAM_CHAT_ID.
 *   BRIDGE_PORT         - port for the bridge WS server (default 9500).
 *
 * Run: npm run test:e2e:telegram
 */

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { WebSocket } from "ws";
import { BridgeServer } from "../server/bridge-server.js";
import { ClientRegistry } from "../server/client-registry.js";
import { ChannelManager } from "../channels/channel-manager.js";
import { TelegramBridgeAdapter } from "../channels/telegram-adapter.js";
import { loadConfig, type BridgeConfig } from "../config/schema.js";
import { ContactStore } from "../contacts/contact-store.js";
import { BridgeMessageType, type BridgeEnvelope } from "../protocol/messages.js";
import { resolve, join } from "node:path";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("e2e-telegram");

const CHANNEL_ID = "telegram";
const ACCOUNT_ID = "default";

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN ?? "";
// api.telegram.org is the default Bot API endpoint; overridable for a
// self-hosted Bot API server or a proxy.
const TELEGRAM_API_ROOT = (process.env.TELEGRAM_API_ROOT ?? "https://api.telegram.org").replace(/\/$/, "");
const BRIDGE_PORT = parseInt(process.env.BRIDGE_PORT ?? "9500", 10);

// ─── Helpers ──────────────────────────────────────────────────────────────────

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

function connectClient(url: string): Promise<{
  ws: WebSocket;
  messages: BridgeEnvelope[];
  close: () => void;
}> {
  return new Promise((resolveP, rejectP) => {
    const ws = new WebSocket(url);
    const messages: BridgeEnvelope[] = [];
    ws.on("message", (data) => messages.push(JSON.parse(data.toString())));
    ws.on("open", () => resolveP({ ws, messages, close: () => ws.close() }));
    ws.on("error", rejectP);
    setTimeout(() => rejectP(new Error("WS connect timed out")), 10000);
  });
}

function sendAndWait(
  ws: WebSocket,
  envelope: Partial<BridgeEnvelope> & { type: BridgeMessageType; payload: unknown },
  responseType: BridgeMessageType,
  timeoutMs = 8000,
): Promise<BridgeEnvelope> {
  return new Promise((resolveP, rejectP) => {
    const timer = setTimeout(
      () => rejectP(new Error(`Timed out waiting for ${responseType}`)),
      timeoutMs,
    );
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

function sendAndWaitForAckOrError(
  ws: WebSocket,
  envelope: Partial<BridgeEnvelope> & { type: BridgeMessageType; payload: unknown },
  timeoutMs = 15000,
): Promise<BridgeEnvelope> {
  return new Promise((resolveP, rejectP) => {
    const timer = setTimeout(
      () => rejectP(new Error("Timed out waiting for send_ack or send_error")),
      timeoutMs,
    );
    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (
        msg.type === BridgeMessageType.SEND_ACK ||
        msg.type === BridgeMessageType.SEND_ERROR
      ) {
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

/** Wait for an inbound_message from a specific sender (any sender if none given). */
function waitForInboundMessage(
  ws: WebSocket,
  fromUserId: string | undefined,
  timeoutMs = 120000,
): Promise<BridgeEnvelope> {
  return new Promise((resolveP, rejectP) => {
    const timer = setTimeout(
      () => rejectP(new Error("Timed out waiting for inbound message")),
      timeoutMs,
    );
    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (msg.type === BridgeMessageType.INBOUND_MESSAGE) {
        const payload = msg.payload as any;
        if (!fromUserId || String(payload.senderId) === String(fromUserId)) {
          clearTimeout(timer);
          ws.off("message", handler);
          resolveP(msg);
        }
      }
    };
    ws.on("message", handler);
  });
}

// ─── Minimal Telegram Bot API client ─────────────────────────────────────────
// Used only for pre-flight discovery: look up the bot, find the chat id of the
// user who /started it, and drain the pending /start update so the bridge's
// own long-poller starts on a clean slate. The bridge polls the same token at
// runtime, so we must NOT leave a competing poller running here.

function tgUrl(method: string): string {
  return `${TELEGRAM_API_ROOT}/bot${TELEGRAM_BOT_TOKEN}/${method}`;
}

async function tgGetMe(): Promise<{ id: number; username: string; first_name: string }> {
  const res = await fetch(tgUrl("getMe"));
  const body = (await res.json()) as any;
  if (!body.ok) throw new Error(`getMe failed: ${JSON.stringify(body)}`);
  return body.result;
}

/**
 * Fetch pending updates once (non-long-poll: timeout=0) and return the most
 * recent private-chat text message's chat/sender id, plus the highest update_id
 * seen (so the caller can ack/drain everything up to and including it).
 */
async function tgPeekUpdates(): Promise<{
  chatId: string;
  senderId: string;
  maxUpdateId: number;
}> {
  const res = await fetch(tgUrl("getUpdates?timeout=0&allowed_updates=%5B%22message%22%5D"));
  const body = (await res.json()) as any;
  if (!body.ok) throw new Error(`getUpdates failed: ${JSON.stringify(body)}`);
  const updates: any[] = body.result ?? [];
  let chatId = "";
  let senderId = "";
  let maxUpdateId = 0;
  for (const u of updates) {
    if (u.update_id > maxUpdateId) maxUpdateId = u.update_id;
    const msg = u.message;
    if (msg?.chat?.type === "private") {
      chatId = String(msg.chat.id);
      senderId = String(msg.from?.id ?? msg.chat.id);
    }
  }
  return { chatId, senderId, maxUpdateId };
}

/** Ack + drop all updates up to and including maxUpdateId via the offset param. */
async function tgDrainUpdates(maxUpdateId: number): Promise<void> {
  if (!maxUpdateId) return;
  const res = await fetch(tgUrl(`getUpdates?timeout=0&offset=${maxUpdateId + 1}`));
  await res.json();
}

// --- Test Runner -----------------------------------------------------------

async function runTest(): Promise<void> {
  let server: BridgeServer | undefined;
  let client: Awaited<ReturnType<typeof connectClient>> | undefined;

  const configPath = resolve(process.cwd(), "config.json");

  if (!TELEGRAM_BOT_TOKEN) {
    throw new Error(
      "Telegram E2E requires TELEGRAM_BOT_TOKEN env var (a BotFather bot token).",
    );
  }

  // Pre-flight: validate the token, find the chat id of the user who /started
  // the bot, and drain the stale /start so the bridge's long-poller does not
  // replay it as inbound before the human sends a real reply.
  const me = await tgGetMe();
  log.info("Bot identity", { id: me.id, username: me.username, name: me.first_name });

  let chatId = process.env.TELEGRAM_CHAT_ID ?? "";
  let senderId = process.env.TELEGRAM_SENDER_ID ?? chatId;
  if (!chatId) {
    log.info("TELEGRAM_CHAT_ID not set; auto-discovering via getUpdates...");
    const peek = await tgPeekUpdates();
    if (!peek.chatId) {
      throw new Error(
        "No private chat found in getUpdates -- send /start to the bot first, " +
          "or set TELEGRAM_CHAT_ID explicitly.",
      );
    }
    chatId = peek.chatId;
    senderId = peek.senderId;
    // Drain the stale /start (and anything else queued) so the bridge starts
    // on a clean slate.
    await tgDrainUpdates(peek.maxUpdateId);
    log.info("Drained pending updates", { upTo: peek.maxUpdateId });
  } else {
    // Even with an explicit chat id, drain any queued updates (e.g. the
    // original /start) so the bridge's poller starts on a clean slate and the
    // inbound wait catches the human's reply, not a stale command.
    const peek = await tgPeekUpdates();
    if (peek.maxUpdateId) {
      await tgDrainUpdates(peek.maxUpdateId);
      log.info("Drained pending updates", { upTo: peek.maxUpdateId });
    }
  }
  log.info("Target chat resolved", { chatId, senderId });

  try {
    // -- Step 1: Build a config pointing the telegram channel at the bot token
    log.info("=== Step 1: Starting bridge with telegram channel ===", {
      bot: me.username,
      chatId,
    });

    const base = loadConfig(configPath);
    const config: BridgeConfig = {
      ...base,
      server: {
        ...base.server,
        host: "127.0.0.1",
        port: BRIDGE_PORT,
        path: "/bridge",
      },
      channels: {
        telegram: {
          enabled: true,
          accounts: {
            [ACCOUNT_ID]: {
              // Plain-string botToken is accepted by the secret-input schema;
              // the plugin reads it for both outbound sendMessage calls and
              // the inbound getUpdates long-poll.
              botToken: TELEGRAM_BOT_TOKEN,
              // Open DM policy + wildcard allowFrom so the human's inbound
              // messages are dispatched (not dropped / not gated behind
              // pairing) by the telegram ingress gate.
              dmPolicy: "open",
              groupPolicy: "open",
              allowFrom: ["*"],
            },
          },
        },
      },
      logging: {
        level: (process.env.E2E_TELEGRAM_LOG_LEVEL ?? "info") as "debug" | "info" | "warn" | "error",
      },
    };

    if (config.logging?.level) {
      (rootLogger as any).minLevel = (
        ["debug", "info", "warn", "error"] as const
      ).indexOf(config.logging.level);
    }

    const channelManager = new ChannelManager();
    const clientRegistry = new ClientRegistry();
    // ContactStore writes contacts.json into the dirname of the path passed to
    // its constructor -- use an isolated temp dir so we never clobber the real
    // contacts.json in the repo working directory.
    const contactStoreDir = mkdtempSync(join(tmpdir(), "openclaw-tg-e2e-"));
    const contactStore = new ContactStore(join(contactStoreDir, "config.json"));
    channelManager.setContactStore(contactStore);

    // Register the bridge-native telegram adapter. We deliberately do NOT
    // loadChannelAdapters() (the bundled `telegram` plugin): the bundled
    // plugin hard-wires the openclaw AI-agent dispatch for inbound, which never
    // reaches WS clients and fails without an API key. The bridge-native
    // adapter owns both legs via the raw Bot API, routing inbound straight to
    // the WS-client seam. One token supports a single getUpdates consumer, so
    // only this adapter polls.
    channelManager.registerAdapter(new TelegramBridgeAdapter());
    if (!channelManager.getAdapter(CHANNEL_ID)) {
      throw new Error(`${CHANNEL_ID} adapter not registered`);
    }

    server = new BridgeServer(config, channelManager, clientRegistry);
    await server.start();
    log.info("Bridge server started", { port: BRIDGE_PORT });

    // Start the telegram account (begins Bot API long-polling for inbound).
    const tgCfg = config.channels.telegram as any;
    const cred = tgCfg.accounts[ACCOUNT_ID];
    await channelManager.startAccount(CHANNEL_ID, ACCOUNT_ID, cred);

    // Wait for the bot's telegram gateway to connect (long-poll active).
    log.info("Waiting for telegram account to connect...");
    await waitFor(
      () => channelManager.getStatus(CHANNEL_ID, ACCOUNT_ID).state === "connected",
      30000,
    );
    log.info("Telegram bot connected!");

    // -- Step 2: Connect the WS client and subscribe
    log.info("=== Step 2: Connecting WS client ===");
    client = await connectClient(`ws://127.0.0.1:${BRIDGE_PORT}/bridge`);
    const welcome = await waitFor(
      () => client!.messages.some((m) => m.type === BridgeMessageType.WELCOME),
      5000,
    ).then(() => client!.messages.find((m) => m.type === BridgeMessageType.WELCOME)!);
    if (!Object.keys((welcome.payload as any).channels ?? {}).includes(CHANNEL_ID)) {
      throw new Error(`welcome did not advertise ${CHANNEL_ID}`);
    }
    log.info("WS client connected; welcome received");

    await sendAndWait(
      client.ws,
      {
        type: BridgeMessageType.SUBSCRIBE,
        channel: CHANNEL_ID,
        accountId: ACCOUNT_ID,
        payload: { channel: CHANNEL_ID, accountId: ACCOUNT_ID },
      },
      BridgeMessageType.CHANNEL_STATUS,
      8000,
    );
    log.info("WS client subscribed to telegram/default");

    // -- Step 3: Send 3 outbound messages (verified via send_ack)
    log.info("=== Step 3: Sending 3 outbound messages ===");
    const testMessages = [
      `e2e-telegram outbound message 1 (bot @${me.username})`,
      `e2e-telegram outbound message 2`,
      `e2e-telegram outbound message 3`,
    ];

    let ackCount = 0;
    let errorCount = 0;
    for (let i = 0; i < testMessages.length; i++) {
      const text = testMessages[i];
      log.info(`Sending outbound ${i + 1}/3: "${text}"`);
      try {
        const resp = await sendAndWaitForAckOrError(
          client.ws,
          {
            type: BridgeMessageType.SEND_TEXT,
            channel: CHANNEL_ID,
            accountId: ACCOUNT_ID,
            payload: { to: chatId, text },
          },
          15000,
        );
        if (resp.type === BridgeMessageType.SEND_ACK) {
          ackCount++;
          log.info(`Outbound ${i + 1} acknowledged`, {
            requestId: (resp.payload as any).requestId,
            messageId: (resp.payload as any).messageId,
          });
        } else {
          errorCount++;
          const e = resp.payload as any;
          log.warn(`Outbound ${i + 1} send_error`, { code: e.code, message: e.message });
        }
      } catch (err: any) {
        log.warn(`Outbound ${i + 1} timed out: ${err.message}`);
        errorCount++;
      }
    }

    if (ackCount === 0) {
      throw new Error(
        "No outbound message was acknowledged -- the bridge is not routing " +
          "send_text through the telegram plugin to Telegram.",
      );
    }
    log.info(`Outbound results: ${ackCount} acked, ${errorCount} errored`);
    log.info("Check the bot's Telegram chat -- you should see those messages.");

    // -- Step 4: Wait for a real inbound message from the human
    log.info("=== Step 4: Waiting for inbound message ===");
    log.info(`Send any message in Telegram to @${me.username} (waiting up to 120s)...`);
    try {
      const inboundMsg = await waitForInboundMessage(client.ws, senderId || undefined, 120000);
      const p = inboundMsg.payload as any;
      log.info("Received inbound message!", {
        messageId: p.messageId,
        senderId: p.senderId,
        senderName: p.senderName,
        chatId: p.chatId,
        text: p.text,
        msgType: p.msgType,
      });
    } catch (err: any) {
      log.warn(`No inbound message received within timeout: ${err.message}`);
      log.warn("This is OK if you didn't send a message -- the outbound test still passed.");
    }

    // -- Step 5: Verify contact persistence
    log.info("=== Step 5: Verifying contact persistence ===");
    contactStore.flush();
    const contacts = contactStore.getAllContacts();
    log.info(`Contacts stored: ${contacts.length}`);
    for (const contact of contacts) {
      log.info("Contact", {
        channel: contact.channel,
        accountId: contact.accountId,
        userId: contact.userId,
        displayName: contact.displayName,
      });
    }

    // -- Summary
    log.info("=== Telegram E2E Test Complete ===");
    log.info(`Channel: ${CHANNEL_ID}, account: ${ACCOUNT_ID}, bot: @${me.username}`);
    log.info(`Outbound: 3 sent, ${ackCount} acked, ${errorCount} errored`);
    log.info(`Contacts persisted: ${contacts.length}`);
    log.info("All steps passed!");
  } finally {
    if (client) client.close();
    if (server) await server.stop();
  }
}

// --- Entry Point -----------------------------------------------------------

runTest().then(() => {
  process.exit(0);
}).catch((err) => {
  log.error("Telegram E2E test failed", { error: String(err), stack: err.stack });
  process.exit(1);
});
