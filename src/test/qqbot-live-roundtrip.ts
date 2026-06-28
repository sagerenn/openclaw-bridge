/**
 * Live qqbot round-trip harness:
 *   start server → connect qqbot → WS client subscribes → wait for inbound
 *   message from a real QQ user → echo a reply back to the sender's phone
 *   → verify send_ack. Stays up until both legs are observed or timeout.
 *
 *   QQBOT_REPLY_TEXT env overrides the reply body (default: an ack string).
 */
import { resolve } from "node:path";
import { WebSocket } from "ws";
import { BridgeServer } from "../server/bridge-server.js";
import { ClientRegistry } from "../server/client-registry.js";
import { ChannelManager } from "../channels/channel-manager.js";
import { loadChannelAdapters } from "../channels/plugin-loader.js";
import { loadConfig } from "../config/schema.js";
import { ContactStore } from "../contacts/contact-store.js";
import { BridgeMessageType, type BridgeEnvelope } from "../protocol/messages.js";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("qqbot-live");
const CHANNEL_ID = "qqbot";
const ACCOUNT_ID = process.env.QQBOT_ACCOUNT_ID ?? "default";
const REPLY_TEXT = process.env.QQBOT_REPLY_TEXT ?? "✅ openclaw-bridge qqbot round-trip OK — inbound received, this reply is outbound to your phone.";
const INBOUND_TIMEOUT_MS = Number(process.env.QQBOT_INBOUND_TIMEOUT_MS ?? 600000); // 10 min

function sendAndWait(ws: WebSocket, envelope: Partial<BridgeEnvelope> & { type: BridgeMessageType; payload: unknown }, responseType: BridgeMessageType, timeoutMs = 5000): Promise<BridgeEnvelope> {
  return new Promise((resolveP, rejectP) => {
    const timer = setTimeout(() => rejectP(new Error(`Timed out waiting for ${responseType}`)), timeoutMs);
    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (msg.type === responseType) { clearTimeout(timer); ws.off("message", handler); resolveP(msg); }
    };
    ws.on("message", handler);
    const full: BridgeEnvelope = { v: 1, id: envelope.id ?? `live-${Date.now()}`, type: envelope.type,
      channel: envelope.channel ?? "*", accountId: envelope.accountId, payload: envelope.payload };
    ws.send(JSON.stringify(full));
  });
}

function sendAndWaitAckOrErr(ws: WebSocket, envelope: Partial<BridgeEnvelope> & { type: BridgeMessageType; payload: unknown }, timeoutMs = 15000): Promise<BridgeEnvelope> {
  return new Promise((resolveP, rejectP) => {
    const timer = setTimeout(() => rejectP(new Error("Timed out waiting for send_ack or send_error")), timeoutMs);
    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (msg.type === BridgeMessageType.SEND_ACK || msg.type === BridgeMessageType.SEND_ERROR) {
        clearTimeout(timer); ws.off("message", handler); resolveP(msg);
      }
    };
    ws.on("message", handler);
    const full: BridgeEnvelope = { v: 1, id: envelope.id ?? `live-${Date.now()}`, type: envelope.type,
      channel: envelope.channel ?? "*", accountId: envelope.accountId, payload: envelope.payload };
    ws.send(JSON.stringify(full));
  });
}

function waitForInbound(ws: WebSocket, timeoutMs: number): Promise<BridgeEnvelope> {
  return new Promise((resolveP, rejectP) => {
    const timer = setTimeout(() => rejectP(new Error("Timed out waiting for inbound message")), timeoutMs);
    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (msg.type === BridgeMessageType.INBOUND_MESSAGE) {
        clearTimeout(timer); ws.off("message", handler); resolveP(msg);
      }
    };
    ws.on("message", handler);
  });
}

function waitFor(cond: () => boolean, timeoutMs: number, intervalMs = 200): Promise<void> {
  return new Promise<void>((resolveP, rejectP) => {
    const start = Date.now();
    const check = () => {
      if (cond()) return resolveP();
      if (Date.now() - start > timeoutMs) return rejectP(new Error(`waitFor timed out after ${timeoutMs}ms`));
      setTimeout(check, intervalMs);
    };
    check();
  });
}

async function main() {
  let server: BridgeServer | undefined;
  let client: { ws: WebSocket; messages: BridgeEnvelope[]; close: () => void } | undefined;
  const configPath = resolve(process.cwd(), "config.json");
  try {
    const config = loadConfig(configPath);
    if (config.logging?.level) (rootLogger as any).minLevel = ["debug","info","warn","error"].indexOf(config.logging.level);
    const qqbotCfg = config.channels[CHANNEL_ID] as any;
    if (!qqbotCfg) throw new Error("qqbot channel not configured");
    const cred = qqbotCfg.accounts[ACCOUNT_ID];
    const channelManager = new ChannelManager();
    const clientRegistry = new ClientRegistry();
    const contactStore = new ContactStore(configPath);
    channelManager.setContactStore(contactStore);
    for (const [, adapter] of await loadChannelAdapters()) channelManager.registerAdapter(adapter);
    server = new BridgeServer(config, channelManager, clientRegistry);
    await server.start();
    const mergedConfig = { ...cred, ...(qqbotCfg.transport ?? {}) };
    await channelManager.startAccount(CHANNEL_ID, ACCOUNT_ID, mergedConfig);
    await waitFor(() => channelManager.getStatus(CHANNEL_ID, ACCOUNT_ID).state === "connected", 15000);
    log.info("qqbot connected");

    const port = config.server.port ?? 9300;
    const path = config.server.path ?? "/bridge";
    client = await new Promise<{ ws: WebSocket; messages: BridgeEnvelope[]; close: () => void }>((res, rej) => {
      const ws = new WebSocket(`ws://127.0.0.1:${port}${path}`);
      const messages: BridgeEnvelope[] = [];
      ws.on("message", (d) => messages.push(JSON.parse(d.toString())));
      ws.on("open", () => res({ ws, messages, close: () => ws.close() }));
      ws.on("error", rej);
      setTimeout(() => rej(new Error("WS connect timed out")), 10000);
    });

    await sendAndWait(client.ws, { type: BridgeMessageType.SUBSCRIBE, channel: CHANNEL_ID, accountId: ACCOUNT_ID,
      payload: { channel: CHANNEL_ID, accountId: ACCOUNT_ID } }, BridgeMessageType.CHANNEL_STATUS, 5000);
    log.info("subscribed — waiting for inbound message from your QQ. Send a message to the bot now.");

    const inbound = await waitForInbound(client.ws, INBOUND_TIMEOUT_MS);
    const p = inbound.payload as any;
    log.info("INBOUND RECEIVED on WS client", { messageId: p.messageId, senderId: p.senderId,
      senderName: p.senderName, chatId: p.chatId, replyTo: p.replyTo, msgType: p.msgType, text: p.text });

    const target = p.replyTo ?? p.senderId;
    log.info("sending outbound reply to phone", { to: target, text: REPLY_TEXT });
    const resp = await sendAndWaitAckOrErr(client.ws, { type: BridgeMessageType.SEND_TEXT,
      channel: CHANNEL_ID, accountId: ACCOUNT_ID, payload: { to: target, text: REPLY_TEXT,
        replyToMessageId: p.messageId } }, 15000);

    if (resp.type === BridgeMessageType.SEND_ACK) {
      const ack = resp.payload as any;
      log.info("OUTBOUND ACK — reply delivered to phone", { requestId: ack.requestId, messageId: ack.messageId });
    } else {
      const err = resp.payload as any;
      log.error("OUTBOUND ERROR", { code: err.code, message: err.message });
    }

    contactStore.flush();
    const found = contactStore.getContactsForAccount(CHANNEL_ID, ACCOUNT_ID).find((c) => c.userId === p.senderId);
    log.info("contact persistence", { senderPersisted: !!found, userId: p.senderId });

    log.info("=== ROUND-TRIP COMPLETE ===");
    log.info(`inbound: ✅ received on WS client (sender ${p.senderId})`);
    log.info(`outbound: ${resp.type === BridgeMessageType.SEND_ACK ? "✅ acked, delivered to phone" : "❌ send_error"}`);
  } finally {
    if (client) client.close();
    if (server) await server.stop();
  }
}

main().then(() => process.exit(0)).catch((err) => { log.error("live harness failed", { error: String(err), stack: err.stack }); process.exit(1); });
