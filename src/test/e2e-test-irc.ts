/**
 * E2E test: IRC channel over a real self-hosted IRC server.
 *
 * This is the only fully self-contained, credential-free channel E2E test:
 * it spins up no external SaaS, needs no bot tokens, and exercises the
 * complete bridge pipeline against a real IRC daemon (Ergo) —
 *
 *   WS client ──[WebSocket]──▶ Bridge ──[IRC channel plugin]──▶ IRC server
 *        ◀──[inbound_message]──        ◀──[PRIVMSG inbound]──
 *
 * The "real IM user" on the other side is a minimal raw IRC client driven
 * in-process by this test (a second nick that receives outbound PRIVMSGs
 * from the bot and replies to produce inbound traffic).
 *
 * Exchange of 3 messages. For each of 3 messages:
 *   1. WS client sends send_text  -> bridge -> plugin.outbound.sendText ->
 *      the raw IRC user receives the PRIVMSG.            (outbound leg)
 *   2. The raw IRC user PRIVMSGs the bot back             (inbound leg)
 *      -> plugin gateway -> deliver -> bridge -> WS client
 *      receives inbound_message.
 *
 * Prerequisites:
 *   - An IRC server reachable at IRC_HOST:IRC_PORT (plaintext). In CI this is
 *     an Ergo service container; locally any IRC daemon works.
 *   - The `irc` channel plugin bundled inside the `openclaw` package
 *     (auto-discovered from node_modules/openclaw/dist/extensions/irc).
 *
 * Configuration (env vars):
 *   IRC_HOST        - IRC server host (default 127.0.0.1)
 *   IRC_PORT        - IRC server plaintext port (default 6667)
 *   IRC_BOT_NICK    - nick the bridge bot registers as (default openclaw-bot)
 *   IRC_SENDER_NICK - nick the in-test raw IRC user uses (default e2e-sender)
 *   BRIDGE_PORT     - port for the bridge WS server (default 9498)
 *
 * Run: npm run test:e2e:irc
 */

import net from "node:net";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { WebSocket } from "ws";
import { BridgeServer } from "../server/bridge-server.js";
import { ClientRegistry } from "../server/client-registry.js";
import { ChannelManager } from "../channels/channel-manager.js";
import { loadChannelAdapters } from "../channels/plugin-loader.js";
import { loadConfig, type BridgeConfig } from "../config/schema.js";
import { ContactStore } from "../contacts/contact-store.js";
import { BridgeMessageType, type BridgeEnvelope } from "../protocol/messages.js";
import { resolve, join } from "node:path";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("e2e-irc");

const CHANNEL_ID = "irc";
const ACCOUNT_ID = "default";

const IRC_HOST = process.env.IRC_HOST ?? "127.0.0.1";
const IRC_PORT = parseInt(process.env.IRC_PORT ?? "6667", 10);
const IRC_BOT_NICK = process.env.IRC_BOT_NICK ?? "openclaw-bot";
const IRC_SENDER_NICK = process.env.IRC_SENDER_NICK ?? "e2e-sender";
const BRIDGE_PORT = parseInt(process.env.BRIDGE_PORT ?? "9498", 10);

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
  timeoutMs = 10000,
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

/** Wait for the next inbound_message whose text matches `expectedText`. */
function waitForInbound(
  ws: WebSocket,
  expectedText: string,
  timeoutMs = 30000,
): Promise<BridgeEnvelope> {
  return new Promise((resolveP, rejectP) => {
    const timer = setTimeout(
      () => rejectP(new Error(`Timed out waiting for inbound "${expectedText}"`)),
      timeoutMs,
    );
    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (msg.type === BridgeMessageType.INBOUND_MESSAGE) {
        const payload = msg.payload as any;
        if (payload.text === expectedText) {
          clearTimeout(timer);
          ws.off("message", handler);
          resolveP(msg);
        }
      }
    };
    ws.on("message", handler);
  });
}

// ─── Minimal raw IRC client (the "real IM user") ────────────────────────────

/**
 * A tiny line-based IRC client used as the far-end IM user: it registers a
 * nick, exposes received PRIVMSGs, and can send PRIVMSGs back to the bot.
 */
class RawIrcUser {
  private socket: net.Socket | null = null;
  private buffer = "";
  private ready = false;
  private onPrivmsg: (fromNick: string, to: string, text: string) => void;

  constructor(
    private readonly host: string,
    private readonly port: number,
    private readonly nick: string,
    onPrivmsg: (fromNick: string, to: string, text: string) => void,
  ) {
    this.onPrivmsg = onPrivmsg;
  }

  connect(): Promise<void> {
    return new Promise((resolveP, rejectP) => {
      const socket = net.connect(this.port, this.host, () => {
        socket.write(`NICK ${this.nick}\r\nUSER ${this.nick} 0 * :${this.nick}\r\n`);
      });
      this.socket = socket;
      socket.setEncoding("utf8");
      const fail = (err: Error) => rejectP(err);
      socket.on("error", fail);
      socket.on("data", (chunk: string) => {
        this.buffer += chunk;
        let idx: number;
        while ((idx = this.buffer.indexOf("\r\n")) >= 0) {
          const line = this.buffer.slice(0, idx);
          this.buffer = this.buffer.slice(idx + 2);
          if (line.startsWith("PING")) {
            socket.write(`PONG ${line.slice(5)}\r\n`);
            continue;
          }
          if (!this.ready && /\s001\s/.test(line)) {
            this.ready = true;
            socket.off("error", fail);
            resolveP();
          }
          const m = line.match(/^:([^!\s]+)!\S+ PRIVMSG (\S+) :(.*)$/);
          if (m) this.onPrivmsg(m[1], m[2], m[3]);
        }
      });
      setTimeout(() => rejectP(new Error("IRC sender connect timed out")), 10000);
    });
  }

  sendPrivmsg(target: string, text: string): void {
    this.socket?.write(`PRIVMSG ${target} :${text}\r\n`);
  }

  close(): void {
    try {
      this.socket?.write("QUIT :e2e done\r\n");
    } catch {}
    this.socket?.destroy();
  }
}

// ─── Test Runner ──────────────────────────────────────────────────────────────

async function runTest(): Promise<void> {
  let server: BridgeServer | undefined;
  let client: Awaited<ReturnType<typeof connectClient>> | undefined;
  let sender: RawIrcUser | undefined;

  const configPath = resolve(process.cwd(), "config.json");

  try {
    // ── Step 1: Build a config pointing the irc channel at the test IRC server
    log.info("=== Step 1: Starting bridge with irc channel ===", { IRC_HOST, IRC_PORT, IRC_BOT_NICK });

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
        irc: {
          enabled: true,
          accounts: {
            [ACCOUNT_ID]: {
              host: IRC_HOST,
              port: IRC_PORT,
              tls: false,
              nick: IRC_BOT_NICK,
              username: IRC_BOT_NICK,
              realname: "OpenClaw Bridge Bot",
              // open DM policy + wildcard allowFrom so the in-test sender's
              // messages are dispatched (not dropped) by the IRC ingress gate.
              dmPolicy: "open",
              groupPolicy: "open",
              allowFrom: ["*"],
            },
          },
        },
      },
      logging: {
        level: (process.env.E2E_IRC_LOG_LEVEL ?? "info") as "debug" | "info" | "warn" | "error",
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
    // its constructor — use an isolated temp dir so we never clobber the real
    // contacts.json in the repo working directory.
    const contactStoreDir = mkdtempSync(join(tmpdir(), "openclaw-irc-e2e-"));
    const contactStore = new ContactStore(join(contactStoreDir, "config.json"));
    channelManager.setContactStore(contactStore);

    // Discover and load channel adapters (the bundled irc plugin is discovered
    // from node_modules/openclaw/dist/extensions/irc and its runtime installed).
    const adapters = await loadChannelAdapters();
    for (const [, adapter] of adapters) {
      channelManager.registerAdapter(adapter);
    }
    if (!channelManager.getAdapter(CHANNEL_ID)) {
      throw new Error(
        `${CHANNEL_ID} adapter not loaded — is the openclaw package installed ` +
          `(node_modules/openclaw/dist/extensions/irc)?`,
      );
    }

    server = new BridgeServer(config, channelManager, clientRegistry);
    await server.start();
    log.info("Bridge server started", { port: BRIDGE_PORT });

    // Start the IRC account.
    const ircCfg = config.channels.irc as any;
    const cred = ircCfg.accounts[ACCOUNT_ID];
    await channelManager.startAccount(CHANNEL_ID, ACCOUNT_ID, cred);

    // Wait for the bot's IRC gateway to connect.
    log.info("Waiting for irc account to connect...");
    await waitFor(
      () => channelManager.getStatus(CHANNEL_ID, ACCOUNT_ID).state === "connected",
      15000,
    );
    log.info("IRC bot connected!");

    // ── Step 2: Connect the far-end IM user (raw IRC sender)
    log.info("=== Step 2: Connecting raw IRC sender ===", { nick: IRC_SENDER_NICK });
    sender = new RawIrcUser(IRC_HOST, IRC_PORT, IRC_SENDER_NICK, () => {});
    await sender.connect();
    // Give the server a moment to register the sender nick fully.
    await new Promise((r) => setTimeout(r, 1000));
    log.info("Raw IRC sender connected");

    // ── Step 3: Connect the WS client and subscribe
    log.info("=== Step 3: Connecting WS client ===");
    client = await connectClient(`ws://127.0.0.1:${BRIDGE_PORT}/bridge`);
    // The welcome message arrives asynchronously after the socket opens — wait
    // for it rather than racing the (possibly empty) messages array.
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
    log.info("WS client subscribed to irc/default");

    // ── Step 4: Exchange 3 messages (full round-trip each)
    log.info("=== Step 4: Exchanging 3 messages ===");

    const outboundTexts = [
      "e2e-irc round-trip message 1",
      "e2e-irc round-trip message 2",
      "e2e-irc round-trip message 3",
    ];
    const inboundTexts = [
      "e2e-irc reply 1",
      "e2e-irc reply 2",
      "e2e-irc reply 3",
    ];

    // Capture PRIVMSGs the bot sends to the sender (outbound leg).
    const receivedBySender: string[] = [];
    sender["onPrivmsg"] = (_from: string, _to: string, text: string) =>
      receivedBySender.push(text);

    for (let i = 0; i < outboundTexts.length; i++) {
      const outText = outboundTexts[i];
      const replyText = inboundTexts[i];
      log.info(`--- Exchange ${i + 1}/3 ---`);

      // Outbound: WS client -> bridge -> plugin -> IRC -> sender receives.
      const ack = await sendAndWaitForAckOrError(
        client.ws,
        {
          type: BridgeMessageType.SEND_TEXT,
          channel: CHANNEL_ID,
          accountId: ACCOUNT_ID,
          payload: { to: IRC_SENDER_NICK, text: outText },
        },
        10000,
      );
      if (ack.type !== BridgeMessageType.SEND_ACK) {
        const e = ack.payload as any;
        throw new Error(`send_text #${i + 1} failed: ${e.code} ${e.message}`);
      }
      log.info(`outbound #${i + 1} acked`, { requestId: (ack.payload as any).requestId });

      // Wait for the sender to actually receive the PRIVMSG from the bot.
      await waitFor(
        () => receivedBySender.includes(outText),
        10000,
      );
      log.info(`sender received outbound #${i + 1}: "${outText}"`);

      // Inbound: sender PRIVMSGs the bot -> plugin gateway -> WS client.
      sender.sendPrivmsg(IRC_BOT_NICK, replyText);
      const inbound = await waitForInbound(client.ws, replyText, 30000);
      const p = inbound.payload as any;
      log.info(`inbound #${i + 1} received by WS client`, {
        senderId: p.senderId,
        text: p.text,
      });
    }

    // ── Step 5: Verify contact persistence
    log.info("=== Step 5: Verifying contact persistence ===");
    contactStore.flush();
    const contacts = contactStore.getAllContacts();
    log.info(`Contacts stored: ${contacts.length}`);

    // ── Summary
    log.info("=== IRC E2E Test Complete ===");
    log.info(`Exchanges: 3/3 round-trips succeeded (3 outbound + 3 inbound)`);
    log.info(`Contacts persisted: ${contacts.length}`);
    log.info("✅ All steps passed!");
  } finally {
    if (client) client.close();
    if (sender) sender.close();
    if (server) await server.stop();
  }
}

// ─── Entry Point ──────────────────────────────────────────────────────────────

runTest().then(() => {
  process.exit(0);
}).catch((err) => {
  log.error("IRC E2E test failed", { error: String(err), stack: err.stack });
  process.exit(1);
});
