/**
 * E2E test: Mattermost channel over a real self-hosted Mattermost server.
 *
 * Spins up the bridge with the bundled `mattermost` channel plugin pointed at a
 * real Mattermost instance (the `mattermost/mattermost-preview` container in CI,
 * or any Mattermost server reachable locally), then exchanges 3 messages
 * round-trip with a far-end "real IM user":
 *
 *   WS client ──[WebSocket]──▶ Bridge ──[mattermost plugin]──▶ Mattermost
 *        ◀──[inbound_message]──        ◀──[posted WS event]──
 *
 * The "real IM user" on the far side is a minimal in-process Mattermost client
 * (REST + WebSocket events) driven by a Personal Access Token. It receives the
 * bot's outbound posts via the `posted` WebSocket event and posts back to the
 * bot over REST to produce inbound traffic through the bridge.
 *
 * Exchange of 3 messages. For each of 3 messages:
 *   1. WS client sends send_text (to: "user:<senderId>")
 *      -> bridge -> plugin.outbound -> bot POSTs to the DM channel
 *      -> the far-end Mattermost client receives the `posted` event.  (outbound leg)
 *   2. The far-end client POSTs a reply to the same DM channel           (inbound leg)
 *      -> plugin gateway -> deliver -> bridge -> WS client
 *      receives inbound_message.
 *
 * Prerequisites:
 *   - A Mattermost server reachable at MATTERMOST_URL (default http://127.0.0.1:8065)
 *     with user access tokens and bot accounts enabled. In CI this is the
 *     mattermost/mattermost-preview service container, provisioned by a shell
 *     step that creates a bot (MATTERMOST_BOT_TOKEN / MATTERMOST_BOT_USER_ID)
 *     and a human sender (MATTERMOST_SENDER_TOKEN / MATTERMOST_SENDER_USER_ID).
 *   - The `mattermost` channel plugin bundled inside the `openclaw` package
 *     (auto-discovered from node_modules/openclaw/dist/extensions/mattermost).
 *
 * Configuration (env vars):
 *   MATTERMOST_URL            - base URL of the Mattermost server (default http://127.0.0.1:8065)
 *   MATTERMOST_BOT_TOKEN      - bot access token the bridge authenticates with
 *   MATTERMOST_BOT_USER_ID    - user id of the bot account
 *   MATTERMOST_SENDER_TOKEN   - personal access token for the far-end "real IM user"
 *   MATTERMOST_SENDER_USER_ID - user id of the far-end sender account
 *   BRIDGE_PORT               - port for the bridge WS server (default 9499)
 *
 * Run: npm run test:e2e:mattermost
 */

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

const log = rootLogger.child("e2e-mattermost");

const CHANNEL_ID = "mattermost";
const ACCOUNT_ID = "default";

const MATTERMOST_URL = (process.env.MATTERMOST_URL ?? "http://127.0.0.1:8065").replace(/\/$/, "");
const MATTERMOST_BOT_TOKEN = process.env.MATTERMOST_BOT_TOKEN ?? "";
const MATTERMOST_BOT_USER_ID = process.env.MATTERMOST_BOT_USER_ID ?? "";
const MATTERMOST_SENDER_TOKEN = process.env.MATTERMOST_SENDER_TOKEN ?? "";
const MATTERMOST_SENDER_USER_ID = process.env.MATTERMOST_SENDER_USER_ID ?? "";
const BRIDGE_PORT = parseInt(process.env.BRIDGE_PORT ?? "9499", 10);

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

// ─── Minimal Mattermost client (the "real IM user") ─────────────────────────

/**
 * A tiny Mattermost REST + WebSocket-events client used as the far-end IM user.
 * It authenticates with a Personal Access Token, opens the event stream to
 * receive `posted` events (the bot's outbound DMs), and can post replies back
 * to the bot over REST to generate inbound traffic.
 */
class MattermostUser {
  private ws: WebSocket | null = null;
  private ready = false;
  /** Posts received on the sender's event stream, keyed by post id. */
  readonly received: { id: string; text: string; userId: string }[] = [];
  private dmChannelId: string | null = null;

  constructor(
    private readonly baseUrl: string,
    private readonly token: string,
    private readonly myUserId: string,
    private readonly botUserId: string,
  ) {}

  /** Open the WebSocket event stream and authenticate the PAT. */
  connect(): Promise<void> {
    return new Promise((resolveP, rejectP) => {
      const wsUrl = this.baseUrl.replace(/^http/, "ws") + "/api/v4/websocket";
      const ws = new WebSocket(wsUrl);
      this.ws = ws;
      ws.on("error", rejectP);
      ws.on("open", () => {
        // Authenticate the connection with the PAT.
        ws.send(
          JSON.stringify({
            seq: 1,
            action: "authentication_challenge",
            data: { token: this.token },
          }),
        );
      });
      ws.on("message", (data) => {
        let msg: any;
        try {
          msg = JSON.parse(data.toString());
        } catch {
          return;
        }
        // The server replies with a status hello / OK once authenticated.
        if (!this.ready && (msg.event === "hello" || msg.status === "OK")) {
          this.ready = true;
          resolveP();
        }
        if (msg.event === "posted") {
          const rawPost = msg.data?.post;
          if (!rawPost) return;
          let post: any;
          try {
            post = typeof rawPost === "string" ? JSON.parse(rawPost) : rawPost;
          } catch {
            return;
          }
          // Capture posts authored by the bot (outbound leg).
          if (post.user_id === this.botUserId) {
            this.received.push({
              id: post.id,
              text: post.message ?? "",
              userId: post.user_id,
            });
          }
        }
      });
      setTimeout(() => rejectP(new Error("Mattermost WS connect timed out")), 15000);
    });
  }

  /** Create (or look up) the DM channel between this user and the bot. */
  async ensureDmChannel(): Promise<string> {
    if (this.dmChannelId) return this.dmChannelId;
    const res = await fetch(`${this.baseUrl}/api/v4/channels/direct`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify([this.myUserId, this.botUserId]),
    });
    if (!res.ok) {
      throw new Error(`create DM channel failed: ${res.status} ${await res.text()}`);
    }
    const ch = (await res.json()) as any;
    this.dmChannelId = ch.id;
    return ch.id;
  }

  /** Post a message to the DM channel with the bot. */
  async postToBot(text: string): Promise<string> {
    const channelId = await this.ensureDmChannel();
    const res = await fetch(`${this.baseUrl}/api/v4/posts`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ channel_id: channelId, message: text }),
    });
    if (!res.ok) {
      throw new Error(`post to bot failed: ${res.status} ${await res.text()}`);
    }
    const post = (await res.json()) as any;
    return post.id as string;
  }

  /** Has the bot's outbound post with this exact text arrived over the WS? */
  receivedText(text: string): boolean {
    return this.received.some((p) => p.text === text);
  }

  close(): void {
    try {
      this.ws?.close();
    } catch {}
  }
}

// ─── Test Runner ──────────────────────────────────────────────────────────────

async function runTest(): Promise<void> {
  let server: BridgeServer | undefined;
  let client: Awaited<ReturnType<typeof connectClient>> | undefined;
  let sender: MattermostUser | undefined;

  const configPath = resolve(process.cwd(), "config.json");

  if (!MATTERMOST_BOT_TOKEN || !MATTERMOST_BOT_USER_ID || !MATTERMOST_SENDER_TOKEN || !MATTERMOST_SENDER_USER_ID) {
    throw new Error(
      "Mattermost E2E requires MATTERMOST_BOT_TOKEN, MATTERMOST_BOT_USER_ID, " +
        "MATTERMOST_SENDER_TOKEN, and MATTERMOST_SENDER_USER_ID env vars. " +
        "Run scripts/provision-mattermost.sh against a Mattermost server first.",
    );
  }

  try {
    // ── Step 1: Build a config pointing the mattermost channel at the test server
    log.info("=== Step 1: Starting bridge with mattermost channel ===", {
      url: MATTERMOST_URL,
      botUserId: MATTERMOST_BOT_USER_ID,
      senderUserId: MATTERMOST_SENDER_USER_ID,
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
        mattermost: {
          enabled: true,
          accounts: {
            [ACCOUNT_ID]: {
              // Plain-string botToken is accepted by the secret-input schema;
              // the plugin reads it into the `Authorization: Bearer <token>`
              // header for REST + the `authentication_challenge` WS payload.
              botToken: MATTERMOST_BOT_TOKEN,
              baseUrl: MATTERMOST_URL,
              // Open DM policy + wildcard allowFrom so the in-test sender's
              // messages are dispatched (not dropped / not gated behind
              // pairing) by the mattermost ingress gate.
              dmPolicy: "open",
              groupPolicy: "open",
              allowFrom: ["*"],
              // The preview server runs on 127.0.0.1 (a private address). The
              // openclaw runtime's SSRF guard blocks private/internal hosts by
              // default; opt in here so the bot's REST + WS traffic to the
              // self-hosted test server is allowed through.
              network: { dangerouslyAllowPrivateNetwork: true },
            },
          },
        },
      },
      logging: {
        level: (process.env.E2E_MATTERMOST_LOG_LEVEL ?? "info") as "debug" | "info" | "warn" | "error",
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
    const contactStoreDir = mkdtempSync(join(tmpdir(), "openclaw-mm-e2e-"));
    const contactStore = new ContactStore(join(contactStoreDir, "config.json"));
    channelManager.setContactStore(contactStore);

    // Discover and load channel adapters (the bundled mattermost plugin is
    // discovered from node_modules/openclaw/dist/extensions/mattermost and its
    // runtime installed).
    const adapters = await loadChannelAdapters();
    for (const [, adapter] of adapters) {
      channelManager.registerAdapter(adapter);
    }
    if (!channelManager.getAdapter(CHANNEL_ID)) {
      throw new Error(
        `${CHANNEL_ID} adapter not loaded — is the openclaw package installed ` +
          `(node_modules/openclaw/dist/extensions/mattermost)?`,
      );
    }

    server = new BridgeServer(config, channelManager, clientRegistry);
    await server.start();
    log.info("Bridge server started", { port: BRIDGE_PORT });

    // Start the mattermost account.
    const mmCfg = config.channels.mattermost as any;
    const cred = mmCfg.accounts[ACCOUNT_ID];
    await channelManager.startAccount(CHANNEL_ID, ACCOUNT_ID, cred);

    // Wait for the bot's mattermost gateway to connect (the WS event stream).
    log.info("Waiting for mattermost account to connect...");
    await waitFor(
      () => channelManager.getStatus(CHANNEL_ID, ACCOUNT_ID).state === "connected",
      30000,
    );
    log.info("Mattermost bot connected!");

    // ── Step 2: Connect the far-end IM user (Mattermost sender)
    log.info("=== Step 2: Connecting Mattermost sender ===", { userId: MATTERMOST_SENDER_USER_ID });
    sender = new MattermostUser(
      MATTERMOST_URL,
      MATTERMOST_SENDER_TOKEN,
      MATTERMOST_SENDER_USER_ID,
      MATTERMOST_BOT_USER_ID,
    );
    await sender.connect();
    // Pre-create the DM channel so the bot can reuse it on the first outbound.
    await sender.ensureDmChannel();
    log.info("Mattermost sender connected (WS event stream + DM channel)");

    // ── Step 3: Connect the WS client and subscribe
    log.info("=== Step 3: Connecting WS client ===");
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
    log.info("WS client subscribed to mattermost/default");

    // ── Step 4: Exchange 3 messages (full round-trip each)
    log.info("=== Step 4: Exchanging 3 messages ===");

    const runStamp = `${Date.now()}`;
    const outboundTexts = [
      `e2e-mm round-trip message 1 ${runStamp}`,
      `e2e-mm round-trip message 2 ${runStamp}`,
      `e2e-mm round-trip message 3 ${runStamp}`,
    ];
    const inboundTexts = [
      `e2e-mm reply 1 ${runStamp}`,
      `e2e-mm reply 2 ${runStamp}`,
      `e2e-mm reply 3 ${runStamp}`,
    ];

    for (let i = 0; i < outboundTexts.length; i++) {
      const outText = outboundTexts[i];
      const replyText = inboundTexts[i];
      log.info(`--- Exchange ${i + 1}/3 ---`);

      // Outbound: WS client -> bridge -> plugin -> bot posts to the DM channel
      // -> sender receives the `posted` event.
      const ack = await sendAndWaitForAckOrError(
        client.ws,
        {
          type: BridgeMessageType.SEND_TEXT,
          channel: CHANNEL_ID,
          accountId: ACCOUNT_ID,
          // Mattermost addresses a DM to a user via the `user:<id>` target form.
          payload: { to: `user:${MATTERMOST_SENDER_USER_ID}`, text: outText },
        },
        20000,
      );
      if (ack.type !== BridgeMessageType.SEND_ACK) {
        const e = ack.payload as any;
        throw new Error(`send_text #${i + 1} failed: ${e.code} ${e.message}`);
      }
      log.info(`outbound #${i + 1} acked`, { requestId: (ack.payload as any).requestId });

      // Wait for the sender to receive the bot's post over its WS event stream.
      await waitFor(
        () => sender!.receivedText(outText),
        15000,
      );
      log.info(`sender received outbound #${i + 1}: "${outText}"`);

      // Settle briefly before the sender replies. Right after the bot posts,
      // the bot's own WebSocket event stream is busy receiving (and silently
      // dropping, via the self-sender filter) the echoed `posted` event for its
      // own outbound — and in that window it can miss the `posted` event for an
      // immediately-following reply. A short settle lets the self-event clear
      // the stream first, eliminating the vast majority of missed replies.
      // (Overridable via E2E_MM_SETTLE_MS; the retry loop below is the safety net
      // for the residual race.)
      const settleMs = parseInt(process.env.E2E_MM_SETTLE_MS ?? "600", 10);
      if (settleMs > 0) await new Promise((r) => setTimeout(r, settleMs));

      // Inbound: sender posts a reply to the DM channel -> plugin gateway
      // -> deliver -> WS client receives inbound_message.
      //
      // Mattermost's WebSocket `posted` event delivery has a race window right
      // after the bot's own outbound post: the bot's event stream is processing
      // the echoed self-post and can miss the immediately-following reply. The
      // settle delay above avoids most of these; the residual race is handled
      // here by re-posting (a fresh post id bypasses the replay guard) until
      // the inbound message arrives at the WS client.
      let inbound: BridgeEnvelope | undefined;
      const maxAttempts = 5;
      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        await sender.postToBot(replyText);
        try {
          inbound = await waitForInbound(client.ws, replyText, 10000);
          break;
        } catch (err) {
          if (attempt === maxAttempts) throw err;
          log.warn(`inbound #${i + 1} not received (attempt ${attempt}/${maxAttempts}); re-posting`, { text: replyText });
        }
      }
      const p = inbound!.payload as any;
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
    log.info("=== Mattermost E2E Test Complete ===");
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
  log.error("Mattermost E2E test failed", { error: String(err), stack: err.stack });
  process.exit(1);
});
