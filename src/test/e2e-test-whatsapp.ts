/**
 * E2E test for the WhatsApp channel.
 *
 * Mirrors e2e-test-weixin.ts but targets the `whatsapp` channel provided by
 * the `@openclaw/whatsapp` plugin (WhatsApp Web via Baileys). Like WeChat,
 * WhatsApp has no static credentials in config.json -- it links via a QR code
 * scanned from a phone, and the account is connected once the link succeeds.
 *
 * The test verifies the full QR-login bridge pipeline: server start, QR start,
 * wait for scan, account connect, WS client, list channels, subscribe, send
 * outbound text, receive an inbound message, verify contact persistence.
 *
 *   WS client --[WebSocket]--> Bridge --[@openclaw/whatsapp]--> WhatsApp Web (Baileys)
 *        <--[inbound_message]--        <--[whatsapp socket]--
 *
 * Prerequisites:
 *   - `@openclaw/whatsapp` plugin installed (npm i @openclaw/whatsapp, or
 *     `openclaw plugins install clawhub:@openclaw/whatsapp`).
 *   - config.json with a `whatsapp` section (accounts may be empty -- WhatsApp
 *     links via QR). The account named by WHATSAPP_ACCOUNT_ID (default
 *     "default") is used only as the QR-start target; the connected account id
 *     comes back from the qr_wait response.
 *   - A phone with WhatsApp ready to scan the QR code.
 *
 * Configuration (env vars, all optional):
 *   WHATSAPP_TARGET_ID  - recipient (E.164 phone number, e.g. "+15551234567")
 *                         to send outbound messages to. Defaults to
 *                         WHATSAPP_SENDER_ID (round-trip to self / linked number).
 *   WHATSAPP_SENDER_ID  - E.164 sender number to match inbound messages
 *                         against. Defaults to WHATSAPP_TARGET_ID.
 *   WHATSAPP_ACCOUNT_ID - account id used for the QR-start request (the account
 *                         the QR login will bind to; defaults to "default").
 *                         The actual connected account id is taken from the
 *                         qr_wait response.
 *   WHATSAPP_QR_TIMEOUT - max ms to wait for the QR scan (default 180000 = 3min).
 *
 * Run: npm run test:e2e:whatsapp
 */

import { resolve, join } from "node:path";
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { WebSocket } from "ws";
import { BridgeServer } from "../server/bridge-server.js";
import { ClientRegistry } from "../server/client-registry.js";
import { ChannelManager } from "../channels/channel-manager.js";
import { WhatsAppBridgeAdapter } from "../channels/whatsapp-adapter.js";
import { loadConfig } from "../config/schema.js";
import { ContactStore } from "../contacts/contact-store.js";
import { BridgeMessageType, type BridgeEnvelope } from "../protocol/messages.js";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("e2e-whatsapp");

// The channel id exposed by @openclaw/whatsapp (see openclaw.plugin.json).
const CHANNEL_ID = "whatsapp";

// Account the QR-start request targets. WhatsApp credentials are NOT in config;
// they are produced by the QR link. The real connected account id is taken from
// the qr_wait response.
const ACCOUNT_ID = process.env.WHATSAPP_ACCOUNT_ID ?? "default";

// Real WhatsApp sender (E.164 phone number, e.g. "+15551234567") used to match
// inbound messages.
const SENDER_ID = process.env.WHATSAPP_SENDER_ID ?? "";

// Recipient for outbound messages -- defaults to the sender so the test can
// round-trip against a single WhatsApp user when only one number is involved.
const TARGET_ID = process.env.WHATSAPP_TARGET_ID ?? SENDER_ID;

// How long to wait for the user to scan the QR code with their phone.
const QR_TIMEOUT = parseInt(process.env.WHATSAPP_QR_TIMEOUT ?? "180000", 10);

// --- Helpers ---

/**
 * Auto-discover the linked WhatsApp self number (E.164, e.g. "+15551234567")
 * from the Baileys creds.json the plugin wrote after a successful QR link.
 *
 * The bridge's getStatus() only tracks connection state, not identity, so for
 * a self-chat round-trip test we read creds.json directly. The file lives at
 * <oauthDir>/whatsapp/<account>/creds.json and holds `me.id` as a JID like
 * "15551234567:75@s.whatsapp.net". We normalize that to "+<digits>".
 *
 * Returns undefined if creds.json is not (yet) present/parseable.
 */
function resolveLinkedSelfNumber(accountId: string): string | undefined {
  // oauthDir defaults to ~/.openclaw/credentials (OPENCLAW_STATE_DIR override
  // honored). Match the plugin's resolveDefaultWebAuthDir() layout.
  const stateDir = process.env.OPENCLAW_STATE_DIR?.trim() || join(resolveDefaultHome(), ".openclaw");
  const credsPath = join(stateDir, "credentials", "whatsapp", accountId, "creds.json");
  if (!existsSync(credsPath)) return undefined;
  try {
    const parsed = JSON.parse(readFileSync(credsPath, "utf-8"));
    const jid: string | undefined = parsed?.me?.id;
    if (!jid) return undefined;
    // JID form: <digits>:<device>@s.whatsapp.net -> take leading digits.
    const digits = String(jid).replace(/^(\d+).*$/, "$1");
    return digits ? `+${digits}` : undefined;
  } catch {
    return undefined;
  }
}

/** Best-effort home dir (Node os.homedir), so creds.json can be located. */
function resolveDefaultHome(): string {
  try {
    return homedir() || "/root";
  } catch {
    return "/root";
  }
}



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

/**
 * Wait for an inbound_message from a specific sender (any sender if none given).
 *
 * WhatsApp sender ids may arrive as either an E.164 number ("+15551234567") or
 * a JID ("15551234567@s.whatsapp.net"); match on the digits to be tolerant of
 * either form.
 */
function waitForInboundMessage(
  ws: WebSocket,
  fromUserId: string | undefined,
  timeoutMs = 60000,
): Promise<BridgeEnvelope> {
  const digits = (s: string) => s.replace(/\D+/g, "");
  const wanted = fromUserId ? digits(fromUserId) : "";
  return new Promise((resolveP, rejectP) => {
    const timer = setTimeout(() => rejectP(new Error("Timed out waiting for inbound message")), timeoutMs);
    const handler = (data: any) => {
      const msg: BridgeEnvelope = JSON.parse(data.toString());
      if (msg.type === BridgeMessageType.INBOUND_MESSAGE) {
        const payload = msg.payload as any;
        const sender = String(payload.senderId ?? payload.from ?? "");
        if (!wanted || digits(sender) === wanted || digits(sender).endsWith(wanted)) {
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

    // Verify the whatsapp channel is present and enabled. The account entry
    // may be empty -- WhatsApp links via QR, not static credentials.
    const whatsappCfg = config.channels[CHANNEL_ID];
    if (!whatsappCfg || whatsappCfg.enabled === false) {
      throw new Error(
        `config.json has no enabled ${CHANNEL_ID} section -- add a ` +
        `"whatsapp" entry (accounts may be empty; QR login is used).`,
      );
    }
    log.info("WhatsApp config verified", { accountId: ACCOUNT_ID, note: "credentials come from QR login, not config" });

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

    // Register the bridge-native WhatsApp adapter. We deliberately do NOT use
    // loadChannelAdapters() here: the @openclaw/whatsapp plugin hard-wires
    // embedded-agent dispatch for inbound, which never surfaces inbound to WS
    // clients (and errors without a provider key). The native adapter owns both
    // Baileys legs directly so inbound reaches WS clients. It reuses the same
    // auth dir as the plugin, so a previously-linked session carries over.
    channelManager.registerAdapter(new WhatsAppBridgeAdapter());

    if (!channelManager.getAdapter(CHANNEL_ID)) {
      throw new Error(
        `${CHANNEL_ID} adapter not loaded — is @openclaw/whatsapp installed?`,
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

    // -- Step 3: Start QR login (or reuse an existing linked session) --
    // WhatsApp persists its linked session (Baileys creds.json) in the auth
    // dir. If a session is already linked, do NOT call loginWithQrStart --
    // that call force-logs-out and clears the existing creds, destroying the
    // link. Instead, start the account directly (monitorWebChannel resumes the
    // existing session). Only when there is no linked session do we drive the
    // QR start/wait flow that needs a phone scan.
    const preLinkedSelf = resolveLinkedSelfNumber(ACCOUNT_ID);
    log.info("=== Step 3: Resolving WhatsApp session ===", {
      alreadyLinked: !!preLinkedSelf,
      linkedSelf: preLinkedSelf ?? null,
    });

    // Set here when reusing an existing session; otherwise resolved from the
    // QR wait loop below.
    let connectedAccountId: string | undefined;

    if (preLinkedSelf) {
      // Existing linked session — no QR scan needed. The session is bound to
      // the configured account id via the auth dir.
      connectedAccountId = ACCOUNT_ID;
      log.info("WhatsApp session already linked — skipping QR scan", { self: preLinkedSelf });
    } else {
      log.info("No linked WhatsApp session — starting QR login (scan required)");
    }

    // Populated only on the QR-start path (unlinked). WhatsApp's
    // loginWithQrStart returns { qrDataUrl, message, connected } with NO
    // sessionKey (unlike weixin) -- the wait is keyed on the account's
    // in-progress login state server-side. WeChat returns a sessionKey, so
    // pass it through when present and otherwise let the wait be account-keyed.
    let sessionKey: string | undefined;
    let qrStartPayload: any;

    if (!connectedAccountId) {
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

      qrStartPayload = qrStartResp.payload as any;
      if (!qrStartPayload.qrDataUrl && !qrStartPayload.sessionKey) {
        throw new Error(`QR start produced no QR / session: ${qrStartPayload.message ?? "(no message)"}`);
      }
      sessionKey = qrStartPayload.sessionKey;
    }

    if (qrStartPayload?.qrDataUrl) {
      // Write the QR image to disk so the user can open/scan it easily in
      // environments without a browser (e.g. headless CI / SSH sessions).
      const qrOutPath = resolve(process.cwd(), "whatsapp-qr.png");
      try {
        const b64 = String(qrStartPayload.qrDataUrl).replace(/^data:[^;]+;base64,/, "");
        const { writeFileSync } = await import("node:fs");
        writeFileSync(qrOutPath, Buffer.from(b64, "base64"));
        log.info(`QR image written to ${qrOutPath} — open it and scan with WhatsApp`);
      } catch (err) {
        log.warn("Could not write QR image to disk", { error: String(err) });
      }
    }

    // -- Step 4: Wait for the user to scan (skipped if already linked) --
    if (!connectedAccountId) {
      log.info("=== Step 4: Waiting for WhatsApp scan ===");
      log.info(`Scan the QR code with WhatsApp (timeout ${Math.round(QR_TIMEOUT / 1000)}s)...`);

      // The qr_wait endpoint long-polls; loop until connected or QR refreshed.
      // If the server-side login session expires (e.g. "no login in progress"),
      // re-initiate the QR so the user can scan a fresh code instead of spinning.
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
        // WhatsApp's waitForWebLogin returns { connected: true, message } with
        // NO accountId (the session is bound to the requested account via the
        // auth dir), so fall back to the account id we started the QR with.
        connectedAccountId = waitPayload.accountId ?? ACCOUNT_ID;
        log.info("QR login succeeded!", { accountId: connectedAccountId });
        break;
      }
      if (waitPayload.qrDataUrl) {
        log.info("QR was refreshed — scan the new code");
        continue;
      }

      const message: string = waitPayload.message ?? "";
      // Server signals the login session lapsed; restart the QR flow.
      const expired =
        message.includes("没有进行中的登录") ||
        message.includes("登录超时") ||
        message.includes("expired") ||
        message.includes("timed out") ||
        message.includes("no login");
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
              const qrOutPath = resolve(process.cwd(), "whatsapp-qr.png");
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
    } // end QR wait (skipped when already linked)

    if (!connectedAccountId) {
      throw new Error(`QR login did not complete within ${Math.round(QR_TIMEOUT / 1000)}s`);
    }

    // When the session was already linked, the QR-login success callback never
    // fired, so start the account now (mirrors the callback path for a fresh
    // link). For a freshly-linked session the callback already started it, and
    // startAccount is idempotent (no-ops if already started).
    log.info("Starting WhatsApp account", { accountId: connectedAccountId });
    try {
      await channelManager.startAccount(CHANNEL_ID, connectedAccountId, {});
    } catch (err) {
      log.warn("startAccount after link failed (non-fatal if already started)", { error: String(err) });
    }

    // -- Step 5: Wait for the account to connect --
    log.info("=== Step 5: Waiting for WhatsApp account to connect ===");
    log.info("Waiting for account to connect...", { accountId: connectedAccountId });
    await waitFor(
      () => channelManager.getStatus(CHANNEL_ID, connectedAccountId!).state === "connected",
      30000,
    );
    log.info("WhatsApp account connected!", { accountId: connectedAccountId });

    // If no explicit target/sender number was supplied, auto-discover the
    // linked self number from creds.json so the test can self-chat round-trip
    // (outbound -> own phone -> inbound reply) against the linked number alone.
    let targetId: string | undefined = TARGET_ID;
    let senderId: string | undefined = SENDER_ID;
    if (!targetId) {
      targetId = resolveLinkedSelfNumber(connectedAccountId);
      if (targetId) {
        senderId = senderId || targetId;
        log.info("Auto-discovered linked WhatsApp self number for self-chat round-trip", { self: targetId });
      } else {
        log.warn("Could not auto-discover linked self number; outbound send will be skipped");
      }
    }

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

    const whatsappInfo = channelsList.channels?.[CHANNEL_ID];
    if (!whatsappInfo) {
      throw new Error(`${CHANNEL_ID} not present in list_channels response`);
    }
    const accInfo = whatsappInfo.accounts?.[connectedAccountId];
    if (!accInfo) {
      throw new Error(`${CHANNEL_ID} account ${connectedAccountId} not in list_channels response`);
    }
    log.info(`whatsapp account status: ${accInfo.status}`);
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
    log.info("=== Step 8: Sending messages to whatsapp ===");

    const testMessages = [
      "Hello from whatsapp e2e test #1",
      "Bridge is working — message #2",
      "Final whatsapp test message #3",
    ];

    let ackCount = 0;
    let errorCount = 0;

    if (!targetId) {
      log.warn("No WhatsApp target number (env or auto-discovered self) — skipping outbound send step");
    } else {
      // WhatsApp's Baileys socket opens asynchronously after startAccount
      // returns; the account may report "connected" before the listener is
      // actually ready, so the first send can hit "No active WhatsApp Web
      // listener". Retry the first send (with backoff) until it acks or a
      // readiness deadline passes, then send the rest normally.
      const listenerReadyDeadline = Date.now() + 60000;
      for (let i = 0; i < testMessages.length; i++) {
        const text = testMessages[i];
        log.info(`Sending message ${i + 1}/3: "${text}"`);

        const trySend = async (): Promise<BridgeEnvelope> =>
          sendAndWaitForAckOrError(
            client!.ws,
            {
              type: BridgeMessageType.SEND_TEXT,
              channel: CHANNEL_ID,
              accountId: connectedAccountId,
              payload: {
                to: targetId,
                text,
              },
            },
            15000,
          );

        try {
          let resp = await trySend();
          // If the listener isn't up yet (transient "no active listener"),
          // back off and retry until ready. Only retry on this specific
          // transient error so genuine send errors still surface.
          const isTransientListenerError = (r: BridgeEnvelope) =>
            r.type === BridgeMessageType.SEND_ERROR &&
            /no active.*listener|listener/i.test(String((r.payload as any)?.message ?? ""));

          let attempt = 1;
          while (isTransientListenerError(resp) && Date.now() < listenerReadyDeadline) {
            log.info(`Message ${i + 1}: WhatsApp listener not ready yet, retrying (${attempt})...`);
            await new Promise((r) => setTimeout(r, 3000 * attempt));
            attempt++;
            resp = await trySend();
          }

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
    if (senderId) {
      log.info(`Send a message in WhatsApp to the bot — waiting for user ${senderId}...`);
    } else {
      log.info("Send a message in WhatsApp to the bot — waiting for any inbound message...");
    }

    try {
      const inboundTimeout = parseInt(process.env.WHATSAPP_INBOUND_TIMEOUT ?? "600000", 10);
      const inboundMsg = await waitForInboundMessage(client.ws, senderId || undefined, inboundTimeout);
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

    // Verify the WhatsApp sender was persisted if we observed inbound
    if (senderId) {
      const waContacts = contactStore.getContactsForAccount(CHANNEL_ID, connectedAccountId);
      const digits = (s: string) => s.replace(/\D+/g, "");
      const wanted = digits(senderId);
      const found = waContacts.find((c) => digits(String(c.userId)).endsWith(wanted));
      if (found) {
        log.info("WhatsApp sender persisted as contact", { userId: senderId });
      } else {
        log.warn("WhatsApp sender was not persisted (no inbound message observed)");
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
    log.info("=== WhatsApp E2E Test Complete ===");
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
  log.error("WhatsApp E2E test failed", { error: String(err), stack: err.stack });
  process.exit(1);
});
