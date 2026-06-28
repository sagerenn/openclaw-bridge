/**
 * Standalone WhatsApp inbound monitor.
 *
 * Keeps a persistent Baileys socket open (reusing the linked creds in the auth
 * dir) and logs every messages.upsert event + the normalized inbound envelope,
 * so we can confirm inbound reaches the bridge layer independent of the e2e
 * test's 120s window. Ctrl-C to stop.
 *
 * Run: node dist/test/whatsapp-inbound-monitor.js [--debug]
 */
import { useMultiFileAuthState, fetchLatestBaileysVersion, makeCacheableSignalKeyStore, makeWASocket, getContentType, isJidGroup } from "baileys";
import { join } from "node:path";
import { homedir } from "node:os";
import { mkdirSync } from "node:fs";

const debug = process.argv.includes("--debug") || !!process.env.OPENCLAW_WHATSAPP_DEBUG;
const accountId = process.env.WHATSAPP_ACCOUNT_ID ?? "default";
const stateDir = process.env.OPENCLAW_STATE_DIR?.trim() || join(homedir() || "/root", ".openclaw");
const authDir = join(stateDir, "credentials", "whatsapp", accountId);
mkdirSync(authDir, { recursive: true });

const logger = {
  level: debug ? "debug" : "warn",
  trace: () => {},
  debug: (...a: any[]) => { if (debug) console.log("[baileys:debug]", ...a); },
  info: (...a: any[]) => console.log("[baileys:info]", ...a),
  warn: (...a: any[]) => console.warn("[baileys:warn]", ...a),
  error: (...a: any[]) => console.error("[baileys:error]", ...a),
  fatal: (...a: any[]) => console.error("[baileys:fatal]", ...a),
  child: () => logger,
};

function extractText(message: any): string {
  if (!message) return "";
  const type = getContentType(message) ?? "";
  const c = message[type];
  if (type === "conversation") return String(message.conversation ?? "");
  if (type === "extendedTextMessage") return String(c?.text ?? "");
  if (type === "imageMessage" || type === "videoMessage" || type === "audioMessage")
    return String(c?.caption ?? "") || `<${type}>`;
  if (type === "documentMessage") return String(c?.caption ?? "") || "<document>";
  return type ? `<${type}>` : "";
}

// Fallback LID→PN resolver (reads Baileys' persisted mapping) — the adapter
// prefers key.remoteJidAlt; this is only reached when that is absent.
function lookupPnForLid(jid: string): string {
  if (typeof jid !== "string" || !jid.endsWith("@lid")) return jid;
  try {
    const baseUser = jid.split("@")[0]?.split(":")[0];
    if (!baseUser) return jid;
    const path = join(authDir, `lid-mapping-${baseUser}_reverse.json`);
    const fs = require("node:fs");
    if (!fs.existsSync(path)) return jid;
    const pnUser = JSON.parse(fs.readFileSync(path, "utf-8"));
    if (typeof pnUser === "string" && pnUser) return `${pnUser}@s.whatsapp.net`;
  } catch {
    // mapping not available yet — keep the LID
  }
  return jid;
}

// Mirror of the adapter's recent-outbound echo cache.
const recentOutbound = new Map<string, number>();
const OUTBOUND_TTL = 20 * 60_000;
function rememberOutbound(remoteJid: string, messageId: string) {
  if (messageId) recentOutbound.set(`${remoteJid}:${messageId}`, Date.now());
}
function isOutboundEcho(remoteJid: string, messageId: string): boolean {
  const sentAt = recentOutbound.get(`${remoteJid}:${messageId}`);
  if (sentAt == null) return false;
  if (Date.now() - sentAt > OUTBOUND_TTL) { recentOutbound.delete(`${remoteJid}:${messageId}`); return false; }
  return true;
}

async function main() {
  console.log(`[monitor] authDir=${authDir}`);
  const { state, saveCreds } = await useMultiFileAuthState(authDir);
  const { version } = await fetchLatestBaileysVersion();
  console.log(`[monitor] baileys version ${version.join(".")}`);
  console.log(`[monitor] linked self: ${state.creds?.me?.id ?? "(none — QR scan needed)"}`);

  const sock = makeWASocket({
    auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger as any) },
    version,
    printQRInTerminal: false,
    browser: ["openclaw", "cli", "1.0.0"],
    markOnlineOnConnect: false,
    logger: logger as any,
  });

  // Send a probe so we can observe the bot's own echo being dropped.
  const probe = async () => {
    try {
      const r = await sock.sendMessage(`${process.env.MONITOR_PROBE_TO ?? "85298193482"}@s.whatsapp.net`, { text: "[monitor] probe " + Date.now() });
      const id = (r as any)?.key?.id;
      const rj = (r as any)?.key?.remoteJid;
      if (id && rj) rememberOutbound(rj, id);
      console.log(`[monitor] sent probe ${id} (recorded for echo suppression)`);
    } catch (e) { console.warn("[monitor] probe failed", String((e as any)?.message ?? e)); }
  };

  sock.ev.on("creds.update", saveCreds);
  sock.ev.on("connection.update", (u: any) => {
    const { connection, qr, lastDisconnect } = u || {};
    if (qr) console.log("[monitor] QR emitted (scan to link)");
    if (connection) console.log(`[monitor] connection: ${connection}${lastDisconnect ? ` (${String(lastDisconnect?.error)})` : ""}`);
    if (connection === "open") {
      console.log(`[monitor] OPEN — self=${sock.user?.id}. Waiting for inbound messages...`);
      if (process.env.MONITOR_PROBE) setTimeout(() => probe().catch(() => {}), 4000);
    }
  });

  sock.ev.on("messages.upsert", (upsert: any) => {
    console.log(`[monitor] messages.upsert type=${upsert?.type} count=${upsert?.messages?.length}`);
    if (upsert?.type !== "notify") {
      console.log("[monitor]   (not notify; skipping)");
      return;
    }
    for (const env of upsert.messages || []) {
      const key = env?.key;
      const rawChatId: string = key?.remoteJid ?? key?.participant ?? "";
      // Mirror the adapter: prefer key.remoteJidAlt (PN) for DM LIDs.
      const resolvePn = (jid: string, alt?: string) =>
        isJidGroup(jid) || typeof jid !== "string" || !jid.endsWith("@lid")
          ? jid
          : typeof alt === "string" && alt.endsWith("@s.whatsapp.net")
            ? alt
            : lookupPnForLid(jid);
      const chatId = resolvePn(rawChatId, key?.remoteJidAlt);
      const rawSender: string = (isJidGroup(rawChatId) ? key?.participant : key?.remoteJid) ?? rawChatId;
      const senderId = resolvePn(rawSender, key?.remoteJidAlt);
      const text = extractText(env?.message);
      const ts = env?.messageTimestamp ? Number(env.messageTimestamp) * 1000 : Date.now();

      // Apply the adapter's echo rule: drop only verbatim echoes of bot sends.
      const echo = key?.fromMe && key.id && isOutboundEcho(chatId, key.id);
      console.log("[monitor] <<< INBOUND >>>");
      console.log(`  messageId : ${key?.id}`);
      console.log(`  chatId    : ${chatId}`);
      console.log(`  senderId  : ${senderId}`);
      console.log(`  fromMe    : ${!!key?.fromMe}`);
      console.log(`  verdict   : ${echo ? "DROP (echo of bot's own send)" : "DELIVER to WS clients"}`);
      console.log(`  text      : ${JSON.stringify(text)}`);
      console.log(`  ts        : ${new Date(ts).toISOString()}`);
      console.log(`  contentK  : ${getContentType(env?.message)}`);
    }
  });

  // Keep alive
  setInterval(() => {}, 1 << 30);
}

main().catch((err) => {
  console.error("[monitor] fatal", err);
  process.exit(1);
});
