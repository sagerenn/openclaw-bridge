/**
 * Build a Receiver instance from environment + config. Shared by the Node dev
 * server and the CLI — the platform adapters (vercel/cloudflare) build their
 * own from their request context instead.
 */

import { Receiver, TokenRegistry, type ReceiverConfig } from "./core.js";
import { pickStoreFromEnv, loadTokenBindings } from "./storage.js";

export interface BuildResult {
  receiver: Receiver;
  tokens: TokenRegistry;
}

export function buildReceiver(config?: ReceiverConfig): BuildResult {
  const tokens = new TokenRegistry();
  for (const b of loadTokenBindings()) {
    tokens.register(b.token, { channel: b.channel, accountId: b.accountId });
  }
  const store = pickStoreFromEnv();
  const receiver = new Receiver(store, tokens, config ?? {
    sharedSecret: env("WH_SHARED_SECRET"),
    signatureHeader: env("WH_SIGNATURE_HEADER") ?? "x-signature",
    signatureEncoding: (env("WH_SIGNATURE_ENCODING") as "hex" | "base64") ?? "hex",
    pollBearerToken: env("WH_POLL_TOKEN"),
    maxPollBatch: envInt("WH_MAX_POLL_BATCH", 50),
    maxAgeMs: envInt("WH_MAX_AGE_MS", 24 * 60 * 60 * 1000),
  });
  return { receiver, tokens };
}

function env(name: string): string | undefined {
  const v = process.env[name];
  return typeof v === "string" && v.length > 0 ? v : undefined;
}

function envInt(name: string, def: number): number {
  const v = process.env[name];
  if (!v) return def;
  const n = parseInt(v, 10);
  return Number.isNaN(n) ? def : n;
}
