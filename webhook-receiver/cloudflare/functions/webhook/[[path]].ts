/**
 * Cloudflare Pages Functions adapter.
 *
 * Pages Functions live under /functions and receive an `onRequest` context with
 * a Fetch Request + bindings (env). The KV binding is injected as `env.WH_KV`.
 *
 * Deploy:
 *   1. `cd webhook-receiver && npm install && npm run build`
 *   2. In Cloudflare: create a KV namespace and bind it to WH_KV on this Pages
 *      project (Settings > Functions > KV namespace bindings).
 *   3. Set env vars WH_TOKENS (or WH_TOKEN_*) + optional WH_SHARED_SECRET /
 *      WH_POLL_TOKEN.
 *
 * Routes (mount at the Pages domain root):
 *   POST /webhook/{token}            IM -> receiver
 *   GET  /webhook/{token}/poll       receiver -> bridge
 *   GET  /webhook/{token}/ack?id=…   ack only
 *
 * The [[path]] splat catches /webhook/<anything>; we forward the request to the
 * core unchanged.
 */

import { Receiver } from "../../src/core.js";
import { pickStoreFromEnv, loadTokenBindings } from "../../src/storage.js";
import { TokenRegistry } from "../../src/core.js";
import type { CfKvLike } from "../../src/storage.js";

interface CfEnv {
  WH_KV?: CfKvLike;
  WH_TOKENS?: string;
  WH_SHARED_SECRET?: string;
  WH_SIGNATURE_HEADER?: string;
  WH_POLL_TOKEN?: string;
}

let cached:
  | { envFingerprint: string; receiver: Receiver; tokens: TokenRegistry }
  | null = null;

function build(env: CfEnv): Receiver {
  const fingerprint = JSON.stringify({
    kv: !!env.WH_KV,
    tokens: env.WH_TOKENS ?? "",
    secret: env.WH_SHARED_SECRET ? "set" : "",
  });
  if (cached && cached.envFingerprint === fingerprint) return cached.receiver;

  const tokens = new TokenRegistry();
  // loadTokenBindings reads process.env; on CF, copy env vars into process.env
  // (Pages Functions expose them on the context env, not process.env).
  const procEnv = (globalThis as any).process?.env ?? {};
  const merged = { ...procEnv, WH_TOKENS: env.WH_TOKENS, WH_SHARED_SECRET: env.WH_SHARED_SECRET };
  (globalThis as any).process = (globalThis as any).process ?? { env: {} };
  (globalThis as any).process.env = { ...(globalThis as any).process.env, ...merged };

  for (const b of loadTokenBindings()) {
    tokens.register(b.token, { channel: b.channel, accountId: b.accountId });
  }
  const store = pickStoreFromEnv(env.WH_KV ?? null);
  const receiver = new Receiver(store, tokens, {
    sharedSecret: env.WH_SHARED_SECRET,
    signatureHeader: env.WH_SIGNATURE_HEADER ?? "x-signature",
    pollBearerToken: env.WH_POLL_TOKEN,
  });
  cached = { envFingerprint: fingerprint, receiver, tokens };
  return receiver;
}

export const onRequest: PagesFunction<CfEnv> = async (ctx) => {
  const receiver = build(ctx.env);
  return receiver.handle(ctx.request);
};
