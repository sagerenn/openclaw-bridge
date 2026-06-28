/**
 * Vercel Serverless Function adapter.
 *
 * Vercel exposes each file under /api as a function receiving a Web Fetch
 * `Request` and returning a `Response` — exactly what the core expects. So this
 * is a one-liner: build a Receiver from env and forward.
 *
 * Deploy:
 *   1. `cd webhook-receiver && npm install && npm run build`
 *   2. Set env in Vercel: WH_TOKENS (or WH_TOKEN_*), and a remote store
 *      (UPSTASH_REDIS_REST_URL + _TOKEN, or KV_REST_API_URL + _TOKEN — in-memory
 *      is NOT safe across Vercel's per-request instances).
 *   3. Point Vercel at this repo's webhook-receiver/ as the project root, or
 *      copy vercel/api into your app's /api.
 *
 * The function is mounted at POST/GET /api/webhook/[token]/[action]? — here we
 * reconstruct the path from the raw URL so the core's /webhook/{token} routing
 * still matches regardless of how Vercel mounts it.
 */

import { buildReceiver } from "../src/build.js";

// Cache the receiver across warm invocations in the same instance.
let cached: ReturnType<typeof buildReceiver> | null = null;
function get(): ReturnType<typeof buildReceiver> {
  if (!cached) cached = buildReceiver();
  return cached;
}

export const config = { runtime: "edge" };

export default async function handler(req: Request): Promise<Response> {
  // Normalize: Vercel mounts /api/webhook, so strip a leading /api to let the
  // core's /^\/webhook\/.../ matcher work. If already /webhook, leave it.
  const url = new URL(req.url);
  let path = url.pathname;
  if (path.startsWith("/api/")) path = path.slice(4);
  if (!path.startsWith("/webhook")) path = `/webhook${path}`;
  const rewritten = new Request(new URL(path, url.origin), req);
  return get().receiver.handle(rewritten);
}
