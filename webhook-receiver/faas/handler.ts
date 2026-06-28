/**
 * Generic FaaS adapter — a `handler(req)` exporting a Web Fetch Response.
 *
 * Use this on any platform that gives you a Fetch-style handler (Deno Deploy,
 * Netlify Functions v2, Cloudflare Workers — not Pages, Bun, Vercel Edge) but
 * doesn't have its own dedicated adapter directory yet. Each platform's
 * entrypoint just re-exports this `handler` as its runtime expects.
 *
 *   // worker.js / netlify function / deno
 *   import { handler } from "openclaw-webhook-receiver/faas/handler";
 *   export default handler;       // Cloudflare Workers / Deno
 *   export { handler };           // re-export for netlify
 */

import { buildReceiver } from "../src/build.js";

let cached: ReturnType<typeof buildReceiver> | null = null;
function get(): ReturnType<typeof buildReceiver> {
  if (!cached) cached = buildReceiver();
  return cached;
}

export async function handler(req: Request): Promise<Response> {
  return get().receiver.handle(req);
}

export default handler;
