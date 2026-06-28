#!/usr/bin/env node
/**
 * Standalone Node server for the webhook receiver.
 *
 * Use this for:
 *   - local development
 *   - self-hosted Docker / VPS deploys (long-lived process — in-memory store OK)
 *   - any plain Node runtime
 *
 * For serverless (Vercel functions / Cloudflare Pages functions), use the
 * thin adapters in vercel/ and cloudflare/ instead — they share the same core.
 *
 * Env:
 *   PORT                 listen port (default 9301)
 *   HOST                 bind host   (default 0.0.0.0)
 *   WH_SHARED_SECRET     optional HMAC secret for inbound POST signatures
 *   WH_POLL_TOKEN        optional bearer the bridge presents when polling
 *   WH_TOKENS            JSON [{token,channel,accountId},...]
 *   WH_TOKEN_<CHANNEL>_<ACCOUNT>   shorthand token binding
 *   UPSTASH_REDIS_REST_URL/_TOKEN  remote store (multi-instance safe)
 *   KV_REST_API_URL/_TOKEN          Vercel KV remote store
 *
 * Manage tokens at runtime (persists to wh-tokens.json beside the server):
 *   node dist/cli.js register <channel> [accountId]
 *   node dist/cli.js list
 */

import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { buildReceiver } from "./build.js";
import { TokenRegistry } from "./core.js";
import { PersistentTokenRegistry } from "./token-store.js";

const PORT = parseInt(process.env.PORT ?? "9301", 10);
const HOST = process.env.HOST ?? "0.0.0.0";

async function main(): Promise<void> {
  const tokens = new PersistentTokenRegistry();
  const { receiver } = buildReceiver();
  // Merge file-persisted tokens into the env-built registry the receiver holds.
  for (const { token, binding } of tokens.listWithBindings()) {
    receiver.registerToken(token, binding);
  }

  const server = createServer((req, res) => {
    toFetchRequest(req).then((fetchReq) =>
      receiver.handle(fetchReq).then((resp) => sendFetchResponse(resp, res)),
    ).catch((err) => {
      console.error("[webhook-receiver] error", err);
      if (!res.headersSent) {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "Internal error" }));
      }
    });
  });

  server.listen(PORT, HOST, () => {
    console.log(`[webhook-receiver] listening on http://${HOST}:${PORT}`);
    console.log(`[webhook-receiver] ${tokens.listWithBindings().length} token(s) registered`);
    if (tokens.listWithBindings().length === 0) {
      console.log("[webhook-receiver] no tokens registered — run: node dist/cli.js register <channel>");
    }
  });

  const shutdown = (sig: string) => {
    console.log(`[webhook-receiver] ${sig}, shutting down`);
    server.close(() => process.exit(0));
  };
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

/** Convert a Node IncomingMessage into a Web Fetch Request. */
async function toFetchRequest(req: IncomingMessage): Promise<Request> {
  const chunks: Buffer[] = [];
  for await (const c of req) chunks.push(c as Buffer);
  const body = Buffer.concat(chunks);
  const host = req.headers.host ?? "localhost";
  const proto = (req.headers["x-forwarded-proto"] as string) ?? "http";
  const url = `${proto}://${host}${req.url ?? "/"}`;
  const headers = new Headers();
  for (const [k, v] of Object.entries(req.headers)) {
    if (Array.isArray(v)) v.forEach((x) => headers.append(k, x));
    else if (v != null) headers.set(k, v);
  }
  const init: RequestInit = { method: req.method ?? "GET", headers };
  if (req.method !== "GET" && req.method !== "HEAD" && body.length > 0) {
    init.body = body;
  }
  return new Request(url, init);
}

/** Send a Web Fetch Response back through a Node ServerResponse. */
function sendFetchResponse(resp: Response, res: ServerResponse): void {
  const headers: Record<string, string> = {};
  resp.headers.forEach((v, k) => { headers[k] = v; });
  res.writeHead(resp.status, headers);
  // Response.body is a ReadableStream; tee it into the Node stream.
  if (resp.body) {
    const reader = resp.body.getReader();
    (async () => {
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const { done, value } = await reader.read();
        if (done) { res.end(); break; }
        res.write(value);
      }
    })();
  } else {
    res.end();
  }
}

void main();
