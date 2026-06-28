# openclaw-webhook-receiver

A **standalone webhook receiver** that buffers inbound IM events for the
[openclaw-bridge](..). It deploys independently on **Vercel**, **Cloudflare
Pages/Workers**, or **any FaaS** — wherever the IM platform can post webhooks.

## Why

Some channel plugins can't hold a long-lived inbound connection back to the
bridge (their IM platform only offers *outbound* webhooks — e.g. MS Teams,
Slack outgoing webhooks, generic webhook bots). For those, the inbound path is:

```
IM platform --[POST]--> webhook receiver --[poll]--> bridge --[WS]--> your client
```

The bridge (a long-lived process) **polls** the receiver; the receiver is a
stateless-friendly buffer that any FaaS instance can serve.

```
ws client -> bridge server -> channel outbound -> IM
IM -> webhook receiver <- (poll msgs) <- bridge server -> ws client
```

## Quick start (local / self-hosted)

```bash
cd webhook-receiver
npm install
npm run build

# Register a webhook token for a channel/account (persists to wh-tokens.json)
node dist/cli.js register msteams default
# -> Registered webhook token:
#    { "token": "wK9...", "channel": "msteams", "accountId": "default" }
#    Inbound URL (give to IM platform):  http://localhost:9301/webhook/wK9...
#    Poll URL     (bridge polls this):   http://localhost:9301/webhook/wK9.../poll

# Start the server
npm start
```

Verify the round-trip:

```bash
# IM posts an event
curl -X POST http://localhost:9301/webhook/wK9... \
  -H 'content-type: application/json' \
  -d '{"text":"hello","from":{"id":"u1","name":"Alice"},"conversation":{"id":"c1"}}'

# Bridge polls it back
curl http://localhost:9301/webhook/wK9.../poll
# -> {"channel":"msteams","accountId":"default","messages":[{"id":"1",...}]}

# Bridge acks after processing (pass the last id seen as ?ack=)
curl http://localhost:9301/webhook/wK9.../poll?ack=1
```

## Deploy on Vercel

1. `cd webhook-receiver && npm install && npm run build`
2. In Vercel project settings, set:
   - `WH_TOKENS` = `[{"token":"<random>","channel":"msteams","accountId":"default"}]`
   - A **remote store** (in-memory is unsafe across Vercel's per-request instances):
     - `UPSTASH_REDIS_REST_URL` + `UPSTASH_REDIS_REST_TOKEN`, **or**
     - `KV_REST_API_URL` + `KV_REST_API_TOKEN` (Vercel KV)
3. The function in `vercel/api/webhook.ts` exposes `/api/webhook/...`. Register
   `https://<project>.vercel.app/api/webhook/<token>` with the IM platform.
4. Point the bridge's webhook channel at `https://<project>.vercel.app/api/webhook/<token>/poll`.

## Deploy on Cloudflare Pages

1. Build as above.
2. Create a **KV namespace** and bind it to `WH_KV` on the Pages project
   (Settings → Functions → KV namespace bindings).
3. Set env `WH_TOKENS`, optional `WH_SHARED_SECRET` / `WH_POLL_TOKEN`.
4. `cloudflare/functions/webhook/[[path]].ts` exposes `/webhook/...` at the
   Pages domain root.

## Deploy on other FaaS (Deno Deploy / Workers / Netlify v2)

Import the generic handler and export it the way your runtime expects:

```ts
import { handler } from "openclaw-webhook-receiver/faas/handler";
export default handler;
```

## Configuration (env)

| Env | Meaning |
|-----|---------|
| `WH_TOKENS` | JSON `[{token,channel,accountId},...]` token bindings |
| `WH_TOKEN_<CHANNEL>_<ACCOUNT>` | shorthand binding (value = token) |
| `WH_SHARED_SECRET` | optional HMAC secret to verify inbound signatures |
| `WH_SIGNATURE_HEADER` | header the platform signs (default `x-signature`) |
| `WH_SIGNATURE_ENCODING` | `hex` (default) or `base64` |
| `WH_POLL_TOKEN` | optional bearer the bridge must present when polling |
| `WH_MAX_POLL_BATCH` | max messages per poll (default 50) |
| `WH_MAX_AGE_MS` | drop messages older than this (default 24h) |
| `UPSTASH_REDIS_REST_URL`/`_TOKEN` | Upstash Redis store (multi-instance safe) |
| `KV_REST_API_URL`/`_TOKEN` | Vercel KV store |
| `WH_KV` (binding) | Cloudflare KV namespace binding |

## Storage backends

All implement the same `MessageStore` contract (`src/core.ts`):

- **In-memory** (default) — volatile, single-instance. Local dev only.
- **Upstash Redis** — REST-based, works on every platform. Recommended for FaaS.
- **Vercel KV** — Upstash-compatible REST; selected by `KV_REST_API_URL`.
- **Cloudflare KV** — binding-based; selected when a `WH_KV` binding is present.

Selected automatically by `pickStoreFromEnv()`.

## API

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/webhook/{token}` | IM pushes an inbound event |
| `GET` | `/webhook/{token}/poll` | bridge drains buffered messages (`?ack=<lastId>&limit=N`) |
| `GET`/`POST` | `/webhook/{token}/ack?id=<lastId>` | ack without draining |
| `GET` | `/healthz` | liveness |
| `GET` | `/` | banner |

### Poll response

```json
{
  "channel": "msteams",
  "accountId": "default",
  "messages": [ { "id": "1", "channel": "msteams", "accountId": "default",
    "messageId": "...", "chatId": "c1", "senderId": "u1", "senderName": "Alice",
    "msgType": "text", "text": "hello", "timestamp": 1719500000000,
    "replyTo": "msteams:c1", "raw": { ... } } ]
}
```

The bridge emits each `messages[]` entry to WS clients as an `inbound_message`.
After processing a batch, it polls again with `?ack=<lastId>` to trim consumed
messages (idempotent).

## Architecture

```
src/core.ts        portable core (Request/Response, store, auth, routing)
src/storage.ts     memory + Upstash + Vercel KV + Cloudflare KV backends
src/build.ts       build a Receiver from env
src/server.ts      standalone Node http server (dev / self-hosted)
src/cli.ts         token management CLI
src/token-store.ts file-persisted token registry (server only)
vercel/api/        Vercel function adapter
cloudflare/functions/  Cloudflare Pages function adapter
faas/handler.ts    generic Fetch-handler adapter (Deno/Workers/Netlify)
```

The core imports **nothing** platform-specific — it speaks the Web Fetch
`Request`/`Response` shapes only, so one codebase runs everywhere.
