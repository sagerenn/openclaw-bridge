# OpenClaw Bridge

Universal WebSocket bridge server that bridges WS clients to multiple backend IM channels via openclaw channel plugins.

```
Client ──[WebSocket]──▶ Bridge Server ──[ChannelPlugin API]──▶ Backend Channels
```

**Zero plugin-specific code.** The bridge works with **any** openclaw channel plugin through the standard `ChannelPlugin` interface. Install a plugin, add its credentials to `config.json`, and it just works.

## Features

- **Universal** — works with all openclaw channel plugins (liangzimixin, weixin, and any future plugins)
- **Multi-channel** — bridge to multiple IM backends simultaneously
- **Multi-account** — multiple accounts per channel (e.g., two WeChat bots on one bridge)
- **Dynamic discovery** — plugins are auto-discovered from `node_modules` via `openclaw.plugin.json` manifests
- **Subscription model** — WS clients subscribe to specific channel accounts and only receive relevant messages
- **Sender filtering** — optionally filter inbound messages by sender ID
- **Envelope protocol** — structured JSON envelope protocol with correlation IDs for request/response matching
- **Contact persistence** — automatically records user IDs from inbound messages to `contacts.json`
- **Online notifications** — sends "🟢 Bridge is back online" to all known contacts on restart
- **QR code login** — HTTP API and WS protocol for plugins that support QR auth (e.g. WeChat)

## Quick Start

### 1. Install

```bash
git clone <repo-url> openclaw-bridge && cd openclaw-bridge
npm install
```

### 2. Install channel plugins

```bash
# Install the channels you need
npm install liangzimixin
npm install @tencent-weixin/openclaw-weixin
npm install @larksuite/openclaw-lark
```

> **Note:** `@larksuite/openclaw-lark` ships CJS files that use `import.meta`
> (ESM-only), which breaks loading under Node's syntax-based module detection.
> The bridge auto-patches this on `postinstall` and again at startup, so no
> manual steps are needed. If you install the plugin directly, run
> `npm run patch:lark` (or just restart the bridge).


### 3. Configure

Copy the example config and fill in your credentials:

```bash
cp config.example.json config.json
```

Edit `config.json`:

```json
{
  "server": {
    "port": 9300,
    "path": "/bridge"
  },
  "channels": {
    "liangzimixin": {
      "enabled": true,
      "accounts": {
        "default": {
          "appId": "YOUR_APP_ID",
          "appSecret": "YOUR_APP_SECRET"
        }
      }
    }
  },
  "logging": {
    "level": "info"
  }
}
```

> **Note:** `config.json` is gitignored — your credentials stay local.

### 4. Build & Run

```bash
npm run build
npm start
```

The server starts on `ws://0.0.0.0:9300/bridge` by default.

### 5. Connect a client

```javascript
import WebSocket from "ws";

const ws = new WebSocket("ws://localhost:9300/bridge");

ws.on("message", (data) => {
  const msg = JSON.parse(data.toString());
  console.log(msg.type, msg.payload);
});

ws.on("open", () => {
  // List available channels
  ws.send(JSON.stringify({
    v: 1, id: "1", type: "list_channels", channel: "*", payload: {}
  }));

  // Subscribe to a channel account
  ws.send(JSON.stringify({
    v: 1, id: "2", type: "subscribe", channel: "liangzimixin",
    payload: { channel: "liangzimixin", accountId: "default" }
  }));

  // Send a text message
  ws.send(JSON.stringify({
    v: 1, id: "3", type: "send_text", channel: "liangzimixin",
    accountId: "default",
    payload: { to: "recipient-user-id", text: "Hello from the bridge!" }
  }));
});
```

## Configuration Reference

### `config.json` structure

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `server.host` | string | `"0.0.0.0"` | HTTP server bind address |
| `server.port` | number | `9300` | HTTP server port |
| `server.path` | string | `"/bridge"` | WebSocket upgrade path |
| `server.maxClients` | number | `100` | Maximum concurrent WS clients |
| `server.clientHeartbeatMs` | number | `30000` | Heartbeat ping interval (ms) |
| `server.maxMessageSize` | number | `10485760` | Max WS message size (10 MB) |
| `server.asyncApiSpecUrl` | string | `"/spec/asyncapi.json"` | URL advertised for the live AsyncAPI spec (WebSocket API) |
| `server.openApiSpecUrl` | string | `"/spec/openapi.json"` | URL advertised for the live OpenAPI spec (HTTP API) |
| `proxy` | string \| object | — | Global outbound proxy for all channel-plugin traffic (see [Proxy support](#proxy-support)) |
| `channels` | object | `{}` | Channel configurations (see below) |
| `logging.level` | string | `"info"` | Log level: `debug`, `info`, `warn`, `error` |
| `logging.dir` | string | — | Log output directory (not yet implemented) |

### Channel configuration

Each channel is keyed by its channel ID (matching the plugin's `openclaw.plugin.json`). The bridge is fully generic — it doesn't prescribe the account credential schema. Whatever you put in `accounts.<id>` is passed directly to the plugin's `gateway.startAccount()`.

```json
{
  "channels": {
    "<channel-id>": {
      "enabled": true,
      "accounts": {
        "<account-id>": {
          // Plugin-specific credentials — passed through to the plugin
          "proxy": "socks5://user:pass@host:1080"  // optional account-level override
        }
      },
      "transport": {
        // Optional transport-level overrides merged into each account config
      },
      "proxy": "http://proxy:8080"  // optional channel-level proxy override
    }
  }
}
```

## WebSocket Protocol

All messages use a JSON envelope format:

```typescript
{
  v: 1,                          // Protocol version
  id: "br-xxx-1",                // Correlation ID (echoed in responses)
  type: "send_text",             // Message type
  channel: "liangzimixin",       // Channel ID
  accountId: "default",          // Account ID (optional, defaults to "default")
  payload: { ... },              // Type-specific payload
  ts: 1719400000000              // Server timestamp (set on inbound messages)
}
```

### Client → Server

| Type | Payload | Response |
|------|---------|----------|
| `send_text` | `{ to, text, replyToMessageId?, contextToken? }` | `send_ack` or `send_error` |
| `send_media` | `{ to, mediaUrl, text?, mediaType?, contextToken? }` | `send_ack` or `send_error` |
| `send_typing` | `{ to, typing }` | — |
| `subscribe` | `{ channel, accountId?, filter?: { fromUserIds? } }` | `channel_status` |
| `unsubscribe` | `{ channel, accountId? }` | — |
| `list_channels` | `{ verbose? }` | `channels_list` |
| `qr_start` | `{ accountId?, force? }` | `qr_result` |
| `qr_wait` | `{ accountId?, sessionKey?, timeoutMs? }` | `qr_result` |
| `ping` | `{}` | `pong` |

### Server → Client

| Type | Payload | Trigger |
|------|---------|---------|
| `welcome` | `{ version, channels }` | On connection |
| `inbound_message` | `{ messageId, chatId, senderId, senderName?, msgType, text, timestamp, mediaUrl?, ... }` | Inbound message from backend |
| `channel_status` | `{ status, detail?, error? }` | Status change or subscribe response |
| `channels_list` | `{ channels: { [id]: { label, accounts: { [id]: { status, detail?, error? } } } } }` | Response to `list_channels` |
| `send_ack` | `{ requestId, messageId? }` | Successful outbound send |
| `send_error` | `{ requestId, code, message }` | Failed outbound send |
| `qr_result` | `{ connected?, qrDataUrl?, message, sessionKey?, accountId? }` | QR login start or wait result |
| `pong` | `{}` | Response to `ping` |

### Subscription model

Clients must subscribe to a `channel:accountId` pair to receive inbound messages. Subscriptions support optional sender filtering:

```json
{
  "type": "subscribe",
  "channel": "liangzimixin",
  "payload": {
    "channel": "liangzimixin",
    "accountId": "default",
    "filter": { "fromUserIds": ["user-123", "user-456"] }
  }
}
```

Multiple clients can subscribe to the same channel account. Each client only receives messages matching its filters.

## CLI Usage

```bash
# Build
npm run build

# Run with default config (./config.json)
npm start

# Run with custom config path
node dist/server.js --config /path/to/config.json

# Watch mode (development)
npm run dev

# Run E2E test (requires config.json with valid credentials)
npm run test:e2e

# Feishu/Lark E2E test (requires a `feishu` section in config.json)
FEISHU_TARGET_ID=<open_id_or_chat_id> npm run test:e2e:feishu

# QQ Bot E2E test (requires a `qqbot` section in config.json)
QQBOT_TARGET_ID=qqbot:c2c:<user_openid> npm run test:e2e:qqbot

# WeChat E2E test (requires an `openclaw-weixin` section; logs in via QR scan)
WEIXIN_SENDER_ID=<user_id>@im.wechat npm run test:e2e:weixin

# IRC E2E test (credential-free — runs against a real Ergo IRC daemon)
IRC_HOST=127.0.0.1 IRC_PORT=6667 npm run test:e2e:irc

# Mattermost E2E test (runs against a real Mattermost server; provision
# accounts first with scripts/provision-mattermost.sh)
MATTERMOST_BOT_TOKEN=... MATTERMOST_BOT_USER_ID=... \
MATTERMOST_SENDER_TOKEN=... MATTERMOST_SENDER_USER_ID=... \
npm run test:e2e:mattermost

# Spec URL E2E test (no channel credentials needed — verifies the live
# AsyncAPI/OpenAPI docs and the welcome-envelope spec URLs)
BRIDGE_CONFIG_PATH=./config.json npm run test:e2e:spec
```

The IRC and Mattermost E2E suites are **fully credential-free and self-hosted**:
each spins up a real IM server (an Ergo IRC daemon / the `mattermost/mattermost-preview`
container) and exchanges 3 messages (3 outbound + 3 inbound) round-trip through
the complete bridge pipeline. They run automatically in CI (see the `e2e-irc`
and `e2e-mattermost` jobs in `.github/workflows/ci.yml`); locally, point them at
any reachable IRC / Mattermost server. The other channel suites (feishu / qqbot /
weixin / generic) require live channel credentials and real inbound traffic, so
they are not automated in CI.

## Project Structure

```
src/
├── server.ts                      # Entry point — starts server, loads plugins
├── config/
│   └── schema.ts                  # Config types, defaults, and loader
├── protocol/
│   └── messages.ts                # WS protocol envelope types and helpers
├── channels/
│   ├── channel-adapter.ts         # ChannelAdapter interface (generic contract + QR login)
│   ├── channel-manager.ts         # Plugin discovery + adapter lifecycle management
│   ├── openclaw-adapter.ts        # OpenClawChannelAdapter — drives any plugin generically
│   ├── plugin-loader.ts           # Dynamic plugin loading with CJS-ESM interop
│   └── runtime-shim.ts            # Minimal openclaw runtime shim (no full framework)
├── contacts/
│   └── contact-store.ts           # Contact persistence — records user IDs, sends online notifications
├── server/
│   ├── bridge-server.ts           # Core WS+HTTP server — message routing, client management, QR + spec API
│   ├── client-connection.ts       # Single WS client with subscriptions and filters
│   ├── client-registry.ts         # Client tracking and broadcast routing
│   ├── http-routes.ts             # Single source of truth for HTTP routes (shared with the OpenAPI spec)
│   └── spec-generator.ts          # Live AsyncAPI/OpenAPI spec generators (derived from the protocol)
├── util/
│   ├── logger.ts                  # Structured logger
│   └── proxy-setup.ts             # HTTP/SOCKS5 proxy for plugin fetch + WebSocket traffic
└── test/
    ├── e2e-test.ts                # End-to-end test (generic, any channel)
    ├── e2e-test-feishu.ts         # End-to-end test for the Feishu/Lark channel
    ├── e2e-test-qqbot.ts          # End-to-end test for the QQ Bot channel
    ├── e2e-test-weixin.ts         # End-to-end test for the WeChat channel (QR login)
    ├── e2e-test-irc.ts            # E2E test for the IRC channel (real Ergo server, credential-free)
    ├── e2e-test-mattermost.ts     # E2E test for the Mattermost channel (real server, credential-free)
    └── e2e-test-spec.ts           # E2E test for the live AsyncAPI/OpenAPI spec URLs

scripts/
└── provision-mattermost.sh        # Bootstraps an admin + bot + sender on a Mattermost server for E2E
```

## HTTP API

The bridge exposes HTTP endpoints for plugin operations alongside the WebSocket server.

> **Specs are generated live.** The WebSocket API is documented as an AsyncAPI spec and the HTTP API as an OpenAPI spec, both generated **on every request** from the bridge's own protocol definitions (the message-type enum and the route table). Adding or changing an API updates the spec automatically — there's nothing to update twice. See [API Specifications](#api-specifications).



### QR Code Login

For plugins that support QR code authentication (e.g., WeChat), the bridge provides HTTP endpoints:

#### `GET /plugin/:channelId/:accountId/qr`

Starts a QR login flow and returns an HTML page with the QR code image. The page auto-polls for login status.

Query params:
- `force=true` — Force a new QR even if one is already active

Open in a browser to scan the QR code with your phone.

#### `GET /plugin/:channelId/:accountId/qr/json`

Starts a QR login flow and returns the result as JSON (for programmatic use).

Response:
```json
{
  "qrDataUrl": "data:image/png;base64,...",
  "message": "Scan QR code to login",
  "sessionKey": "abc-123"
}
```

#### `GET /spec/asyncapi.json`

Returns the AsyncAPI 2.6.0 specification describing the WebSocket API. Generated live from the bridge's message-type enum, so it always reflects the current protocol.

#### `GET /spec/openapi.json`

Returns the OpenAPI 3.1.0 specification describing the HTTP API. Generated live from the bridge's route table, so it always reflects the current endpoints.

#### `GET /plugin/:channelId/:accountId/qr/status`

### Example: WeChat QR Login

```bash
# 1. Start QR login (returns JSON with QR data URL and session key)
curl http://localhost:9300/plugin/openclaw-weixin/default/qr/json

# 2. Or open in browser for a friendly scan page
open http://localhost:9300/plugin/openclaw-weixin/default/qr

# 3. Poll for status (use sessionKey from step 1)
curl "http://localhost:9300/plugin/openclaw-weixin/default/qr/status?sessionKey=abc-123"
```

## Proxy Support

The bridge can route **all** outbound channel-plugin traffic — both HTTP `fetch()` calls (REST APIs, token refresh, media fetches, long-polls) and WebSocket/gateway connections — through an HTTP or SOCKS5 proxy. This is done generically, with no plugin-specific code: the proxy is installed at the two network chokepoints every plugin shares (undici's global fetch dispatcher, and the `ws` package's socket creation).

### Configuring a proxy

Set a global proxy at the top level, or override it per channel / per account:

```json
{
  "proxy": "socks5://user:pass@proxy.example.com:1080",
  "channels": {
    "openclaw-weixin": {
      "proxy": "http://corporate-proxy:8080",
      "accounts": {
        "default": { "proxy": "socks5://another-proxy:1080" }
      }
    }
  }
}
```

Supported schemes:
- `http://[user:pass@]host:port` — HTTP CONNECT tunnel
- `https://[user:pass@]host:port` — HTTPS proxy
- `socks5://[user:pass@]host:port` (alias `socks://`) — SOCKS5 with optional username/password auth

Precedence: **account > channel > global**. If no explicit proxy is configured anywhere, the bridge honors the standard environment variables `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY` (case-insensitive), so a shell proxy just works.

### How it works

- **HTTP fetch traffic** — an undici global dispatcher (`ProxyAgent` for http(s) proxies, `Socks5ProxyAgent` for socks5) is installed before any channel starts. undici is the engine behind Node's global `fetch()`, so every plugin's `fetch()` is transparently proxied.
- **WebSocket traffic** — every installed copy of the `ws` package is patched (idempotently, on startup) so `new WebSocket()` tunnels its connection through the proxy via a SOCKS5 or HTTP-CONNECT handshake. The patch is inert when no proxy is active, so connections behave normally otherwise.

### Limitations

undici's fetch dispatcher is **process-wide**, so the active proxy is shared across all accounts. If multiple accounts configure different proxies, the last-started account's proxy wins for fetch traffic (logged as a warning); per-account proxying is fully honored for WebSocket connections opened after the switch.

## API Specifications

The bridge serves machine-readable API specs, generated live on every request from the bridge's own code — so they never drift from the actual API:

| Endpoint | Spec | Describes |
|----------|------|-----------|
| `GET /spec/asyncapi.json` | [AsyncAPI 2.6.0](https://www.asyncapi.com/) | The WebSocket protocol (all message types and payloads) |
| `GET /spec/openapi.json` | [OpenAPI 3.1.0](https://www.openapis.org/) | The HTTP API (all routes) |

The WebSocket spec is derived from the `BridgeMessageType` enum and its descriptor maps in `src/protocol/messages.ts`; the HTTP spec is derived from the route table in `src/server/http-routes.ts`. **Adding a new message type or HTTP route is the only change needed** — it automatically appears in the served spec. The specs are also advertised in the `welcome` envelope (`asyncApiSpecUrl` / `openApiSpecUrl`), whose URLs can be overridden via `server.asyncApiSpecUrl` / `server.openApiSpecUrl`.

## Contact Persistence & Online Notifications

The bridge automatically records the sender of every inbound message to `contacts.json` (stored alongside `config.json`). This file is gitignored.

When the server restarts, it sends an online notification ("🟢 Bridge is back online") to all persisted contacts whose channel accounts are connected.

**How it works:**
1. User sends a message to the bot on any channel
2. Bridge records the user ID, channel, and account to `contacts.json`
3. On next server restart, after channels connect, the bridge sends "🟢 Bridge is back online" to each contact
4. Contacts are deduplicated by `channel:accountId:userId` key

**`contacts.json` format:**
```json
{
  "contacts": {
    "liangzimixin:default:user-abc123": {
      "userId": "user-abc123",
      "channel": "liangzimixin",
      "accountId": "default",
      "displayName": "Alice",
      "firstSeenAt": 1719400000000,
      "lastSeenAt": 1719400000000
    }
  }
}
```

> **Note:** This file is auto-managed — you don't need to edit it manually. To reset contacts, simply delete `contacts.json`.

## Adding a New Channel Plugin

No code changes needed. Just:

1. `npm install <plugin-package>`
2. Add the channel section to `config.json` with the plugin's required credentials
3. Restart the server

The bridge auto-discovers the plugin via its `openclaw.plugin.json` manifest and loads it through the standard `ChannelPlugin` interface.

## License

MIT
