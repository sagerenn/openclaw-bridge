# Design Document

## Overview

OpenClaw Bridge is a universal WebSocket bridge server that connects WS clients to multiple backend IM channels through openclaw channel plugins. The key design principle: **zero plugin-specific code** — the bridge drives every plugin through the standard `ChannelPlugin` interface.

## Architecture

```
┌─────────────┐     WebSocket      ┌──────────────────────────────────────────┐
│  WS Client  │ ◄───────────────▶ │              Bridge Server               │
│  (any lang)  │   ws://host:port  │                                          │
└─────────────┘                    │  ┌─────────────┐  ┌──────────────────┐  │
                                   │  │ BridgeServer │  │  ClientRegistry  │  │
                                   │  │  (WS + HTTP) │  │  (subscriptions) │  │
                                   │  └──────┬──────┘  └────────┬─────────┘  │
                                   │         │                   │            │
                                   │         ▼                   │            │
                                   │  ┌──────────────┐           │            │
                                   │  │ChannelManager │           │            │
                                   │  │  (lifecycle)  │           │            │
                                   │  └──────┬──────┘           │            │
                                   │         │                   │            │
                                   │         ▼                   ▼            │
                                   │  ┌──────────────────────────────────┐   │
                                   │  │     OpenClawChannelAdapter        │   │
                                   │  │  (generic — drives any plugin)    │   │
                                   │  └──────────────┬───────────────────┘   │
                                   │                 │                        │
                                   └─────────────────┼────────────────────────┘
                                                     │ ChannelPlugin API
                                                     ▼
                                           ┌──────────────────┐
                                           │  Channel Plugin   │
                                           │  (gateway,        │
                                           │   outbound,       │
                                           │   config, ...)    │
                                           └────────┬─────────┘
                                                    │
                                                    ▼
                                           ┌──────────────────┐
                                           │  Backend Channel  │
                                           │  (IM platform)    │
                                           └──────────────────┘
```

## Component Design

### 1. Plugin Discovery (`channel-manager.ts` → `discoverPlugins()`)

The bridge discovers installed plugins by scanning `node_modules/` for `openclaw.plugin.json` manifests — the same mechanism openclaw uses.

**Discovery algorithm:**
1. Scan `node_modules/` top-level directories
2. For scoped packages (`@scope/name`), scan the scope subdirectory
3. For each package, check for `openclaw.plugin.json`
4. Parse the manifest to get `id` and `channels[]`
5. Resolve the entry point from `package.json` → `openclaw.runtimeExtensions[0]` or `openclaw.extensions[0]`

**Manifest format** (`openclaw.plugin.json`):
```json
{
  "id": "liangzimixin",
  "channels": ["liangzimixin"],
  "channelConfigs": { ... }
}
```

### 2. Plugin Loading (`plugin-loader.ts` → `loadChannelAdapters()`)

After discovery, each plugin is dynamically imported and its `ChannelPlugin` object is extracted. Plugins use one of two export patterns:

**Pattern 1 — Direct ChannelPlugin export:**
```javascript
// liangzimixin: exports the ChannelPlugin object directly
export default quantumImPlugin;  // { id, gateway, outbound, ... }
```

**Pattern 2 — register(api) pattern:**
```javascript
// weixin: exports an entry with register() that calls api.registerChannel()
export default { id, name, register(api) { api.registerChannel({ plugin: weixinPlugin }); } }
```

**CJS-ESM interop:** When a CJS plugin is loaded via ESM `import()`, the module structure is:
- `mod.default` → CJS `module.exports` object
- `mod.default.default` → inner ESM default export (where `register()` lives)

The `findRegisterEntry()` helper checks `mod`, `mod.default`, and `mod.default.default` for the `register()` method.

**Runtime initialization:** For Pattern 1 plugins, `register()` is also called to initialize `setPluginRuntime()` — otherwise `getPluginRuntime()` throws at runtime when the plugin tries to access the runtime.

**Plugin validation:** `isChannelPluginLike()` checks that an object has an `id` string and at least one adapter surface (`gateway`, `outbound`, `config`, `setup`, `pairing`, or `status`).

### 3. Runtime Shim (`runtime-shim.ts`)

The bridge does **not** run the full openclaw framework. Instead, it provides a minimal runtime shim that satisfies the plugin's expectations:

| Shim | Purpose |
|------|---------|
| `buildPluginApi()` | Mock `api` object passed to `register()`. Captures the `ChannelPlugin` from `api.registerChannel()`. |
| `buildPluginRuntime()` | Mock `PluginRuntime` with version `"2026.6.10"` (passes `assertHostCompatibility(">=2026.3.22")`). Provides stubs for `channel`, `subagent`, `nodes`. |
| `buildChannelShim()` | Mock `ChannelRuntimeSurface` with stubs for `reply`, `routing`, `session`, `media`, `commands`, `text`, `pairing`, `activity`, `mentions`, `reactions`, `groups`, `debounce`, `outbound`, `inbound`, `threadBindings`, `runtimeContexts`. |
| `buildGatewayContext()` | Builds the `ctx` object passed to `gateway.startAccount(ctx)` with config, account info, abort signal, and channelRuntime. |

**The critical interception point — `deliver`:**

In the real openclaw runtime, the `reply` pipeline dispatches AI-generated responses. In bridge mode, we intercept the `deliver` callback in `createReplyDispatcherWithTyping()` to route inbound messages to WS clients instead:

```
Plugin gateway receives message
  → calls createReplyDispatcherWithTyping({ deliver, ctx })
    → our shim intercepts deliver()
      → calls onDeliver() callback
        → OpenClawChannelAdapter.emitDeliverAsInbound()
          → ChannelManager message callbacks
            → BridgeServer.routeInboundMessage()
              → ClientRegistry.broadcast() to subscribed WS clients
```

**AI dispatch is skipped:** `dispatchReplyFromConfig()` and `dispatchReplyWithBufferedBlockDispatcher()` return immediately with empty counts — the bridge is a passthrough, not an AI host.

### 4. OpenClawChannelAdapter (`openclaw-adapter.ts`)

The adapter wraps a `ChannelPlugin` and drives it generically:

| Operation | Plugin API call |
|-----------|----------------|
| Start account | `plugin.gateway.startAccount(ctx)` |
| Stop account | `plugin.gateway.stopAccount(ctx)` + abort signal |
| Send text | `plugin.outbound.sendText({ cfg, to, text, accountId, ... })` |
| Send media | `plugin.outbound.sendMedia({ cfg, to, mediaUrl, ... })` |
| Send typing | `plugin.outbound.sendTyping({ cfg, to, typing, accountId })` |

**Fire-and-forget gateway start:** Some plugins (e.g., liangzimixin) block `gateway.startAccount()` until the abort signal fires — they never resolve during normal operation. The adapter calls `startAccount()` without awaiting and marks the account as "connected" immediately:

```typescript
gateway.startAccount(ctx).then(() => {
  // Normal resolution (abort signal fired)
  this.updateStatus(accountId, "connected", "Plugin gateway started");
}).catch((err) => {
  if (abortController.signal.aborted) {
    this.updateStatus(accountId, "disconnected", "Stopped");
  } else {
    this.updateStatus(accountId, "error", `Start failed: ${err}`);
  }
});
// Mark as connected optimistically
handle.connected = true;
this.updateStatus(accountId, "connected", "Plugin gateway started");
```

**Account lifecycle:**
- Each account gets its own `AbortController`
- `stop()` aborts the signal first (tells the plugin's gateway to stop), then calls `gateway.stopAccount()`
- Status transitions: `disconnected` → `reconnecting` → `connected` → `disconnected` (or `error`)

### 5. BridgeServer (`bridge-server.ts`)

The core WS server handles:
- **Connection management** — accepts WS upgrades, enforces `maxClients`, sends `welcome` on connect
- **Message routing** — dispatches client messages by type to handler methods
- **Inbound routing** — broadcasts inbound messages from channels to subscribed clients
- **Status routing** — broadcasts channel status changes to subscribed clients
- **Heartbeat** — pings clients periodically, terminates stale connections

**Startup order:** HTTP server starts **before** channel accounts. This ensures WS clients can connect immediately while backends are still connecting.

### 6. ClientConnection & ClientRegistry

**ClientConnection** tracks:
- Subscription keys (`"channel:accountId"`)
- Per-subscription sender filters (`{ fromUserIds?: string[] }`)
- `shouldReceiveMessage(key, senderId)` — checks subscription + filter

**ClientRegistry** provides:
- `broadcast(channel, accountId, envelope, senderId?)` — sends to all subscribed clients respecting filters
- `sendTo(clientId, envelope)` — sends to a specific client
- `getSubscribers(channel, accountId)` — gets all clients subscribed to a channel account

### 7. Configuration (`config/schema.ts`)

The config is fully generic — no channel-specific types. Each channel section is a dynamic map:

```typescript
interface ChannelSectionConfig {
  enabled?: boolean;
  accounts: Record<string, Record<string, unknown>>;  // arbitrary credentials
  transport?: Record<string, unknown>;                  // optional overrides
}
```

Account credentials are passed through to the plugin's `gateway.startAccount()` without validation by the bridge. The `transport` field is merged into each account config — useful for shared settings like custom API URLs.

## Message Flow

### Outbound (Client → Backend)

```
1. Client sends: { type: "send_text", channel: "liangzimixin", accountId: "default", payload: { to, text } }
2. BridgeServer.handleSendText()
3. ChannelManager.getAdapter("liangzimixin")
4. OpenClawChannelAdapter.sendText({ to, text, accountId: "default" })
5. plugin.outbound.sendText({ cfg, to, text, accountId })
6. Plugin sends to backend API
7. Result flows back: send_ack or send_error
```

### Inbound (Backend → Client)

```
1. Backend pushes message to plugin gateway
2. Plugin calls createReplyDispatcherWithTyping({ deliver, ctx })
3. Runtime shim intercepts deliver() → calls onDeliver()
4. OpenClawChannelAdapter.emitDeliverAsInbound() → normalizes message
5. ChannelManager message callbacks → BridgeServer.routeInboundMessage()
6. ClientRegistry.broadcast("liangzimixin", "default", envelope, senderId)
7. Each subscribed client with matching filter receives the message
```

## Key Design Decisions

### No plugin-specific code

The bridge contains zero plugin-specific logic. All interaction goes through the standard `ChannelPlugin` interface (`gateway`, `outbound`). This means:
- Adding support for a new plugin requires **zero code changes** — just install it
- The bridge doesn't need to be updated when plugins change their internal implementation
- Config schemas are plugin-defined, not bridge-defined

### Runtime shim instead of full framework

Running the full openclaw framework would pull in the AI dispatch pipeline, agent system, and many other components the bridge doesn't need. The shim provides just enough surface for plugins to load and run their gateways.

### Fire-and-forget gateway start

Some plugins block `startAccount()` until abort. Awaiting would prevent the server from starting. The fire-and-forget pattern with optimistic status marking ensures the server is responsive immediately.

### HTTP server starts first

Channel backends can take several seconds to connect. Starting the HTTP server first means WS clients can connect and subscribe immediately, receiving status updates as backends come online.

### Envelope-based protocol

The wire protocol uses structured envelopes with correlation IDs, version fields, and typed payloads. This enables:
- Request/response matching via `id` field
- Protocol versioning for future compatibility
- Type-safe payload handling on both sides
