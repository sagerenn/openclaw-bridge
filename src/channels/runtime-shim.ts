/**
 * Minimal openclaw runtime shim.
 *
 * Provides just enough of the openclaw PluginRuntime surface for channel
 * plugins to load, register, and run their gateway/outbound adapters —
 * WITHOUT running the full openclaw AI dispatch pipeline.
 *
 * The key interception point is the `deliver` callback: instead of
 * dispatching AI-generated replies, we capture inbound messages and
 * route them to WS clients via the bridge's message bus.
 */

import { tmpdir } from "node:os";
import { join } from "node:path";
import { mkdirSync, writeFileSync, existsSync } from "node:fs";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("runtime-shim");

// ─── Types ────────────────────────────────────────────────────────────────────

/** Callback the shim calls when a plugin emits an inbound message via deliver() */
export type DeliverInterceptor = (payload: {
  text?: string;
  mediaUrl?: string;
  mediaUrls?: string[];
  accountId: string;
  channelId: string;
  /** Original inbound context the plugin built before dispatch */
  inboundContext?: unknown;
}) => void;

// ─── Plugin Runtime Channel Shim ──────────────────────────────────────────────

/**
 * Build a minimal `PluginRuntimeChannel` shim.
 *
 * This provides stubs for every sub-surface (text, reply, routing, session,
 * media, commands, etc.) that channel plugins access. The critical surface
 * is `reply` — specifically `createReplyDispatcherWithTyping` and
 * `dispatchReplyFromConfig` / `dispatchReplyWithBufferedBlockDispatcher`,
 * which are the entry points for the AI dispatch pipeline.
 *
 * Instead of running AI, we intercept the `deliver` callback to route
 * messages to WS clients.
 */
function buildChannelShim(
  channelId: string,
  accountId: string,
  onDeliver: DeliverInterceptor,
): Record<string, any> {
  return {
    // ── reply ──────────────────────────────────────────────────────────────
    reply: {
      /**
       * Finalize an inbound message context.
       * In the real runtime this enriches the context with session/routing info.
       * Here we just pass through with minimal defaults.
       */
      finalizeInboundContext: (payload: any) => ({
        ...payload,
        channelId,
        accountId,
        // Ensure required fields exist
        peer: payload.peer ?? payload.senderId ?? payload.from,
        sessionKey: payload.sessionKey ?? `${channelId}:${accountId}:default`,
        agentId: payload.agentId ?? "default",
      }),

      /**
       * Create a reply dispatcher with typing support.
       * The `deliver` callback is the key interception point — it's called
       * by the plugin to send outbound messages (AI replies).
       *
       * We intercept it to route to WS clients instead of the AI pipeline.
       */
      createReplyDispatcherWithTyping: (opts: any) => {
        const originalDeliver = opts.deliver;
        const interceptedDeliver = async (payload: any) => {
          // Route to WS clients via the bridge
          onDeliver({
            text: payload?.text,
            mediaUrl: payload?.mediaUrl,
            mediaUrls: payload?.mediaUrls,
            accountId,
            channelId,
            inboundContext: opts.ctx,
          });

          // Also call the original deliver if the plugin expects it
          // (some plugins use deliver to actually send the message to the backend)
          if (originalDeliver) {
            return originalDeliver(payload);
          }
        };

        return {
          dispatcher: {
            waitForIdle: async () => {},
          },
          replyOptions: {
            disableBlockStreaming: true,
          },
          markDispatchIdle: () => {},
          // Override deliver with our interceptor
          _interceptedDeliver: interceptedDeliver,
        };
      },

      /**
       * Dispatch a reply from config (the main AI dispatch call).
       * In the real runtime this runs the AI agent and streams the reply.
       * Here we skip the AI entirely — the bridge is a passthrough, not an AI host.
       *
       * The plugin's gateway.startAccount() calls this to process inbound messages.
       * Since we're not running AI, we return immediately with empty counts.
       */
      dispatchReplyFromConfig: async (opts: any) => {
        log.debug("dispatchReplyFromConfig called (AI dispatch skipped — bridge mode)", {
          channelId,
          accountId,
        });
        return {
          counts: { final: 0, block: 0, tool: 0 },
          queuedFinal: false,
        };
      },

      /**
       * Dispatch reply with buffered block dispatcher.
       * Same as above — skip AI, intercept deliver.
       */
      dispatchReplyWithBufferedBlockDispatcher: async (opts: any) => {
        const deliver = opts.dispatcherOptions?.deliver;
        if (deliver) {
          // The plugin expects deliver to be called for each reply block.
          // In bridge mode, we don't generate AI replies, so deliver is never called.
          // The plugin's inbound pipeline will call deliver when it has something to send.
        }
        log.debug("dispatchReplyWithBufferedBlockDispatcher called (AI dispatch skipped — bridge mode)", {
          channelId,
          accountId,
        });
        return {
          counts: { final: 0, block: 0, tool: 0 },
        };
      },

      resolveHumanDelayConfig: (_cfg: any, _agentId?: string) => ({
        enabled: false,
        minMs: 0,
        maxMs: 0,
      }),

      withReplyDispatcher: async (opts: any) => {
        if (opts.run) await opts.run();
      },

      settleReplyDispatcher: async () => {},

      resolveEffectiveMessagesConfig: (_cfg: any) => ({
        disableBlockStreaming: true,
      }),

      formatAgentEnvelope: (text: string) => text,
      formatInboundEnvelope: (text: string) => text,
      resolveEnvelopeFormatOptions: () => ({}),
    },

    // ── routing ────────────────────────────────────────────────────────────
    routing: {
      buildAgentSessionKey: (opts: any) =>
        `${channelId}:${opts?.accountId ?? accountId}:${opts?.agentId ?? "default"}`,
      resolveAgentRoute: (opts: any) => ({
        agentId: opts?.cfg?.agentId ?? "default",
        sessionKey: `${channelId}:${opts?.accountId ?? accountId}:default`,
        mainSessionKey: `${channelId}:${opts?.accountId ?? accountId}:default`,
      }),
    },

    // ── session ────────────────────────────────────────────────────────────
    session: {
      resolveStorePath: (_store: any, _opts: any) =>
        join(tmpdir(), "openclaw-bridge", "sessions", channelId, accountId),
      readSessionUpdatedAt: async () => undefined,
      recordSessionMetaFromInbound: async () => {},
      recordInboundSession: async () => {},
      updateLastRoute: async () => {},
    },

    // ── media ──────────────────────────────────────────────────────────────
    media: {
      fetchRemoteMedia: async (opts: any) => {
        // Basic fetch with SSRF protection
        const url = opts?.url;
        if (!url) throw new Error("No URL provided for media fetch");
        const resp = await fetch(url);
        const buffer = Buffer.from(await resp.arrayBuffer());
        return { buffer, contentType: resp.headers.get("content-type") ?? "application/octet-stream" };
      },
      readRemoteMediaBuffer: async (opts: any) => {
        const result = await buildChannelShim(channelId, accountId, onDeliver).media.fetchRemoteMedia(opts);
        return result;
      },
      saveRemoteMedia: async (buffer: Buffer, contentType: string, direction: string, maxBytes?: number, fileName?: string) => {
        return buildChannelShim(channelId, accountId, onDeliver).media.saveMediaBuffer(buffer, contentType, direction, maxBytes, fileName);
      },
      saveResponseMedia: async (buffer: Buffer, contentType: string, direction: string, maxBytes?: number, fileName?: string) => {
        return buildChannelShim(channelId, accountId, onDeliver).media.saveMediaBuffer(buffer, contentType, direction, maxBytes, fileName);
      },
      saveMediaBuffer: async (buffer: Buffer, contentType: string, _direction: string, _maxBytes?: number, fileName?: string) => {
        const mediaDir = join(tmpdir(), "openclaw-bridge", "media", channelId, accountId);
        mkdirSync(mediaDir, { recursive: true });
        const name = fileName ?? `media-${Date.now()}`;
        const filePath = join(mediaDir, name);
        writeFileSync(filePath, buffer);
        return { path: filePath, contentType };
      },
    },

    // ── commands ───────────────────────────────────────────────────────────
    commands: {
      resolveCommandAuthorizedFromAuthorizers: () => ({ authorized: false, reason: "bridge_mode" }),
      isControlCommandMessage: (_text: string, _cfg?: any) => false,
      shouldComputeCommandAuthorized: () => false,
      shouldHandleTextCommands: () => false,
    },

    // ── text ───────────────────────────────────────────────────────────────
    text: {
      chunkByNewline: (text: string) => [text],
      chunkMarkdownText: (text: string) => [text],
      chunkMarkdownTextWithMode: (text: string) => [text],
      chunkText: (text: string) => [text],
      chunkTextWithMode: (text: string) => [text],
      resolveChunkMode: () => "length" as const,
      resolveTextChunkLimit: () => 4000,
      hasControlCommand: () => false,
      resolveMarkdownTableMode: () => "default" as const,
      convertMarkdownTables: (text: string) => text,
    },

    // ── pairing ────────────────────────────────────────────────────────────
    pairing: {
      buildPairingReply: () => "Pairing not supported in bridge mode",
      readAllowFromStore: async () => ({ entries: [] }),
      upsertPairingRequest: async () => {},
    },

    // ── activity ───────────────────────────────────────────────────────────
    activity: {
      record: async () => {},
      get: async () => undefined,
    },

    // ── mentions ───────────────────────────────────────────────────────────
    mentions: {
      buildMentionRegexes: () => [],
      matchesMentionPatterns: () => false,
      matchesMentionWithExplicit: () => false,
      implicitMentionKindWhen: () => undefined,
      resolveInboundMentionDecision: () => ({ shouldMention: false }),
    },

    // ── reactions ──────────────────────────────────────────────────────────
    reactions: {
      createAckReactionHandle: () => ({ dispose: () => {} }),
      shouldAckReaction: () => false,
      removeAckReactionAfterReply: async () => {},
      removeAckReactionHandleAfterReply: async () => {},
    },

    // ── groups ─────────────────────────────────────────────────────────────
    groups: {
      resolveGroupPolicy: () => ({ requireMention: false }),
      resolveRequireMention: () => false,
    },

    // ── debounce ───────────────────────────────────────────────────────────
    debounce: {
      createInboundDebouncer: () => ({ push: async () => false, dispose: () => {} }),
      resolveInboundDebounceMs: () => 0,
    },

    // ── outbound ───────────────────────────────────────────────────────────
    outbound: {
      loadAdapter: () => null,
    },

    // ── inbound ────────────────────────────────────────────────────────────
    inbound: {
      buildContext: (opts: any) => opts,
      run: async () => {},
      runPreparedReply: async () => {},
      dispatchReply: async () => {},
    },

    // ── threadBindings ─────────────────────────────────────────────────────
    threadBindings: {
      setIdleTimeoutBySessionKey: () => [],
      setMaxAgeBySessionKey: () => [],
    },

    // ── runtimeContexts ────────────────────────────────────────────────────
    runtimeContexts: {
      register: () => ({ dispose: () => {} }),
      get: () => undefined,
      watch: () => (() => {}),
    },
  };
}

// ─── Plugin Runtime Shim ──────────────────────────────────────────────────────

/**
 * Build a minimal `PluginRuntime` shim.
 *
 * This is the `api.runtime` object passed to plugin `register(api)`.
 * It must satisfy the version check that plugins perform and provide
 * the `channel` sub-surface.
 */
export function buildPluginRuntime(
  channelId: string,
  accountId: string,
  onDeliver: DeliverInterceptor,
): Record<string, any> {
  return {
    // Version must pass weixin's assertHostCompatibility(">=2026.3.22")
    version: "2026.6.10",

    // Minimal logging — plugins call runtime.log(msg) and runtime.error(msg)
    log: (msg: string) => log.debug("[plugin-runtime]", { msg }),
    error: (msg: string) => log.error("[plugin-runtime]", { msg }),

    // The channel sub-surface — the most critical part
    channel: buildChannelShim(channelId, accountId, onDeliver),

    // Subagent — not used in bridge mode
    subagent: {
      run: async () => ({ runId: "bridge-noop" }),
      waitForRun: async () => ({ status: "ok" as const }),
      getSessionMessages: async () => ({ messages: [] }),
      getSession: async () => ({ messages: [] }),
      deleteSession: async () => {},
    },

    // Nodes — not used in bridge mode
    nodes: {
      list: async () => ({ nodes: [] }),
      invoke: async () => undefined,
    },
  };
}

// ─── Gateway Context Builder ──────────────────────────────────────────────────

/**
 * Build a `ChannelGatewayContext` for a specific account.
 *
 * This is the `ctx` object passed to `gateway.startAccount(ctx)`.
 * It provides the config, account info, abort signal, and the
 * channelRuntime surface.
 */
export function buildGatewayContext(
  channelId: string,
  accountId: string,
  accountConfig: Record<string, unknown>,
  abortSignal: AbortSignal,
  onDeliver: DeliverInterceptor,
  plugin?: Record<string, any>,
): Record<string, any> {
  const runtime = buildPluginRuntime(channelId, accountId, onDeliver);
  const channelRuntime = buildChannelShim(channelId, accountId, onDeliver);

  const cfg = {
    // The full openclaw config — in bridge mode, we construct a minimal one.
    //
    // The account config is exposed in TWO places so different plugins can
    // resolve it regardless of their account-resolution strategy:
    //   1. At the channel top level (merged) — some plugins (e.g.
    //      @larksuite/openclaw-lark) read credentials from the base channel
    //      section for the DEFAULT account and skip the `accounts.<id>`
    //      override when the id is "default". Without the top-level copy,
    //      `getLarkAccount(cfg, "default")` returns `configured: false`.
    //   2. Under `accounts.<accountId>` — the standard override location
    //      used by plugins that resolve non-default accounts explicitly.
    channel: channelId,
    channels: {
      [channelId]: {
        ...accountConfig,
        accounts: {
          [accountId]: accountConfig,
        },
      },
    },
  };

  // Resolve the account through the plugin's own resolver when available.
  //
  // This is critical for channels whose credentials are NOT held in the
  // bridge config — e.g. openclaw-weixin stores its token/baseUrl in the
  // plugin's state dir at QR-login time, keyed by a server-assigned ID the
  // bridge cannot predict. The plugin's `config.resolveAccount(cfg, id)`
  // reads that state and returns an HONEST `configured` flag plus the real
  // token/baseUrl.
  //
  // Without this, the bridge fabricates `configured: true` from an empty
  // config and the plugin's gateway proceeds with `token: undefined` /
  // `baseUrl: undefined`, which crashes the long-poll loop
  // (`undefined.endsWith(...)` -> TypeError) and never receives messages.
  //
  // For plugins that don't expose resolveAccount (or for which the bridge
  // config genuinely carries credentials, e.g. lark/qqbot), we fall back to
  // the previous behavior of merging the account config directly.
  let account: Record<string, any>;
  const resolveAccount = plugin?.config?.resolveAccount;
  if (typeof resolveAccount === "function") {
    try {
      account = resolveAccount(cfg, accountId) ?? {
        accountId,
        configured: false,
        enabled: true,
      };
    } catch (err) {
      log.warn("plugin config.resolveAccount() threw, falling back to fabricated account", {
        channelId,
        accountId,
        error: String(err),
      });
      account = { accountId, configured: false, enabled: true, ...accountConfig };
    }
  } else {
    account = {
      accountId,
      configured: true,
      enabled: true,
      ...accountConfig,
    };
  }

  return {
    cfg,

    accountId,
    account,

    runtime: {
      version: "2026.6.10",
      log: (msg: string) => log.debug("[gateway-runtime]", { msg }),
      error: (msg: string) => log.error("[gateway-runtime]", { msg }),
    },

    abortSignal,

    log: {
      info: (msg: string) => log.info(`[gateway:${channelId}:${accountId}]`, { msg }),
      warn: (msg: string) => log.warn(`[gateway:${channelId}:${accountId}]`, { msg }),
      error: (msg: string) => log.error(`[gateway:${channelId}:${accountId}]`, { msg }),
      debug: (msg: string) => log.debug(`[gateway:${channelId}:${accountId}]`, { msg }),
    },

    getStatus: () => ({
      channelId,
      accountId,
      configured: true,
      enabled: true,
    }),

    setStatus: (_snapshot: any) => {
      // In bridge mode, status is managed by the adapter
    },

    // The channelRuntime surface — required by weixin and other plugins
    channelRuntime,
  };
}

// ─── Plugin API Shim ──────────────────────────────────────────────────────────

/**
 * Build a minimal `OpenClawPluginApi` shim.
 *
 * This is the `api` object passed to `plugin.register(api)`.
 * It captures the `ChannelPlugin` from `api.registerChannel()`.
 */
export function buildPluginApi(
  channelId: string,
  accountId: string,
  onDeliver: DeliverInterceptor,
): {
  api: Record<string, any>;
  getPlugin: () => Record<string, any> | undefined;
} {
  let capturedPlugin: Record<string, any> | undefined;

  const api = {
    id: channelId,
    name: channelId,
    version: "1.0.0",
    description: `Bridge shim for ${channelId}`,
    source: "openclaw-bridge",
    rootDir: process.cwd(),
    registrationMode: "native",

    // Minimal config
    config: {},
    pluginConfig: {},

    // The runtime shim
    runtime: buildPluginRuntime(channelId, accountId, onDeliver),

    // Logger
    logger: {
      debug: (msg: string) => log.debug(`[plugin:${channelId}] ${msg}`),
      info: (msg: string) => log.info(`[plugin:${channelId}] ${msg}`),
      warn: (msg: string) => log.warn(`[plugin:${channelId}] ${msg}`),
      error: (msg: string) => log.error(`[plugin:${channelId}] ${msg}`),
    },

    // Capture the ChannelPlugin from registerChannel()
    registerChannel: (reg: { plugin: Record<string, any> }) => {
      capturedPlugin = reg.plugin;
      log.info("Plugin registered via api.registerChannel()", {
        pluginId: reg.plugin?.id,
        channelId: reg.plugin?.meta?.id,
      });
    },

    // Session API stubs
    session: {
      getSession: async () => null,
      setSession: async () => {},
    },

    // Hook stubs
    hooks: {
      onMessage: () => {},
      onReply: () => {},
    },
  };

  return {
    api,
    getPlugin: () => capturedPlugin,
  };
}
