#!/usr/bin/env node
/**
 * OpenClaw Bridge — Universal WebSocket Bridge Server
 *
 * Bridges WS clients to multiple backend IM channels using
 * openclaw channel plugins' standard ChannelPlugin interface.
 *
 * Architecture:
 *   Client --[WebSocket]--> Bridge Server --[ChannelPlugin API]--> Backend Channels
 *
 * Channel plugins are NOT dependencies — they are installed on demand:
 *   npm install liangzimixin
 *   npm install @tencent-weixin/openclaw-weixin
 *
 * The server discovers installed plugins via openclaw.plugin.json manifests
 * and loads them generically through the standard plugin interface.
 * No plugin-specific code exists in this server.
 *
 * Usage:
 *   node dist/server.js [--config path/to/config.json]
 */

import { resolve } from "node:path";
import { BridgeServer } from "./server/bridge-server.js";
import { ClientRegistry } from "./server/client-registry.js";
import { ChannelManager } from "./channels/channel-manager.js";
import { loadChannelAdapters } from "./channels/plugin-loader.js";
import { TelegramBridgeAdapter } from "./channels/telegram-adapter.js";
import { loadConfig, normalizeProxyUrl, resolveEffectiveProxy, type BridgeConfig } from "./config/schema.js";
import { ContactStore } from "./contacts/contact-store.js";
import { setupProxy, resolveProxyFromEnv, getActiveProxyUrl } from "./util/proxy-setup.js";
import { rootLogger } from "./util/logger.js";

const log = rootLogger.child("main");

// ─── CLI Argument Parsing ────────────────────────────────────────────────────

function parseArgs(): { config?: string } {
  const args = process.argv.slice(2);
  let config: string | undefined;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--config" && args[i + 1]) {
      config = args[++i];
    }
  }
  return { config };
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const { config: configPath } = parseArgs();
  const resolvedConfigPath = configPath ? resolve(configPath) : undefined;
  const config = loadConfig(resolvedConfigPath);

  // Apply logging level
  if (config.logging?.level) {
    (rootLogger as any).minLevel = ["debug", "info", "warn", "error"].indexOf(config.logging.level);
  }

  log.info("Starting OpenClaw Bridge Server", { version: "1.0.0" });

  // Activate the global outbound proxy BEFORE any channel adapter starts, so
  // every plugin's fetch() and WebSocket traffic is routed through it. The
  // explicit `proxy` config wins; otherwise we honor the standard env vars
  // (HTTPS_PROXY / HTTP_PROXY / ALL_PROXY) so a shell proxy just works.
  const globalProxy = normalizeProxyUrl(config.proxy) ?? resolveProxyFromEnv();
  if (globalProxy) {
    setupProxy(globalProxy);
  }

  // Create core components
  const channelManager = new ChannelManager();
  const clientRegistry = new ClientRegistry();

  // Create contact store — persists known user IDs for online notifications
  const contactStore = new ContactStore(resolvedConfigPath);
  channelManager.setContactStore(contactStore);

  // When QR login succeeds, auto-start the account
  channelManager.setOnQrLoginSuccess(async (channelId, accountId) => {
    log.info("QR login succeeded — auto-starting account", { channelId, accountId });
    const channelConfig = (config as any).channels?.[channelId];
    const accountConfig = channelConfig?.accounts?.[accountId] ?? {};

    // Channels like openclaw-weixin persist credentials in the plugin's own
    // state dir at QR-login time (NOT in the bridge config), keyed by a
    // server-assigned account ID that the bridge cannot predict. Their
    // adapter resolves credentials from disk via the plugin, so an empty
    // account config is expected and fine — start the account regardless of
    // whether it has a static config entry.
    const mergedConfig = {
      ...accountConfig,
      ...(channelConfig?.transport ?? {}),
    };

    // Apply any channel/account-level proxy override before starting.
    const accountProxy = resolveEffectiveProxy(config, channelId, accountId);
    if (accountProxy && accountProxy !== getActiveProxyUrl()) {
      setupProxy(accountProxy);
    }

    try {
      await channelManager.startAccount(channelId, accountId, mergedConfig);
      log.info("Auto-started account after QR login", { channelId, accountId });
    } catch (err) {
      log.error("Failed to auto-start account after QR login", { channelId, accountId, error: String(err) });
    }
  });

  // Discover and load channel adapters from installed plugins
  const adapters = await loadChannelAdapters();
  for (const [channelId, adapter] of adapters) {
    channelManager.registerAdapter(adapter);
  }

  // Override the bundled `telegram` adapter with the bridge-native one. The
  // bundled openclaw telegram plugin hard-wires the real AI-agent dispatch for
  // inbound, which never reaches WS clients (and fails without an API key);
  // the bridge-native TelegramBridgeAdapter owns both legs via the raw Bot API
  // and routes inbound straight to the WS seam. Registering it after the
  // plugin adapters lets it win the channelId="telegram" slot. See
  // src/channels/telegram-adapter.ts.
  channelManager.registerAdapter(new TelegramBridgeAdapter());

  if (adapters.size === 0) {
    log.warn("No channel plugins discovered. Install plugins with:");
    log.warn("  npm install liangzimixin");
    log.warn("  npm install @tencent-weixin/openclaw-weixin");
  }

  // Create and start the bridge server FIRST so clients can connect immediately
  const server = new BridgeServer(config, channelManager, clientRegistry);
  await server.start();

  // Then start configured channel accounts (may take time for backend connections)
  await startConfiguredChannels(config, channelManager);

  // Resume accounts whose credentials were persisted out-of-band by a
  // plugin (e.g. openclaw-weixin via QR login) but have no static config
  // entry. Without this, a logged-in WeChat account would sit idle until the
  // user re-scans — the bridge would never start its long-poll loop.
  await resumeSavedAccounts(config, channelManager);

  // Send online notification to all persisted contacts
  await sendOnlineNotifications(channelManager, contactStore);

  // Graceful shutdown
  const shutdown = async (signal: string) => {
    log.info(`Received ${signal}, shutting down...`);
    contactStore.flush();
    await server.stop();
    process.exit(0);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  log.info("Bridge server is ready");
}

async function startConfiguredChannels(
  config: BridgeConfig,
  channelManager: ChannelManager
): Promise<void> {
  for (const [channelId, channelConfig] of Object.entries(config.channels)) {
    if (!channelConfig || typeof channelConfig !== "object") continue;

    const cfg = channelConfig as any;
    if (cfg.enabled === false) {
      log.info("Channel disabled, skipping", { channelId });
      continue;
    }

    const adapter = channelManager.getAdapter(channelId);
    if (!adapter) {
      log.warn("Channel configured but no adapter available (plugin not installed?)", { channelId });
      continue;
    }

    const accounts = cfg.accounts as Record<string, Record<string, unknown>> | undefined;
    if (!accounts) {
      log.warn("No accounts configured for channel", { channelId });
      continue;
    }

    for (const [accountId, accountConfig] of Object.entries(accounts)) {
      // A channel/account-level proxy override takes precedence over the
      // global one for this account's traffic. Because undici's fetch
      // dispatcher is process-wide, the active proxy is necessarily shared
      // across accounts; if multiple accounts configure different proxies,
      // the last-started one wins — log that so it isn't a silent surprise.
      const accountProxy = resolveEffectiveProxy(config, channelId, accountId);
      if (accountProxy && accountProxy !== getActiveProxyUrl()) {
        if (getActiveProxyUrl()) {
          log.warn("Conflicting per-account proxies — fetch traffic will use the last-started account's proxy (global fetch dispatcher is shared)", {
            channelId,
            accountId,
            previous: getActiveProxyUrl(),
            current: accountProxy,
          });
        }
        setupProxy(accountProxy);
      }

      try {
        // Merge channel-level transport overrides into account config
        const mergedConfig = {
          ...accountConfig,
          ...(cfg.transport ?? {}),
        };
        await channelManager.startAccount(channelId, accountId, mergedConfig);
        log.info("Started channel account", { channelId, accountId });
      } catch (err: any) {
        const errMsg = String(err);
        // If the account needs QR login (e.g. weixin "not configured: missing token"),
        // log a helpful message instead of treating it as a fatal error
        if (errMsg.includes("not configured") || errMsg.includes("missing token")) {
          log.info("Account needs setup — use QR login to configure", {
            channelId,
            accountId,
            hint: `GET /plugin/${channelId}/${accountId}/qr`,
          });
        } else {
          log.error("Failed to start channel account", { channelId, accountId, error: errMsg });
        }
      }
    }
  }
}

/**
 * Resume accounts the plugin itself knows about (credentials persisted by a
 * QR-login flow, e.g. openclaw-weixin) that aren't listed in the bridge config.
 *
 * These accounts aren't started by startConfiguredChannels() because they
 * have no static config entry — the bridge couldn't have predicted their
 * server-assigned account IDs. The plugin's config.listAccountIds(cfg)
 * enumerates them, and its gateway resolves real credentials from disk.
 */
async function resumeSavedAccounts(
  config: BridgeConfig,
  channelManager: ChannelManager
): Promise<void> {
  for (const adapter of channelManager.getAllAdapters()) {
    const channelId = adapter.channelId;
    const savedIds = adapter.listSavedAccountIds();

    for (const accountId of savedIds) {
      // Skip accounts already started (configured at boot, or already resumed)
      if (adapter.listAccounts().includes(accountId)) continue;

      const channelConfig = (config as any).channels?.[channelId];
      if (channelConfig?.enabled === false) {
        log.info("Channel disabled, skipping saved account", { channelId, accountId });
        continue;
      }

      try {
        await channelManager.startAccount(channelId, accountId, {});
        log.info("Resumed saved account", { channelId, accountId });
      } catch (err: any) {
        log.warn("Failed to resume saved account", { channelId, accountId, error: String(err) });
      }
    }
  }
}

main().catch((err) => {
  log.error("Fatal error", { error: String(err) });
  process.exit(1);
});

/**
 * Send an "online" notification to all persisted contacts.
 * Called after startup so contacts know the bridge is back.
 */
async function sendOnlineNotifications(
  channelManager: ChannelManager,
  contactStore: ContactStore
): Promise<void> {
  const activeAccounts = contactStore.getActiveAccounts();
  if (activeAccounts.length === 0) {
    log.info("No persisted contacts — skipping online notifications");
    return;
  }

  for (const { channel, accountId } of activeAccounts) {
    const adapter = channelManager.getAdapter(channel);
    if (!adapter) {
      log.warn("No adapter for contact's channel, skipping online notifications", { channel });
      continue;
    }

    const contacts = contactStore.getContactsForAccount(channel, accountId);
    const status = channelManager.getStatus(channel, accountId);
    if (status.state !== "connected") {
      log.info("Channel not connected, skipping online notifications", { channel, accountId });
      continue;
    }

    log.info("Sending online notifications", { channel, accountId, contactCount: contacts.length });

    for (const contact of contacts) {
      try {
        await adapter.sendText({
          to: contact.userId,
          text: "🟢 Bridge is back online",
          accountId,
        });
        log.info("Online notification sent", { channel, accountId, userId: contact.userId });
      } catch (err) {
        log.warn("Failed to send online notification", {
          channel, accountId, userId: contact.userId, error: String(err),
        });
      }
    }
  }
}
