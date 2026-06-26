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
import { loadConfig, type BridgeConfig } from "./config/schema.js";
import { ContactStore } from "./contacts/contact-store.js";
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

  // Create core components
  const channelManager = new ChannelManager();
  const clientRegistry = new ClientRegistry();

  // Create contact store — persists known user IDs for online notifications
  const contactStore = new ContactStore(resolvedConfigPath);
  channelManager.setContactStore(contactStore);

  // Discover and load channel adapters from installed plugins
  const adapters = await loadChannelAdapters();
  for (const [channelId, adapter] of adapters) {
    channelManager.registerAdapter(adapter);
  }

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
      try {
        // Merge channel-level transport overrides into account config
        const mergedConfig = {
          ...accountConfig,
          ...(cfg.transport ?? {}),
        };
        await channelManager.startAccount(channelId, accountId, mergedConfig);
        log.info("Started channel account", { channelId, accountId });
      } catch (err) {
        log.error("Failed to start channel account", { channelId, accountId, error: String(err) });
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
