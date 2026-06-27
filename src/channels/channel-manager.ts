/**
 * ChannelManager — manages all channel adapter instances.
 * Handles dynamic plugin discovery, loading, and lifecycle.
 */

import { readdirSync, existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import type { ChannelAdapter, QrStartResult, QrWaitResult } from "./channel-adapter.js";
import type { ChannelStatus, NormalizedInboundMessage } from "../protocol/messages.js";
import type { ContactStore } from "../contacts/contact-store.js";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("channel-manager");

// ─── Plugin Discovery ────────────────────────────────────────────────────────

interface PluginManifest {
  id: string;
  channels: string[];
  channelConfigs?: Record<string, { schema?: unknown; label?: string; description?: string }>;
}

interface DiscoveredPlugin {
  /** Plugin ID from openclaw.plugin.json */
  id: string;
  /** Channel IDs this plugin provides */
  channels: string[];
  /** Absolute path to the plugin package root */
  packagePath: string;
  /** The openclaw.plugin.json manifest */
  manifest: PluginManifest;
  /** Entry point path from package.json openclaw.extensions or openclaw.runtimeExtensions */
  entryPath?: string;
  /**
   * Whether this is a channel bundled inside the `openclaw` host package
   * (at `dist/extensions/<id>/`) rather than a separately-installed plugin
   * package. Bundled channels gate their gateway on a module-level runtime
   * store (setXRuntime) and need `setChannelRuntime(core)` called at load.
   */
  bundled?: boolean;
}

/**
 * Discover installed openclaw channel plugins by scanning node_modules
 * for openclaw.plugin.json manifests.
 */
export function discoverPlugins(searchPaths?: string[]): DiscoveredPlugin[] {
  const plugins: DiscoveredPlugin[] = [];
  const seen = new Set<string>();

  const roots = searchPaths ?? [
    resolve(process.cwd(), "node_modules"),
  ];

  for (const root of roots) {
    if (!existsSync(root)) continue;

    try {
      const entries = readdirSync(root, { withFileTypes: true });
      for (const entry of entries) {
        if (!entry.isDirectory()) continue;

        // Handle scoped packages (@scope/name)
        let pkgPath: string;
        if (entry.name.startsWith("@")) {
          const scopePath = join(root, entry.name);
          try {
            const scopeEntries = readdirSync(scopePath, { withFileTypes: true });
            for (const scopeEntry of scopeEntries) {
              if (!scopeEntry.isDirectory()) continue;
              pkgPath = join(scopePath, scopeEntry.name);
              tryDiscoverPlugin(pkgPath, plugins, seen);
            }
          } catch {
            // Skip unreadable scope directories
          }
          continue;
        }

        pkgPath = join(root, entry.name);
        tryDiscoverPlugin(pkgPath, plugins, seen);
      }
    } catch (err) {
      log.warn("Failed to scan node_modules", { root, error: String(err) });
    }
  }

  // Also discover channels BUNDLED inside the `openclaw` host package — they
  // live at `node_modules/openclaw/dist/extensions/<id>/` and are NOT installed
  // as separate packages, so the node_modules scan above won't find them.
  // These are the only self-hostable channels (irc, mattermost, …); the
  // separately-published plugins target external SaaS IMs.
  discoverBundledPlugins(plugins, seen);

  return plugins;
}

/**
 * Discover channel plugins bundled inside the `openclaw` host package at
 * `dist/extensions/<id>/`. Each extension ships its own openclaw.plugin.json.
 */
function discoverBundledPlugins(
  plugins: DiscoveredPlugin[],
  seen: Set<string>,
): void {
  const candidates: string[] = [];
  const nm = resolve(process.cwd(), "node_modules");
  // openclaw may be a direct dependency or a transitive one. Check the common
  // install locations for an extensions dir.
  for (const root of [join(nm, "openclaw")]) {
    candidates.push(join(root, "dist", "extensions"));
  }

  for (const extDir of candidates) {
    if (!existsSync(extDir)) continue;
    let entries: string[];
    try {
      entries = readdirSync(extDir, { withFileTypes: true })
        .filter((e) => e.isDirectory())
        .map((e) => e.name);
    } catch (err) {
      log.warn("Failed to scan bundled extensions dir", { extDir, error: String(err) });
      continue;
    }

    for (const name of entries) {
      tryDiscoverPlugin(join(extDir, name), plugins, seen, true);
    }
  }
}

function tryDiscoverPlugin(
  pkgPath: string,
  plugins: DiscoveredPlugin[],
  seen: Set<string>,
  bundled = false,
): void {
  const manifestPath = join(pkgPath, "openclaw.plugin.json");
  if (!existsSync(manifestPath)) return;

  try {
    const raw = readFileSync(manifestPath, "utf-8");
    const manifest: PluginManifest = JSON.parse(raw);

    if (!manifest.id || !manifest.channels?.length) return;
    if (seen.has(manifest.id)) return;
    seen.add(manifest.id);

    // Resolve entry point from package.json
    let entryPath: string | undefined;
    const pkgJsonPath = join(pkgPath, "package.json");
    if (existsSync(pkgJsonPath)) {
      const pkgJson = JSON.parse(readFileSync(pkgJsonPath, "utf-8"));
      const openclawField = pkgJson.openclaw ?? {};
      const extensions = openclawField.runtimeExtensions ?? openclawField.extensions ?? [];
      if (extensions.length > 0) {
        entryPath = resolve(pkgPath, extensions[0]);
      }
    }

    // Bundled extensions sometimes declare no `channels` array in their
    // package.json openclaw field but DO declare it in the manifest. The
    // manifest.id is the channel id for these single-channel extensions.
    plugins.push({
      id: manifest.id,
      channels: manifest.channels,
      packagePath: pkgPath,
      manifest,
      entryPath,
      bundled,
    });

    log.info("Discovered plugin", {
      id: manifest.id,
      channels: manifest.channels,
      pkgPath,
      bundled,
    });
  } catch (err) {
    log.warn("Failed to parse plugin manifest", { path: manifestPath, error: String(err) });
  }
}

// ─── Channel Manager ─────────────────────────────────────────────────────────

export class ChannelManager {
  private adapters = new Map<string, ChannelAdapter>();
  private statusCache = new Map<string, ChannelStatus>();
  private messageCallbacks: ((msg: NormalizedInboundMessage) => void)[] = [];
  private statusCallbacks: ((status: ChannelStatus) => void)[] = [];
  private contactStore?: ContactStore;
  private onQrLoginSuccess?: (channelId: string, accountId: string) => Promise<void>;

  /** Set the contact store for recording inbound senders */
  setContactStore(store: ContactStore): void {
    this.contactStore = store;
  }

  /** Set callback invoked when a QR login succeeds — used to auto-start the account */
  setOnQrLoginSuccess(callback: (channelId: string, accountId: string) => Promise<void>): void {
    this.onQrLoginSuccess = callback;
  }

  /** Register a channel adapter */
  registerAdapter(adapter: ChannelAdapter): void {
    this.adapters.set(adapter.channelId, adapter);
    adapter.onMessage((msg) => {
      this.statusCache.set(`${msg.channel}:${msg.accountId}`, {
        ...this.getStatus(msg.channel, msg.accountId),
        lastInboundAt: Date.now(),
      });

      // Record the sender as a known contact
      if (this.contactStore && msg.senderId) {
        this.contactStore.recordContact({
          userId: msg.senderId,
          channel: msg.channel,
          accountId: msg.accountId,
          displayName: msg.senderName,
        });
      }

      for (const cb of this.messageCallbacks) cb(msg);
    });
    adapter.onStatusChange((status) => {
      this.statusCache.set(`${status.channel}:${status.accountId}`, status);
      for (const cb of this.statusCallbacks) cb(status);
    });
    log.info("Registered channel adapter", { channelId: adapter.channelId, label: adapter.label });
  }

  /** Start a specific account on a channel */
  async startAccount(channelId: string, accountId: string, credentials: Record<string, unknown>): Promise<void> {
    const adapter = this.adapters.get(channelId);
    if (!adapter) throw new Error(`No adapter registered for channel: ${channelId}`);
    await adapter.start(accountId, credentials);
  }

  /** Stop a specific account on a channel */
  async stopAccount(channelId: string, accountId: string): Promise<void> {
    const adapter = this.adapters.get(channelId);
    if (!adapter) return;
    await adapter.stop(accountId);
  }

  /** Stop all adapters */
  async stopAll(): Promise<void> {
    for (const adapter of this.adapters.values()) {
      try {
        await adapter.stopAll();
      } catch (err) {
        log.error("Error stopping adapter", { channelId: adapter.channelId, error: String(err) });
      }
    }
  }

  /** Get adapter by channel ID */
  getAdapter(channelId: string): ChannelAdapter | undefined {
    return this.adapters.get(channelId);
  }

  /** Get all registered adapters */
  getAllAdapters(): ChannelAdapter[] {
    return [...this.adapters.values()];
  }

  /** Get status for a specific channel account */
  getStatus(channel: string, accountId: string): ChannelStatus {
    const key = `${channel}:${accountId}`;
    return this.statusCache.get(key) ?? {
      channel,
      accountId,
      connected: false,
      state: "disconnected",
    };
  }

  /** Get all statuses */
  getAllStatus(): ChannelStatus[] {
    return [...this.statusCache.values()];
  }

  /** Register a global inbound message callback */
  onMessage(callback: (msg: NormalizedInboundMessage) => void): void {
    this.messageCallbacks.push(callback);
  }

  /** Register a global status change callback */
  onStatusChange(callback: (status: ChannelStatus) => void): void {
    this.statusCallbacks.push(callback);
  }

  /** Start a QR code login flow for a channel account */
  async loginWithQrStart(channelId: string, params: { accountId?: string; force?: boolean }): Promise<QrStartResult> {
    const adapter = this.adapters.get(channelId);
    if (!adapter) throw new Error(`No adapter registered for channel: ${channelId}`);
    return adapter.loginWithQrStart(params);
  }

  /** Wait for a QR code login to complete */
  async loginWithQrWait(channelId: string, params: { accountId?: string; sessionKey?: string; timeoutMs?: number }): Promise<QrWaitResult> {
    const adapter = this.adapters.get(channelId);
    if (!adapter) throw new Error(`No adapter registered for channel: ${channelId}`);
    const result = await adapter.loginWithQrWait(params);

    // If login succeeded, trigger the callback to auto-start the account
    if (result.connected && this.onQrLoginSuccess) {
      const effectiveAccountId = result.accountId ?? params.accountId ?? "default";
      // Fire-and-forget — don't block the response
      this.onQrLoginSuccess(channelId, effectiveAccountId).catch((err) => {
        log.warn("Failed to auto-start account after QR login", {
          channelId,
          accountId: effectiveAccountId,
          error: String(err),
        });
      });
    }

    return result;
  }
}
