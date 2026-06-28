/**
 * Generic plugin loader.
 *
 * Discovers installed openclaw channel plugins and creates ChannelAdapter
 * instances by loading each plugin through its standard ChannelPlugin interface.
 *
 * This module contains ZERO plugin-specific code. It works with ANY openclaw
 * channel plugin by:
 * 1. Discovering plugins via openclaw.plugin.json manifests
 * 2. Loading the plugin entry point (dynamic import)
 * 3. Extracting the ChannelPlugin object (via register() or direct export)
 * 4. Creating an OpenClawChannelAdapter that drives the plugin generically
 *
 * Channel plugins are NOT dependencies of this package — they are installed
 * on demand by the user (e.g., `npm install liangzimixin`).
 */

import { discoverPlugins } from "./channel-manager.js";
import type { ChannelAdapter } from "./channel-adapter.js";
import { OpenClawChannelAdapter } from "./openclaw-adapter.js";
import { buildPluginApi, buildBundledChannelCore } from "./runtime-shim.js";
import { patchLarkPlugin } from "../util/patch-lark-plugin.js";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("plugin-loader");

// ─── Plugin Loading ──────────────────────────────────────────────────────────

/**
 * Try to extract a ChannelPlugin object from a loaded module.
 *
 * Openclaw plugins use one of two patterns:
 *
 * 1. **Direct export** — the module exports a ChannelPlugin object directly
 *    (e.g., liangzimixin exports `quantumImPlugin` with gateway, outbound, etc.)
 *
 * 2. **register() pattern** — the module exports an entry with a `register(api)`
 *    method that calls `api.registerChannel({ plugin })` internally
 *    (e.g., weixin exports `{ id, name, description, register }`)
 *
 * This function handles both patterns.
 */
/**
 * Find the entry object that has a `register(api)` method.
 *
 * Handles CJS-ESM interop: when a CJS module is loaded via ESM `import()`,
 * `mod.default` is the CJS `module.exports` object, and the inner `default`
 * export (which may have `register`) is at `mod.default.default`.
 */
function findRegisterEntry(mod: Record<string, any>): { register: Function } | null {
  // Check multiple levels of nesting for the register() method
  const candidates = [
    mod,                    // ESM module itself
    mod.default,            // ESM default export (or CJS module.exports)
    mod.default?.default,   // CJS inner default export (CJS-ESM interop)
  ];

  for (const candidate of candidates) {
    if (candidate && typeof candidate.register === "function") {
      return candidate;
    }
  }

  return null;
}

function extractChannelPlugin(
  mod: Record<string, any>,
  channelId: string,
): Record<string, any> | undefined {
  // Pattern 1: Direct ChannelPlugin export
  // Look for an object with `id`, `gateway`, and/or `outbound` properties
  const candidates = [
    mod.default,
    // CJS-ESM interop: the inner default may be the ChannelPlugin
    mod.default?.default,
    mod.quantumImPlugin,
    mod.weixinPlugin,
    mod.channelPlugin,
    mod.plugin,
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (isChannelPluginLike(candidate)) {
      log.info("Found ChannelPlugin via direct export", {
        pluginId: candidate.id,
        channelId,
      });

      // Also call the entry's register() if available.
      // Some plugins (e.g., liangzimixin) store the runtime globally via
      // setPluginRuntime(api.runtime) inside register(). If register() is
      // never called, getPluginRuntime() throws later at runtime.
      const entry = findRegisterEntry(mod);
      if (entry) {
        log.info("Also calling register() to initialize plugin runtime", { channelId });
        const { api } = buildPluginApi(channelId, "default", () => {});
        try {
          entry.register(api);
        } catch (err: any) {
          log.warn("register() threw during runtime init (non-fatal)", {
            channelId,
            error: String(err),
          });
        }
      }

      return candidate;
    }
  }

  // Pattern 2: register(api) pattern
  // The module (or its default export) has a `register` method
  const entry = findRegisterEntry(mod);
  if (entry) {
    log.info("Found plugin entry with register() pattern, calling register(api)", { channelId });

    // Build a minimal API shim that captures the plugin from registerChannel()
    const { api, getPlugin } = buildPluginApi(channelId, "default", () => {});

    try {
      entry.register(api);
    } catch (err: any) {
      // Some plugins may fail during registration if the runtime shim
      // is incomplete — log but continue
      log.warn("register() threw during plugin extraction (non-fatal)", {
        channelId,
        error: String(err),
      });
    }

    const plugin = getPlugin();
    if (plugin && isChannelPluginLike(plugin)) {
      log.info("Captured ChannelPlugin from registerChannel()", {
        pluginId: plugin.id,
        channelId,
      });
      return plugin;
    }
  }

  return undefined;
}

/**
 * Heuristic check: does this object look like a ChannelPlugin?
 *
 * A ChannelPlugin must have at minimum an `id` field and at least one
 * of the adapter surfaces (gateway, outbound, config, etc.).
 */
function isChannelPluginLike(obj: any): boolean {
  if (!obj || typeof obj !== "object") return false;
  if (!obj.id || typeof obj.id !== "string") return false;

  // Must have at least one adapter surface
  const surfaces = ["gateway", "outbound", "config", "setup", "pairing", "status"];
  return surfaces.some((s) => obj[s] != null);
}

// ─── Adapter Loading ─────────────────────────────────────────────────────────

/**
 * Discover installed plugins and create adapter instances.
 * Returns a map of channel ID -> ChannelAdapter.
 *
 * For each discovered plugin, the loader:
 * 1. Dynamically imports the plugin entry point
 * 2. Extracts the ChannelPlugin object
 * 3. Creates an OpenClawChannelAdapter wrapping it
 */
export async function loadChannelAdapters(): Promise<Map<string, ChannelAdapter>> {
  const adapters = new Map<string, ChannelAdapter>();

  // Self-heal: the published @larksuite/openclaw-lark ships CJS files that use
  // import.meta (ESM-only), which breaks loading under Node's syntax detection.
  // Re-apply the patch at startup in case postinstall didn't run (e.g. the
  // plugin was installed after the fact, or dist/ wasn't built yet).
  try {
    patchLarkPlugin();
  } catch (err) {
    log.warn("Lark plugin self-heal patch failed (non-fatal)", { error: String(err) });
  }

  const discovered = discoverPlugins();

  for (const plugin of discovered) {
    if (!plugin.entryPath) {
      log.warn("Plugin has no entry point, skipping", { pluginId: plugin.id });
      continue;
    }

    try {
      // Dynamic import of the plugin entry point
      log.info("Loading plugin entry point", { pluginId: plugin.id, entryPath: plugin.entryPath });
      const mod = await import(plugin.entryPath);

      // Some channel plugins (both bundled-inside-the-host AND
      // separately-published ones like @openclaw/whatsapp) ship a
      // `BundledChannelEntryContract` as their default export, with a
      // `loadChannelPlugin()` accessor and a `setChannelRuntime(core)`
      // installer. These plugins gate their gateway on a module-level runtime
      // store (e.g. whatsapp's getWhatsAppRuntime()) that must be populated
      // before `gateway.startAccount()` is called — otherwise startAccount
      // crashes on the first runtime access (e.g. logging.shouldLogVerbose).
      // So: whenever the default export IS a bundled-channel-entry contract,
      // use it and install the bridge's shim core, regardless of whether the
      // plugin was discovered as "bundled" (host-internal) or not.
      const entryContract = mod?.default;
      const isBundledEntry =
        entryContract &&
        entryContract.kind === "bundled-channel-entry" &&
        typeof entryContract.loadChannelPlugin === "function";

      let channelPlugin: Record<string, any> | undefined;

      if (isBundledEntry) {
        channelPlugin = entryContract.loadChannelPlugin();
        if (typeof entryContract.setChannelRuntime === "function") {
          try {
            entryContract.setChannelRuntime(buildBundledChannelCore(plugin.id));
            log.info("Installed bundled channel runtime", { pluginId: plugin.id, bundled: plugin.bundled });
          } catch (err: any) {
            log.warn("setChannelRuntime failed (bundled channel may not receive inbound)", {
              pluginId: plugin.id,
              error: String(err),
            });
          }
        }
      } else {
        // Separately-published plugin — extract the ChannelPlugin via the
        // standard register()/direct-export heuristics.
        channelPlugin = extractChannelPlugin(mod, plugin.id);
      }

      if (!channelPlugin) {
        log.warn("Could not extract ChannelPlugin from module, skipping", {
          pluginId: plugin.id,
          entryPath: plugin.entryPath,
          moduleKeys: Object.keys(mod),
          bundled: plugin.bundled,
        });
        continue;
      }

      // Create the adapter
      const adapter = new OpenClawChannelAdapter(channelPlugin);

      // Register for each channel the plugin provides
      for (const channelId of plugin.channels) {
        adapters.set(channelId, adapter);
        log.info("Loaded channel adapter", {
          pluginId: plugin.id,
          channelId,
          adapterLabel: adapter.label,
          bundled: plugin.bundled,
        });
      }
    } catch (err) {
      log.error("Failed to load plugin", {
        pluginId: plugin.id,
        entryPath: plugin.entryPath,
        error: String(err),
      });
    }
  }

  return adapters;
}
