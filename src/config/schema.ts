/**
 * Bridge server configuration schema and loader.
 *
 * The config is fully generic — no channel-specific types.
 * Each channel section is a dynamic map where the key is the channel ID
 * and the value contains `accounts` with arbitrary credentials.
 */

import { readFileSync, existsSync } from "node:fs";
import { resolve } from "node:path";
import { rootLogger } from "../util/logger.js";

// ─── Config Types ────────────────────────────────────────────────────────────

export interface BridgeConfig {
  server: ServerConfig;
  channels: ChannelsConfig;
  logging?: LoggingConfig;
  /**
   * Optional global proxy used for ALL outbound channel-plugin traffic
   * (HTTP fetches AND WebSocket/gateway connections). May be overridden per
   * channel or per account.
   *
   * Supported schemes:
   *   - http://[user:pass@]host:port
   *   - https://[user:pass@]host:port
   *   - socks5://[user:pass@]host:port   (also socks://)
   *
   * If omitted, the bridge also honors the standard env vars
   * HTTPS_PROXY / HTTP_PROXY / ALL_PROXY (and lowercase) for fetch traffic.
   */
  proxy?: ProxyConfig;
}

/**
 * Proxy configuration. Either an inline URL string or an object with a `url`
 * plus optional no-proxy host patterns.
 */
export type ProxyConfig = string | ProxyConfigObject;

export interface ProxyConfigObject {
  /** Proxy URL, e.g. "socks5://user:pass@host:1080" or "http://host:8080". */
  url: string;
  /**
   * Optional list of hostname substrings (or RegExp source) that should
   * bypass the proxy and connect directly.
   */
  noProxy?: string[];
}

export interface ServerConfig {
  host?: string;
  port?: number;
  path?: string;
  maxClients?: number;
  clientHeartbeatMs?: number;
  maxMessageSize?: number;
  /**
   * Optional URLs where clients can fetch the machine-readable API specs.
   * When set, these are advertised in the `welcome` envelope so tooling can
   * discover them. They may be relative (served by the bridge itself, e.g.
   * "/spec/asyncapi.json") or absolute.
   */
  asyncApiSpecUrl?: string;
  openApiSpecUrl?: string;
}

/**
 * Dynamic channel configuration map.
 * Keys are channel IDs (e.g. "liangzimixin", "openclaw-weixin").
 * Values are channel-specific — the bridge doesn't prescribe the schema;
 * it passes the account config through to the plugin's gateway.startAccount().
 */
export interface ChannelsConfig {
  [channelId: string]: ChannelSectionConfig | undefined;
}

/**
 * A channel section in the config.
 * The `accounts` map contains account IDs -> arbitrary credentials
 * that are passed directly to the plugin's gateway.
 */
export interface ChannelSectionConfig {
  enabled?: boolean;
  accounts: Record<string, Record<string, unknown>>;
  /** Optional transport-level overrides (e.g. custom URLs) */
  transport?: Record<string, unknown>;
  /**
   * Channel-level proxy override. Takes precedence over the global `proxy`.
   * Per-account `proxy` (inside `accounts.<id>`) takes precedence over this.
   */
  proxy?: ProxyConfig;
}

export interface LoggingConfig {
  level?: "debug" | "info" | "warn" | "error";
  dir?: string;
}

// ─── Defaults ────────────────────────────────────────────────────────────────

const DEFAULT_SERVER: ServerConfig = {
  host: "0.0.0.0",
  port: 9300,
  path: "/bridge",
  maxClients: 100,
  clientHeartbeatMs: 30000,
  maxMessageSize: 10 * 1024 * 1024, // 10MB
  asyncApiSpecUrl: undefined,
  openApiSpecUrl: undefined,
};

// ─── Loader ──────────────────────────────────────────────────────────────────

export function loadConfig(configPath?: string): BridgeConfig {
  const resolvedPath = configPath ?? resolve(process.cwd(), "config.json");

  if (!existsSync(resolvedPath)) {
    rootLogger.warn("No config file found, using defaults", { path: resolvedPath });
    return { server: DEFAULT_SERVER, channels: {} };
  }

  const raw = readFileSync(resolvedPath, "utf-8");
  const parsed = JSON.parse(raw);

  return normalizeConfig(parsed);
}

export function normalizeConfig(partial: Partial<BridgeConfig>): BridgeConfig {
  return {
    server: { ...DEFAULT_SERVER, ...partial.server },
    channels: partial.channels ?? {},
    logging: partial.logging,
    proxy: partial.proxy,
  };
}

// ─── Proxy resolution ────────────────────────────────────────────────────────

/**
 * Normalize a `ProxyConfig` (string or object) into a plain URL string.
 * Returns undefined for an empty/invalid config.
 */
export function normalizeProxyUrl(proxy: ProxyConfig | undefined): string | undefined {
  if (!proxy) return undefined;
  if (typeof proxy === "string") return proxy.trim() || undefined;
  const url = proxy.url?.trim();
  return url || undefined;
}

/**
 * Resolve the effective proxy URL for a channel account, applying the
 * precedence: account > channel > global. Returns undefined if no proxy is
 * configured at any level.
 *
 * NOTE: This does NOT consult process env vars — env-var fallback is handled
 * at install time in proxy-setup (only when nothing is configured here) so a
 * user's explicit `null`/absent config still picks up HTTPS_PROXY.
 */
export function resolveEffectiveProxy(
  config: BridgeConfig,
  channelId: string,
  accountId: string,
): string | undefined {
  const channelCfg = config.channels?.[channelId];
  const accountCfg = channelCfg?.accounts?.[accountId] as
    | { proxy?: ProxyConfig }
    | undefined;

  return (
    normalizeProxyUrl(accountCfg?.proxy) ??
    normalizeProxyUrl(channelCfg?.proxy) ??
    normalizeProxyUrl(config.proxy)
  );
}
