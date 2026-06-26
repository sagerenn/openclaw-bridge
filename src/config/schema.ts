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
}

export interface ServerConfig {
  host?: string;
  port?: number;
  path?: string;
  maxClients?: number;
  clientHeartbeatMs?: number;
  maxMessageSize?: number;
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
}

export interface LoggingConfig {
  level?: "debug" | "info" | "warn" | "error";
  dir?: string;
}

// ─── Defaults ────────────────────────────────────────────────────────────────

const DEFAULT_SERVER: Required<ServerConfig> = {
  host: "0.0.0.0",
  port: 9300,
  path: "/bridge",
  maxClients: 100,
  clientHeartbeatMs: 30000,
  maxMessageSize: 10 * 1024 * 1024, // 10MB
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
  };
}
