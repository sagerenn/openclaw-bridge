/**
 * E2E test: spec URLs.
 *
 * Verifies that:
 *   - GET /spec/asyncapi.json serves a valid AsyncAPI 2.6.0 document generated
 *     live from the message-type enum (every BridgeMessageType appears).
 *   - GET /spec/openapi.json serves a valid OpenAPI 3.1.0 document generated
 *     live from the route table (every HTTP_ROUTES path appears).
 *   - The WebSocket `welcome` envelope advertises both spec URLs.
 *
 * No channel credentials required — runs with an empty channels config.
 *
 * Run: node dist/test/e2e-test-spec.js
 */

import { WebSocket } from "ws";
import http from "node:http";
import { BridgeServer } from "../server/bridge-server.js";
import { ClientRegistry } from "../server/client-registry.js";
import { ChannelManager } from "../channels/channel-manager.js";
import { loadConfig, type BridgeConfig } from "../config/schema.js";
import {
  BridgeMessageType,
  type BridgeEnvelope,
} from "../protocol/messages.js";
import { HTTP_ROUTES } from "../server/http-routes.js";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("e2e-spec");

const ASYNC_API_PATH = "/spec/asyncapi.json";
const OPEN_API_PATH = "/spec/openapi.json";

// ─── Helpers ──────────────────────────────────────────────────────────────────

function httpGet(url: string): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    http
      .get(url, (res) => {
        let body = "";
        res.on("data", (c: Buffer) => (body += c.toString()));
        res.on("end", () => resolve({ status: res.statusCode ?? 0, body }));
      })
      .on("error", reject);
  });
}

function assert(cond: boolean, msg: string): void {
  if (!cond) throw new Error(`assertion failed: ${msg}`);
}

// ─── Test ─────────────────────────────────────────────────────────────────────

async function runTest(): Promise<void> {
  // Load the base config but force an empty (no credential) channel set and a
  // fixed high port so this test is self-contained.
  const config: BridgeConfig = loadConfig(process.env.BRIDGE_CONFIG_PATH);
  const baseConfig: BridgeConfig = {
    ...config,
    server: {
      ...config.server,
      port: 9499,
      path: "/bridge",
      asyncApiSpecUrl: undefined,
      openApiSpecUrl: undefined,
    },
    channels: {},
  };

  const port = baseConfig.server.port ?? 9499;
  const path = baseConfig.server.path ?? "/bridge";
  const base = `http://127.0.0.1:${port}`;

  const channelManager = new ChannelManager();
  const clientRegistry = new ClientRegistry();
  const server = new BridgeServer(baseConfig, channelManager, clientRegistry);

  let ws: WebSocket | undefined;
  try {
    await server.start();
    log.info("Server started", { port, path });

    // ── 1. AsyncAPI spec over HTTP ─────────────────────────────────────────
    log.info("=== Step 1: GET /spec/asyncapi.json ===");
    {
      const { status, body } = await httpGet(`${base}${ASYNC_API_PATH}`);
      assert(status === 200, `asyncapi status 200, got ${status}`);
      const spec = JSON.parse(body);
      assert(spec.asyncapi === "2.6.0", `asyncapi version 2.6.0, got ${spec.asyncapi}`);
      assert(
        typeof spec.servers?.bridge?.url === "string",
        "asyncapi servers.bridge.url is a string",
      );
      assert(
        spec.servers.bridge.url.endsWith(path),
        `asyncapi server url ends with ${path}, got ${spec.servers.bridge.url}`,
      );
      const messages = spec.components?.messages ?? {};
      const messageNames = Object.keys(messages);
      const expected = Object.values(BridgeMessageType) as string[];
      for (const name of expected) {
        assert(messageNames.includes(name), `asyncapi includes message ${name}`);
      }
      assert(
        messageNames.length === expected.length,
        `asyncapi message count ${expected.length}, got ${messageNames.length}`,
      );
      // Each direction-bearing message carries a payload $ref (or empty schema for ping/pong).
      assert(messages.welcome.payload.$ref === "#/components/schemas/WelcomePayload", "welcome payload ref");
      assert(messages.ping.payload?.type === "object", "ping has empty payload schema");

      // x-spec-urls advertises the default-served paths.
      assert(spec["x-spec-urls"]?.asyncApiSpecUrl === ASYNC_API_PATH, "asyncapi x-spec-urls.asyncApiSpecUrl");
      assert(spec["x-spec-urls"]?.openApiSpecUrl === OPEN_API_PATH, "asyncapi x-spec-urls.openApiSpecUrl");
      log.info(`AsyncAPI OK — ${messageNames.length} messages, server ${spec.servers.bridge.url}`);
    }

    // ── 2. OpenAPI spec over HTTP ──────────────────────────────────────────
    log.info("=== Step 2: GET /spec/openapi.json ===");
    {
      const { status, body } = await httpGet(`${base}${OPEN_API_PATH}`);
      assert(status === 200, `openapi status 200, got ${status}`);
      const spec = JSON.parse(body);
      assert(spec.openapi === "3.1.0", `openapi version 3.1.0, got ${spec.openapi}`);
      assert(typeof spec.servers?.[0]?.url === "string", "openapi server url present");
      const paths = Object.keys(spec.paths ?? {});
      for (const route of HTTP_ROUTES) {
        assert(paths.includes(route.path), `openapi includes path ${route.path}`);
      }
      // Every route table entry is documented operationally.
      assert(!!spec.paths[ASYNC_API_PATH]?.get, "openapi asyncapi path has GET op");
      assert(!!spec.paths[OPEN_API_PATH]?.get, "openapi openapi path has GET op");
      assert(
        spec["x-spec-urls"]?.asyncApiSpecUrl === ASYNC_API_PATH,
        "openapi x-spec-urls.asyncApiSpecUrl",
      );
      assert(
        spec["x-spec-urls"]?.openApiSpecUrl === OPEN_API_PATH,
        "openapi x-spec-urls.openApiSpecUrl",
      );
      log.info(`OpenAPI OK — ${paths.length} paths, server ${spec.servers[0].url}`);
    }

    // ── 3. Unknown route 404s ──────────────────────────────────────────────
    log.info("=== Step 3: unknown route returns 404 ===");
    {
      const { status } = await httpGet(`${base}/does-not-exist`);
      assert(status === 404, `unknown route 404, got ${status}`);
      log.info("404 OK");
    }

    // ── 4. welcome envelope advertises spec URLs ───────────────────────────
    log.info("=== Step 4: WebSocket welcome advertises spec URLs ===");
    {
      const welcome = await new Promise<BridgeEnvelope>((resolve, reject) => {
        const socket = new WebSocket(`ws://127.0.0.1:${port}${path}`);
        socket.on("message", (data) => resolve(JSON.parse(data.toString())));
        socket.on("error", reject);
        setTimeout(() => reject(new Error("welcome timeout")), 10000).unref();
        ws = socket;
      });
      assert(welcome.type === BridgeMessageType.WELCOME, `welcome type, got ${welcome.type}`);
      const payload = welcome.payload as { asyncApiSpecUrl?: string; openApiSpecUrl?: string };
      assert(payload.asyncApiSpecUrl === ASYNC_API_PATH, `welcome.asyncApiSpecUrl, got ${payload.asyncApiSpecUrl}`);
      assert(payload.openApiSpecUrl === OPEN_API_PATH, `welcome.openApiSpecUrl, got ${payload.openApiSpecUrl}`);
      log.info("Welcome OK — spec URLs advertised", payload);
    }

    log.info("✅ Spec URL E2E test passed!");
  } finally {
    if (ws) ws.close();
    await server.stop();
  }
}

// ─── Entry Point ──────────────────────────────────────────────────────────────

runTest().then(() => {
  process.exit(0);
}).catch((err) => {
  log.error("Spec URL E2E test failed", { error: String(err), stack: err.stack });
  process.exit(1);
});

