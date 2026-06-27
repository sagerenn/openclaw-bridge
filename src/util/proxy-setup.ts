/**
 * Proxy setup — routes ALL outbound channel-plugin traffic through an
 * HTTP or SOCKS5 proxy when configured.
 *
 * Channel plugins reach their backends in two ways:
 *   1. `fetch()` (undici-powered) — used by lark/qqbot/weixin for REST API
 *      calls, media fetches, token refresh, long-polls, etc.
 *   2. raw TCP/TLS sockets via the `ws` package — used by qqbot's WebSocket
 *      gateway (and any future plugin that opens a `wss://` connection).
 *
 * To stay fully generic (no plugin-specific code), we install the proxy at
 * the two chokepoints every plugin shares:
 *
 *   • `installFetchProxy(proxyUrl)` — installs an undici global dispatcher
 *     (`ProxyAgent` for http(s):// proxies, `Socks5ProxyAgent` for socks5://),
 *     so every `fetch()` in the process is transparently proxied.
 *   • `patchWebSocketForProxy(proxyUrl)` — rewrites the `createConnection`
 *     default in every installed copy of the `ws` package so `new WebSocket()`
 *     tunnels through the proxy.
 *
 * undici is NOT a direct dependency of this project, but it ships bundled
 * inside the `openclaw` package (`openclaw/node_modules/undici`) AND is the
 * engine behind Node's global `fetch`. We require it through `openclaw`'s
 * resolution so we don't add a dependency. If undici can't be resolved, fetch
 * proxying falls back to environment variables (HTTPS_PROXY etc.), which
 * undici's global `fetch` honors natively.
 */

import { existsSync, readFileSync, writeFileSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import net from "node:net";
import tls from "node:tls";
import { rootLogger } from "./logger.js";

const require = createRequire(import.meta.url);

const log = rootLogger.child("proxy-setup");

// ─── Types ────────────────────────────────────────────────────────────────────

export interface ResolvedProxy {
  /** Normalized URL string, e.g. "socks5://user:pass@host:1080". */
  url: string;
  protocol: "http" | "https" | "socks5";
  hostname: string;
  port: number;
  username?: string;
  password?: string;
}

// ─── URL parsing ──────────────────────────────────────────────────────────────

/**
 * Parse a proxy URL string into a structured ResolvedProxy.
 * Accepts http://, https://, socks5:// and socks:// (aliased to socks5).
 * Throws on unsupported schemes / missing host.
 */
export function parseProxyUrl(raw: string): ResolvedProxy {
  let url: URL;
  try {
    url = new URL(raw);
  } catch (err) {
    throw new Error(`Invalid proxy URL: ${raw} (${String(err)})`);
  }

  let protocol: ResolvedProxy["protocol"];
  switch (url.protocol) {
    case "http:":
      protocol = "http";
      break;
    case "https:":
      protocol = "https";
      break;
    case "socks5:":
    case "socks:":
      protocol = "socks5";
      break;
    default:
      throw new Error(
        `Unsupported proxy scheme "${url.protocol}" — use http://, https://, socks5:// or socks://`,
      );
  }

  if (!url.hostname) {
    throw new Error(`Proxy URL is missing a host: ${raw}`);
  }

  const defaultPort = protocol === "socks5" ? 1080 : protocol === "https" ? 443 : 8080;
  const port = url.port ? parseInt(url.port, 10) : defaultPort;

  return {
    url: raw,
    protocol,
    hostname: url.hostname,
    port,
    username: url.username ? decodeURIComponent(url.username) : undefined,
    password: url.password ? decodeURIComponent(url.password) : undefined,
  };
}

// ─── fetch() proxy (undici global dispatcher) ─────────────────────────────────

let fetchProxyInstalled: string | undefined;

/**
 * Resolve the bundled undici through the `openclaw` package so we don't add a
 * direct dependency. Returns the undici module object or undefined.
 */
function loadUndici(): any | undefined {
  // openclaw ships its own undici under node_modules/openclaw/node_modules/undici
  try {
    return require("undici");
  } catch {
    // Not resolvable at top level — locate it inside the openclaw package.
    // openclaw's main resolves to <pkg>/dist/index.js, but its bundled deps
    // live at the package root's node_modules, so walk up from the resolved
    // main to find <pkg>/node_modules/undici.
    try {
      const openclawPath = require.resolve("openclaw");
      let dir = dirname(openclawPath);
      for (let i = 0; i < 5 && dir; i++) {
        const undiciPath = join(dir, "node_modules", "undici");
        if (existsSync(join(undiciPath, "package.json"))) return require(undiciPath);
        const parent = dirname(dir);
        if (parent === dir) break;
        dir = parent;
      }
    } catch {
      /* ignore */
    }
  }
  return undefined;
}

/**
 * Install an undici global dispatcher so every `fetch()` in the process
 * (including calls inside channel plugins) is routed through the proxy.
 *
 * - http(s):// proxies -> undici.ProxyAgent (HTTP CONNECT tunnel)
 * - socks5:// proxies  -> undici.Socks5ProxyAgent
 *
 * If undici can't be resolved, we set the standard env vars (HTTPS_PROXY etc.)
 * instead — undici's global fetch honors them natively, so this is a safe
 * fallback that still proxies fetch traffic.
 *
 * No-op (and logged) if the same proxy is already installed.
 */
export function installFetchProxy(proxyUrl: string): void {
  if (fetchProxyInstalled === proxyUrl) return;

  const parsed = parseProxyUrl(proxyUrl);
  const undici = loadUndici();

  if (!undici?.setGlobalDispatcher) {
    // Fallback: rely on env vars that undici's global fetch reads automatically.
    log.warn(
      "undici not resolvable — falling back to HTTPS_PROXY/HTTP_PROXY env vars for fetch proxying",
      { proxyUrl },
    );
    process.env.HTTPS_PROXY = process.env.HTTPS_PROXY ?? proxyUrl;
    process.env.HTTP_PROXY = process.env.HTTP_PROXY ?? proxyUrl;
    process.env.ALL_PROXY = process.env.ALL_PROXY ?? proxyUrl;
    fetchProxyInstalled = proxyUrl;
    return;
  }

  let dispatcher: unknown;
  try {
    if (parsed.protocol === "socks5") {
      if (!undici.Socks5ProxyAgent) {
        throw new Error("bundled undici has no Socks5ProxyAgent export");
      }
      dispatcher = new undici.Socks5ProxyAgent(proxyUrl);
    } else {
      dispatcher = new undici.ProxyAgent({
        uri: proxyUrl,
        // undici ProxyAgent accepts auth embedded in the URL; passing the
        // string form keeps username/password handling consistent.
      });
    }
  } catch (err) {
    throw new Error(`Failed to build undici proxy dispatcher: ${String(err)}`);
  }

  undici.setGlobalDispatcher(dispatcher);
  fetchProxyInstalled = proxyUrl;
  log.info("Installed fetch proxy (undici global dispatcher)", {
    protocol: parsed.protocol,
    host: `${parsed.hostname}:${parsed.port}`,
  });
}

// ─── ws proxy (tunnel createConnection) ───────────────────────────────────────

/**
 * Open a plain TCP socket to the proxy host:port.
 * Resolves once the socket is connected; rejects on error.
 */
function connectToProxy(parsed: ResolvedProxy): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const socket = net.connect(parsed.port, parsed.hostname);
    const onError = (err: Error) => {
      socket.removeListener("connect", onConnect);
      socket.destroy();
      reject(err);
    };
    const onConnect = () => {
      socket.removeListener("error", onError);
      resolve(socket);
    };
    socket.once("error", onError);
    socket.once("connect", onConnect);
  });
}

/** Base64-encode "user:pass" for HTTP Basic proxy auth. */
function httpBasicAuth(parsed: ResolvedProxy): string | undefined {
  if (parsed.username === undefined && parsed.password === undefined) return undefined;
  const user = parsed.username ?? "";
  const pass = parsed.password ?? "";
  return Buffer.from(`${user}:${pass}`).toString("base64");
}

/**
 * Tunnel through an HTTP CONNECT proxy: send `CONNECT host:port` and wait for
 * a `200 Connection` response. The returned socket is the raw tunnel ready for
 * TLS framing (the caller wraps it in tls if the target is wss://).
 */
async function httpConnectTunnel(
  parsed: ResolvedProxy,
  targetHost: string,
  targetPort: number,
): Promise<net.Socket> {
  const socket = await connectToProxy(parsed);

  const connectLine =
    `CONNECT ${targetHost}:${targetPort} HTTP/1.1\r\n` +
    `Host: ${targetHost}:${targetPort}\r\n` +
    `Proxy-Connection: Keep-Alive\r\n`;
  const auth = httpBasicAuth(parsed);
  const req = auth ? connectLine + `Proxy-Authorization: Basic ${auth}\r\n\r\n` : connectLine + "\r\n";

  return new Promise((resolve, reject) => {
    let buf = "";
    const cleanup = () => {
      socket.removeListener("data", onData);
      socket.removeListener("error", onError);
    };
    const onData = (chunk: Buffer) => {
      buf += chunk.toString("ascii");
      const headerEnd = buf.indexOf("\r\n\r\n");
      if (headerEnd === -1) return;
      cleanup();
      const statusLine = buf.split("\r\n", 1)[0];
      const m = statusLine.match(/^HTTP\/\d\.\d\s+(\d{3})/);
      if (!m || m[1] !== "200") {
        socket.destroy();
        reject(new Error(`HTTP proxy CONNECT failed: ${statusLine}`));
        return;
      }
      // Drain any bytes already read past the headers (shouldn't be any).
      resolve(socket);
    };
    const onError = (err: Error) => {
      cleanup();
      socket.destroy();
      reject(new Error(`HTTP proxy socket error: ${err.message}`));
    };
    socket.on("data", onData);
    socket.on("error", onError);
    socket.write(req, "ascii");
  });
}

/**
 * Perform a SOCKS5 handshake over an already-connected TCP socket to the
 * proxy, then issue a CONNECT to the target host:port. Returns the same
 * socket, now a raw tunnel to the target.
 *
 * Minimal RFC 1928 implementation — supports no-auth and username/password
 * auth. Does not support BIND or UDP ASSOCIATE (not needed for outbound WS).
 */
async function socks5Handshake(
  parsed: ResolvedProxy,
  socket: net.Socket,
  targetHost: string,
  targetPort: number,
): Promise<net.Socket> {
  const hasAuth = parsed.username !== undefined || parsed.password !== undefined;

  // 1. Greeting: offer no-auth (0x00) and, if creds present, user/pass (0x02).
  const methods = hasAuth ? [0x00, 0x02] : [0x00];
  await socksWrite(socket, Buffer.from([0x05, methods.length, ...methods]));

  // 2. Method selection.
  const methodResp = await socksRead(socket, 2);
  if (methodResp[0] !== 0x05) throw new Error("SOCKS5: bad version in method reply");
  const method = methodResp[1];
  if (method === 0xff) throw new Error("SOCKS5: proxy rejected all auth methods");

  // 3. Username/password sub-negotiation (RFC 1929) if required.
  if (method === 0x02) {
    const user = Buffer.from(parsed.username ?? "");
    const pass = Buffer.from(parsed.password ?? "");
    if (user.length > 255 || pass.length > 255) throw new Error("SOCKS5: user/pass too long");
    const authReq = Buffer.concat([
      Buffer.from([0x01, user.length]),
      user,
      Buffer.from([pass.length]),
      pass,
    ]);
    await socksWrite(socket, authReq);
    const authResp = await socksRead(socket, 2);
    if (authResp[0] !== 0x01) throw new Error("SOCKS5: bad auth version");
    if (authResp[1] !== 0x00) throw new Error("SOCKS5: authentication failed");
  } else if (method !== 0x00) {
    throw new Error(`SOCKS5: unsupported auth method ${method}`);
  }

  // 4. CONNECT request. Use DOMAINNAME (0x03) addressing so DNS happens at
  //    the proxy — avoids local DNS leaks and supports hostnames.
  const hostBuf = Buffer.from(targetHost);
  if (hostBuf.length > 255) throw new Error("SOCKS5: target host too long");
  const req = Buffer.concat([
    Buffer.from([0x05, 0x01, 0x00, 0x03, hostBuf.length]),
    hostBuf,
    Buffer.from([(targetPort >> 8) & 0xff, targetPort & 0xff]),
  ]);
  await socksWrite(socket, req);

  // 5. Reply: VER REP RSV ATYP BND.ADDR BND.PORT
  const repHead = await socksRead(socket, 4);
  if (repHead[0] !== 0x05) throw new Error("SOCKS5: bad version in reply");
  if (repHead[1] !== 0x00) throw new Error(`SOCKS5: connect failed (code ${repHead[1]})`);

  const atyp = repHead[3];
  // Drain the bound-address field so the socket position sits at the start of
  // the tunnel data stream.
  let addrLen: number;
  switch (atyp) {
    case 0x01: addrLen = 4; break;        // IPv4
    case 0x03: addrLen = (await socksRead(socket, 1))[0]; break; // domain
    case 0x04: addrLen = 16; break;       // IPv6
    default: throw new Error(`SOCKS5: unsupported ATYP ${atyp}`);
  }
  await socksRead(socket, addrLen + 2); // bound addr + port
  return socket;
}

function socksWrite(socket: net.Socket, buf: Buffer): Promise<void> {
  return new Promise((resolve, reject) => {
    socket.write(buf, (err) => (err ? reject(err) : resolve()));
  });
}

function socksRead(socket: net.Socket, n: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let received = 0;
    const onEnd = () => reject(new Error("SOCKS5: socket closed during handshake"));
    const onError = (err: Error) => reject(new Error(`SOCKS5 socket error: ${err.message}`));
    const onData = (chunk: Buffer) => {
      chunks.push(chunk);
      received += chunk.length;
      if (received >= n) {
        socket.removeListener("data", onData);
        socket.removeListener("end", onEnd);
        socket.removeListener("error", onError);
        const full = Buffer.concat(chunks);
        // Push any excess back; in practice we read exactly, but be safe.
        if (received > n) socket.unshift(full.subarray(n));
        resolve(full.subarray(0, n));
      }
    };
    socket.on("data", onData);
    socket.once("end", onEnd);
    socket.once("error", onError);
  });
}

/**
 * A `createConnection` we hand to `ws` (via Node's http/https request layer)
 * so its `new WebSocket()` connections are tunneled through the proxy without
 * any plugin changes.
 *
 * Node's request layer calls createConnection in CALLBACK style:
 *   createConnection(opts, oncreate)
 * We build the tunnel async and hand the live socket to `oncreate`. ws reads
 * whether the target is secure (`wss://`) and sets `_secure` on the opts via
 * our ws patch, so we know whether to wrap in TLS. See createProxyConnectionCallback.
 */
export function createProxyConnection(
  proxyUrl: string,
  options: net.NetConnectOpts & tls.ConnectionOptions,
  oncreate?: (err: Error | null, socket?: net.Socket) => void,
): net.Socket {
  return createProxyConnectionCallback(proxyUrl, options, oncreate);
}

/**
 * Callback-style createConnection used by ws via http.request(opts). Node's
 * request layer invokes `createConnection(opts, oncreate)`: we resolve the
 * tunnel and call `oncreate(err, socket)` with the live socket so the WS
 * upgrade handshake runs over the proxied connection.
 */
export function createProxyConnectionCallback(
  proxyUrl: string,
  options: net.NetConnectOpts & tls.ConnectionOptions,
  oncreate?: (err: Error | null, socket?: net.Socket) => void,
): net.Socket {
  const parsed = parseProxyUrl(proxyUrl);
  const targetHost = String(options.host ?? options.servername ?? "");
  const targetPort = Number(options.port ?? 443);
  const secure = Boolean((options as any)._secure);
  const servername =
    options.servername && options.servername !== ""
      ? options.servername
      : net.isIP(targetHost) ? "" : targetHost;

  // Placeholder returned synchronously; only used if a caller ignores the
  // callback form. Node's http layer never uses it when oncreate is supplied.
  const placeholder = new net.Socket();

  (async () => {
    let raw: net.Socket;
    if (parsed.protocol === "socks5") {
      raw = await connectToProxy(parsed);
      raw = await socks5Handshake(parsed, raw, targetHost, targetPort);
    } else {
      raw = await httpConnectTunnel(parsed, targetHost, targetPort);
    }
    if (!secure) {
      oncreate?.(null, raw);
      return;
    }
    const secureSock = tls.connect({
      socket: raw,
      servername: servername || undefined,
      ...(options as tls.ConnectionOptions),
    });
    oncreate?.(null, secureSock);
  })().catch((err) => {
    oncreate?.(err as Error);
    placeholder.destroy(err as Error);
  });

  return placeholder;
}

// ─── ws patching ──────────────────────────────────────────────────────────────

/**
 * The marker line we inject into ws's `lib/websocket.js` to route both
 * `tlsConnect` (wss://) and `netConnect` (ws://) through our proxy tunnel.
 *
 * ws sets `opts.createConnection = opts.createConnection || (isSecure ? tlsConnect : netConnect)`
 * We prepend a custom default that, when a proxy is active, replaces the
 * default with our tunnel connector. The connector is exposed on a global
 * (`globalThis.__OPENCLAW_BRIDGE_PROXY__`) so the patched ws code can reach it
 * without resolving our TS source. The marker enables idempotent patching.
 */
const WS_PATCH_MARKER = "/* openclaw-bridge proxy patch */";

/** Global namespace the patched ws code reads at connect-time. */
const PROXY_GLOBAL = "__OPENCLAW_BRIDGE_PROXY__";
type ProxyGlobal = { url?: string; secure?: boolean; connect?: (...args: any[]) => any };
function getProxyGlobal(): ProxyGlobal {
  return ((globalThis as any)[PROXY_GLOBAL] ??= {} as ProxyGlobal);
}

/**
 * Build the patch snippet injected just above the createConnection assignment.
 * Reads the global at call time so the proxy URL/connect function can be set
 * after ws is patched (plugins load lazily) and changed at runtime.
 */
function wsPatchSnippet(): string {
  return `${WS_PATCH_MARKER}
  var __obProxy = globalThis.${PROXY_GLOBAL};
  if (__obProxy && __obProxy.url && __obProxy.connect && typeof opts.createConnection !== 'function' && opts.socketPath === undefined && typeof opts.port !== 'string') {
    opts.createConnection = function (ccOpts, oncreate) {
      return __obProxy.connect(__obProxy.url, ccOpts, oncreate, isSecure);
    };
  }`;
}

/**
 * The ws source line we anchor the patch on. Present unchanged in every
 * installed ws 8.x copy:
 *     opts.createConnection =
 *       opts.createConnection || (isSecure ? tlsConnect : netConnect);
 */
const WS_ANCHOR =
  "  opts.createConnection =\n    opts.createConnection || (isSecure ? tlsConnect : netConnect);";

/**
 * Wire the global proxy namespace used by the patched ws copies: the proxy
 * URL and a `connect(url, opts, oncreate, isSecure)` function bridging to our
 * SOCKS5/HTTP-CONNECT tunnel.
 */
function wireProxyGlobal(proxyUrl: string): void {
  const g = getProxyGlobal();
  g.url = proxyUrl;
  g.connect = (
    url: string,
    opts: net.NetConnectOpts & tls.ConnectionOptions,
    oncreate: (err: Error | null, socket?: net.Socket) => void,
    isSecure: boolean,
  ) => {
    // Stamp the secure flag so createProxyConnectionCallback wraps in TLS.
    (opts as any)._secure = isSecure;
    return createProxyConnectionCallback(url, opts, oncreate);
  };
}

/** Find every installed copy of the `ws` package under node_modules. */
function findWsCopies(): string[] {
  const here = dirname(fileURLToPath(import.meta.url));
  // dist/util/proxy-setup.js -> project root is two levels up (dist/util -> dist -> root)
  const projectRoot = dirname(dirname(here));
  const nmRoot = join(projectRoot, "node_modules");
  const roots = existsSync(nmRoot) ? [nmRoot] : [];
  // Fallback for when the bridge is invoked from another cwd (e.g. global bin).
  const cwdNm = join(process.cwd(), "node_modules");
  if (existsSync(cwdNm) && !roots.includes(cwdNm)) roots.push(cwdNm);
  const copies: string[] = [];

  const isWsLib = (file: string): boolean => {
    try {
      const pkg = JSON.parse(readFileSync(join(dirname(dirname(file)), "package.json"), "utf-8"));
      return pkg.name === "ws";
    } catch {
      return false;
    }
  };

  const walk = (dir: string) => {
    let entries;
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      if (!e.isDirectory()) continue;
      const sub = join(dir, e.name);

      // A ws copy nested here: <dir>/<pkg>/node_modules/ws/lib/websocket.js
      const wsFile = join(sub, "node_modules", "ws", "lib", "websocket.js");
      if (existsSync(wsFile) && isWsLib(wsFile)) copies.push(wsFile);

      // Recurse into nested node_modules and scoped dirs.
      const nestedNm = join(sub, "node_modules");
      if (existsSync(nestedNm)) walk(nestedNm);
      if (e.name.startsWith("@")) walk(sub);
    }
  };

  for (const nmRoot of roots) {
    // Top-level ws copy in this node_modules.
    const topWs = join(nmRoot, "ws", "lib", "websocket.js");
    if (existsSync(topWs) && isWsLib(topWs)) copies.push(topWs);

    walk(nmRoot);
  }

  return [...new Set(copies)];
}

/**
 * Patch a single ws `lib/websocket.js` file with our proxy default.
 * Idempotent: if the marker is already present and the anchor unchanged, it's
 * a no-op. If the anchor moved (ws upgraded), we re-apply against the new
 * anchor. Returns true if the file was modified.
 */
function patchWsFile(filePath: string): boolean {
  if (!existsSync(filePath)) return false;
  const src = readFileSync(filePath, "utf-8");

  // Already patched and anchor intact.
  if (src.includes(WS_PATCH_MARKER)) {
    if (src.includes(WS_ANCHOR)) return false; // healthy, no-op
    // Anchor drifted — remove our old snippet so we can re-anchor cleanly.
    const markerIdx = src.indexOf(WS_PATCH_MARKER);
    const anchorStart = src.lastIndexOf("\n", markerIdx - 1) + 1;
    const anchorEnd = src.indexOf("\n", src.indexOf("netConnect);", markerIdx)) + 1;
    const cleaned = src.slice(0, anchorStart) + src.slice(anchorEnd);
    return writeAndCheck(filePath, cleaned);
  }

  if (!src.includes(WS_ANCHOR)) {
    log.warn("ws createConnection anchor not found — skipping patch (ws version unsupported)", {
      file: filePath,
    });
    return false;
  }

  const patched = src.replace(WS_ANCHOR, wsPatchSnippet() + "\n" + WS_ANCHOR);
  return writeAndCheck(filePath, patched);
}

function writeAndCheck(filePath: string, content: string): boolean {
  try {
    writeFileSync(filePath, content, "utf-8");
    return true;
  } catch (err) {
    log.warn("Failed to write ws patch", { file: filePath, error: String(err) });
    return false;
  }
}

/**
 * Patch every installed copy of the `ws` package so WebSocket connections
 * route through the active proxy. Safe to call repeatedly; idempotent.
 * Returns the number of files modified.
 */
export function patchWebSocketForProxy(): number {
  const copies = findWsCopies();
  if (copies.length === 0) {
    log.debug("No ws package copies found to patch");
    return 0;
  }
  let modified = 0;
  for (const file of copies) {
    if (patchWsFile(file)) {
      modified++;
      log.info("Patched ws for proxy support", { file });
    }
  }
  return modified;
}

/**
 * Remove our proxy snippet from all patched ws copies (e.g. when the proxy is
 * disabled). Idempotent.
 */
export function unpatchWebSocketForProxy(): number {
  const copies = findWsCopies();
  let modified = 0;
  for (const file of copies) {
    if (!existsSync(file)) continue;
    const src = readFileSync(file, "utf-8");
    if (!src.includes(WS_PATCH_MARKER)) continue;
    const markerIdx = src.indexOf(WS_PATCH_MARKER);
    const start = src.lastIndexOf("\n", markerIdx - 1) + 1;
    const anchorLineStart = src.indexOf(WS_ANCHOR, markerIdx);
    if (anchorLineStart === -1) continue;
    const cleaned = src.slice(0, start) + src.slice(anchorLineStart);
    if (writeAndCheck(file, cleaned)) modified++;
  }
  return modified;
}

// ─── Orchestrator ─────────────────────────────────────────────────────────────

/**
 * Resolve an effective proxy URL from explicit config OR the standard env
 * vars (HTTPS_PROXY / HTTP_PROXY / ALL_PROXY, case-insensitive). Returns
 * undefined when no proxy is configured anywhere.
 *
 * Used when the bridge has no explicit `proxy` config so it still honors a
 * user's shell proxy settings.
 */
export function resolveProxyFromEnv(): string | undefined {
  const env = process.env;
  const candidates = [
    env.HTTPS_PROXY,
    env.https_proxy,
    env.HTTP_PROXY,
    env.http_proxy,
    env.ALL_PROXY,
    env.all_proxy,
  ];
  for (const c of candidates) {
    if (c && c.trim()) return c.trim();
  }
  return undefined;
}

let activeProxyUrl: string | undefined;

/** Returns the proxy URL currently in effect, if any. */
export function getActiveProxyUrl(): string | undefined {
  return activeProxyUrl;
}

/**
 * Install and activate a proxy for all outbound channel-plugin traffic:
 *   - fetch() via the undici global dispatcher
 *   - WebSocket (`ws`) via the patched createConnection
 *
 * Pass undefined to disable the proxy (restores direct connections). Safe to
 * call multiple times — switching proxies re-installs fetch's dispatcher and
 * just updates the global URL the ws patch reads.
 *
 * This is the single entry point the bridge calls at startup (for the global
 * proxy) and whenever a per-channel/per-account override is the most specific
 * one in effect. Because fetch's global dispatcher is process-wide, the proxy
 * is necessarily global for fetch traffic; ws traffic reads the global URL at
 * connect-time, so per-account ws proxying is honored for connections opened
 * after the switch.
 */
export function setupProxy(proxyUrl: string | undefined): void {
  activeProxyUrl = proxyUrl;

  if (!proxyUrl) {
    // Disable: clear the ws global and leave fetch on its (env) defaults.
    const g = getProxyGlobal();
    g.url = undefined;
    log.info("Proxy disabled — outbound traffic connects directly");
    return;
  }

  const parsed = parseProxyUrl(proxyUrl);
  installFetchProxy(proxyUrl);
  wireProxyGlobal(proxyUrl);
  patchWebSocketForProxy();
  log.info("Proxy activated for channel-plugin traffic", {
    protocol: parsed.protocol,
    host: `${parsed.hostname}:${parsed.port}`,
  });
}



