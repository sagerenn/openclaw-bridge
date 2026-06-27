/**
 * Single source of truth for the bridge's HTTP API routes.
 *
 * Both the live HTTP request dispatcher in `bridge-server.ts` AND the OpenAPI
 * spec generator consume this table, so adding a new route here is the ONLY
 * change needed — the route becomes live and appears in the spec at
 * `/spec/openapi.json` automatically. No separate spec edit required.
 *
 * Route paths use `{param}` placeholders (OpenAPI style). Each route carries
 * enough metadata to (a) match an incoming request and (b) document itself.
 */

// ─── Types ───────────────────────────────────────────────────────────────────

export interface HttpRouteParam {
  name: string;
  in: "path" | "query";
  required: boolean;
  description: string;
  schema: { type: "string" | "integer" | "boolean"; default?: number | boolean };
}

export interface HttpRoute {
  /** OpenAPI-style path, e.g. "/plugin/{channelId}/{accountId}/qr". */
  path: string;
  method: "GET" | "POST";
  operationId: string;
  summary: string;
  description: string;
  tags: string[];
  /** Path/query params (deduced from {placeholders} + explicit query params). */
  params: HttpRouteParam[];
  /** Response status -> mediaType -> schema ref or inline schema. */
  responses: Record<string, {
    description: string;
    content?: Record<string, { schema: unknown }>;
  }>;
  /**
   * Discriminator key consumed by the dispatcher. Maps this route to the
   * handler in bridge-server.ts. New routes add a new key + handler branch.
   */
  handler: HttpRouteHandler;
}

export type HttpRouteHandler =
  | "qr-html"
  | "qr-json"
  | "qr-status"
  | "spec-asyncapi"
  | "spec-openapi";

// ─── Shared param/schemas ────────────────────────────────────────────────────

const channelIdParam: HttpRouteParam = {
  name: "channelId", in: "path", required: true,
  description: "Channel id matching the plugin's openclaw.plugin.json (e.g. openclaw-weixin)",
  schema: { type: "string" },
};
const accountIdParam: HttpRouteParam = {
  name: "accountId", in: "path", required: true,
  description: "Account id within the channel (e.g. default)",
  schema: { type: "string" },
};

const errorResponse = {
  description: "Error response",
  content: { "application/json": { schema: { $ref: "#/components/schemas/Error" } } },
};

// ─── Route table ─────────────────────────────────────────────────────────────

export const HTTP_ROUTES: HttpRoute[] = [
  {
    path: "/plugin/{channelId}/{accountId}/qr",
    method: "GET",
    operationId: "startQrLoginHtml",
    summary: "Start QR login (HTML)",
    description:
      "Starts a QR login flow for the given channel account and returns an HTML page that renders the QR code and auto-polls for login status. Open in a browser to scan.",
    tags: ["QR login"],
    params: [
      channelIdParam,
      accountIdParam,
      { name: "force", in: "query", required: false, description: "Force a new QR even if one is already active", schema: { type: "boolean" } },
    ],
    responses: {
      "200": { description: "HTML page embedding the QR code image", content: { "text/html": { schema: { type: "string" } } } },
      "500": errorResponse,
      "502": errorResponse,
    },
    handler: "qr-html",
  },
  {
    path: "/plugin/{channelId}/{accountId}/qr/json",
    method: "GET",
    operationId: "startQrLoginJson",
    summary: "Start QR login (JSON)",
    description: "Starts a QR login flow and returns the result as JSON for programmatic use.",
    tags: ["QR login"],
    params: [
      channelIdParam,
      accountIdParam,
      { name: "force", in: "query", required: false, description: "Force a new QR even if one is already active", schema: { type: "boolean" } },
    ],
    responses: {
      "200": { description: "QR login started", content: { "application/json": { schema: { $ref: "#/components/schemas/QrStartResponse" } } } },
      "500": errorResponse,
    },
    handler: "qr-json",
  },
  {
    path: "/plugin/{channelId}/{accountId}/qr/status",
    method: "GET",
    operationId: "pollQrLoginStatus",
    summary: "Poll QR login status",
    description:
      "Long-poll endpoint that blocks until the QR login completes or times out. Pass the sessionKey from the /qr or /qr/json response.",
    tags: ["QR login"],
    params: [
      channelIdParam,
      accountIdParam,
      { name: "sessionKey", in: "query", required: true, description: "Session key from the start response", schema: { type: "string" } },
      { name: "timeoutMs", in: "query", required: false, description: "Maximum wait time in milliseconds", schema: { type: "integer", default: 120000 } },
    ],
    responses: {
      "200": { description: "Login status (waiting or completed)", content: { "application/json": { schema: { $ref: "#/components/schemas/QrWaitResponse" } } } },
      "500": errorResponse,
    },
    handler: "qr-status",
  },
  {
    path: "/spec/asyncapi.json",
    method: "GET",
    operationId: "getAsyncApiSpec",
    summary: "AsyncAPI specification (WebSocket API)",
    description: "Machine-readable AsyncAPI document describing the WebSocket protocol, generated live from the bridge's protocol definitions.",
    tags: ["Spec"],
    params: [],
    responses: {
      "200": { description: "AsyncAPI 2.6.0 document", content: { "application/json": { schema: { type: "object" } } } },
    },
    handler: "spec-asyncapi",
  },
  {
    path: "/spec/openapi.json",
    method: "GET",
    operationId: "getOpenApiSpec",
    summary: "OpenAPI specification (HTTP API)",
    description: "Machine-readable OpenAPI document describing the HTTP API, generated live from this route table.",
    tags: ["Spec"],
    params: [],
    responses: {
      "200": { description: "OpenAPI 3.1.0 document", content: { "application/json": { schema: { type: "object" } } } },
    },
    handler: "spec-openapi",
  },
];

// ─── Matching ────────────────────────────────────────────────────────────────

/**
 * Convert an OpenAPI-style path into a RegExp that captures path params by name.
 * e.g. "/plugin/{channelId}/{accountId}/qr" -> /\/plugin\/([^/]+)\/([^/]+)\/qr$/
 */
export function routePathToRegex(path: string): { regex: RegExp; paramNames: string[] } {
  const paramNames: string[] = [];
  const pattern = path
    .replace(/[.+*?^${}()|[\]\\]/g, "\\$&")
    .replace(/\\\{([^}]+)\\\}/g, (_m, name) => {
      paramNames.push(name);
      return "([^/]+)";
    });
  return { regex: new RegExp(`^${pattern}$`), paramNames };
}

export interface MatchedRoute {
  route: HttpRoute;
  /** Map of path-param name -> captured value. */
  pathParams: Record<string, string>;
}

/**
 * Match an incoming request method+pathname against the route table.
 * Returns the first match plus extracted path params.
 */
export function matchHttpRoute(method: string, pathname: string): MatchedRoute | undefined {
  for (const route of HTTP_ROUTES) {
    if (route.method !== method.toUpperCase()) continue;
    const { regex, paramNames } = routePathToRegex(route.path);
    const match = pathname.match(regex);
    if (!match) continue;
    const pathParams: Record<string, string> = {};
    paramNames.forEach((name, i) => {
      pathParams[name] = decodeURIComponent(match[i + 1]);
    });
    return { route, pathParams };
  }
  return undefined;
}
