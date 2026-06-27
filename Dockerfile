# syntax=docker/dockerfile:1.7

# ─── Builder ──────────────────────────────────────────────────────────────────
# Install all deps, compile dist/, run the lark patch (postinstall), then copy
# the built artifacts into the runtime stage.
FROM node:24-bookworm-slim AS builder

WORKDIR /app

# Install prod + dev deps. package-lock must be present (gitignored? no: it is
# committed) so `npm ci` is reproducible. postinstall builds dist/ + patches
# the lark plugin.
COPY package.json package-lock.json ./
RUN npm ci

# Copy sources and (re)build to be safe; postinstall already built once.
COPY tsconfig.json ./
COPY src/ ./src/
RUN npm run build

# Re-run the lark patch in case sources changed the targets; idempotent.
RUN node dist/util/patch-lark-plugin.js

# Prune dev dependencies so they don't ship in the runtime image.
RUN npm prune --omit=dev

# ─── Runtime ──────────────────────────────────────────────────────────────────
FROM node:24-bookworm-slim AS runtime

# Security: create a non-root user and drop to it.
ENV NODE_ENV=production \
    PORT=9300
WORKDIR /app

# tini PID 1 for clean signal handling (WS server + child plugins).
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 1001 openclaw \
 && useradd  --system --uid 1001 --gid openclaw --create-home --home-dir /home/openclaw openclaw

COPY --from=builder --chown=openclaw:openclaw /app/node_modules ./node_modules
COPY --from=builder --chown=openclaw:openclaw /app/dist ./dist
COPY --from=builder --chown=openclaw:openclaw /app/package.json ./package.json

# Default empty config — mount a real config.json at /app/config.json in prod.
# loadConfig() falls back to defaults (host 0.0.0.0, port 9300, path /bridge)
# when no file is present.
COPY --chown=openclaw:openclaw config.example.json ./config.example.json

USER openclaw

EXPOSE 9300

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+process.env.PORT+'/spec/openapi.json').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["node", "dist/server.js"]
