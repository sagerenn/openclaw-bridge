#!/usr/bin/env bash
#
# Provision a Mattermost server with the two accounts this E2E suite needs:
#   - a bot account  (MATTERMOST_BOT_TOKEN / MATTERMOST_BOT_USER_ID)
#   - a human sender (MATTERMOST_SENDER_TOKEN / MATTERMOST_SENDER_USER_ID)
#
# The bot and sender are created via REST; Personal Access Tokens and Bot
# Tokens are then minted for them. Credentials are written to GITHUB_ENV
# (when present) and also `export`ed to stdout so the script can be sourced
# locally.
#
# Two ways to obtain System Admin privileges (needed to create users/bots):
#   1. Existing admin — set MATTERMOST_ADMIN_USERNAME / MATTERMOST_ADMIN_PASSWORD
#      (and MATTERMOST_ADMIN_EMAIL if the server login requires it). The script
#      logs in via REST.
#   2. Fresh server with no admin — set MM_CONTAINER to the docker container name
#      running Mattermost. The script will `docker exec` the Mattermost CLI to
#      create/guarantee a System Admin first, then log in via REST.
#
# Env vars (all optional unless noted):
#   MATTERMOST_URL                 server base URL (default http://127.0.0.1:8065)
#   MATTERMOST_ADMIN_USERNAME      admin username (default e2e-admin)
#   MATTERMOST_ADMIN_EMAIL         admin email (default e2e-admin@e2e.local)
#   MATTERMOST_ADMIN_PASSWORD      admin password (default E2e-Admin!pass1)
#   MATTERMOST_BOT_USERNAME        bot username (default e2e-bot)
#   MATTERMOST_BOT_DISPLAY_NAME    bot display name (default OpenClaw E2E Bot)
#   MATTERMOST_SENDER_USERNAME     sender username (default e2e-sender)
#   MATTERMOST_SENDER_EMAIL        sender email (default e2e-sender@e2e.local)
#   MATTERMOST_SENDER_PASSWORD     sender password (default E2e-Sender!pass1)
#   MM_CONTAINER                   docker container name with the Mattermost
#                                  binary at /mattermost/bin/mattermost (used
#                                  only to bootstrap an admin on a fresh server)
set -euo pipefail

MM_URL="${MATTERMOST_URL:-http://127.0.0.1:8065}"
MM_URL="${MM_URL%/}"

ADMIN_USER="${MATTERMOST_ADMIN_USERNAME:-e2e-admin}"
ADMIN_EMAIL="${MATTERMOST_ADMIN_EMAIL:-e2e-admin@e2e.local}"
ADMIN_PASS="${MATTERMOST_ADMIN_PASSWORD:-E2e-Admin!pass1}"

BOT_USER="${MATTERMOST_BOT_USERNAME:-e2e-bot}"
BOT_DISPLAY="${MATTERMOST_BOT_DISPLAY_NAME:-OpenClaw E2E Bot}"
SENDER_USER="${MATTERMOST_SENDER_USERNAME:-e2e-sender}"
SENDER_EMAIL="${MATTERMOST_SENDER_EMAIL:-e2e-sender@e2e.local}"
SENDER_PASS="${MATTERMOST_SENDER_PASSWORD:-E2e-Sender!pass1}"

emit() { var="$1"; val="$2"; echo "$var=$val"; if [ -n "${GITHUB_ENV:-}" ]; then echo "$var=$val" >> "$GITHUB_ENV"; fi; }

# ─── Wait for the server to answer /api/v4/ping ─────────────────────────────
echo "Waiting for Mattermost at ${MM_URL} ..."
for i in $(seq 1 120); do
  code="$(curl -s -o /dev/null -w '%{http_code}' "${MM_URL}/api/v4/system/ping" 2>/dev/null || true)"
  if [ "$code" = "200" ]; then echo "Mattermost is up"; break; fi
  sleep 2
done
if [ "$code" != "200" ]; then echo "ERROR: Mattermost not reachable" >&2; exit 1; fi

# ─── Bootstrap a System Admin on a fresh server if requested ────────────────
# ─── Bootstrap a System Admin on a fresh server if requested ────────────────
# The mattermost-preview image ships no `mattermost user` CLI subcommand and a
# directly-DB-inserted password row isn't recognized by the login path, so we
# bootstrap the admin the app-friendly way:
#   1. Register the admin via REST (open-server signup) — the app hashes the
#      password correctly itself, so login will work.
#   2. Flip the user's role to system_admin directly in the bundled Postgres
#      (psql is in the container) — role changes aren't password-sensitive.
#   3. Restart the container so the in-memory user/role cache reloads.
# Requires the container to be started with EnableOpenServer=true +
# EnableUserCreation=true (the CI job sets both).
if [ -n "${MM_CONTAINER:-}" ]; then
  echo "Bootstrapping System Admin in container ${MM_CONTAINER} ..."
  # 1. Register (idempotent: 201 created, 400 if the user already exists).
  reg_code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "${MM_URL}/api/v4/users" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"${ADMIN_EMAIL}\",\"username\":\"${ADMIN_USER}\",\"password\":\"${ADMIN_PASS}\"}")"
  if [ "$reg_code" != "201" ] && [ "$reg_code" != "400" ]; then
    echo "ERROR: admin registration returned ${reg_code} (open-server signup must be enabled)" >&2; exit 1
  fi
  # 2. Promote to system_admin in the DB (psql is in the preview container).
  docker exec -e PGPASSWORD=mostest "$MM_CONTAINER" psql -h localhost -U mmuser -d mattermost_test -v ON_ERROR_STOP=1 -c \
    "UPDATE users SET roles = 'system_user system_admin' WHERE username = '${ADMIN_USER}';" 2>&1 || {
    echo "ERROR: could not promote admin role in DB" >&2; exit 1
  }
  echo "System Admin ensured (${ADMIN_USER})"
  # 3. Restart to refresh the in-memory user/role cache.
  echo "Restarting ${MM_CONTAINER} to refresh the user cache..."
  docker restart "$MM_CONTAINER" >/dev/null
  for i in $(seq 1 120); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "${MM_URL}/api/v4/system/ping" 2>/dev/null || true)"
    if [ "$code" = "200" ]; then echo "${MM_CONTAINER} back up"; break; fi
    sleep 2
  done
fi

# Helper: authenticate against REST and capture the admin's session token.
admin_token="$(curl -s -X POST "${MM_URL}/api/v4/users/login" \
  -H 'Content-Type: application/json' \
  -d "{\"login_id\":\"${ADMIN_USER}\",\"password\":\"${ADMIN_PASS}\"}" \
  -D - -o /dev/null | tr -d '\r' | awk -F': ' 'tolower($1)=="token"{print $2}')"

if [ -z "$admin_token" ]; then
  # The login endpoint may accept email as login_id on some servers.
  admin_token="$(curl -s -X POST "${MM_URL}/api/v4/users/login" \
    -H 'Content-Type: application/json' \
    -d "{\"login_id\":\"${ADMIN_EMAIL}\",\"password\":\"${ADMIN_PASS}\"}" \
    -D - -o /dev/null | tr -d '\r' | awk -F': ' 'tolower($1)=="token"{print $2}')"
fi
if [ -z "$admin_token" ]; then echo "ERROR: admin login failed" >&2; exit 1; fi
AUTH="Authorization: Bearer ${admin_token}"
echo "Admin authenticated"

admin_id="$(curl -s -H "$AUTH" "${MM_URL}/api/v4/users/me" | sed -n 's/^.*"id":"\([^"]*\)".*$/\1/p')"

# ─── Create the sender (human) account + personal access token ──────────────
ensure_user() {
  local email="$1" user="$2" pass="$3"
  curl -s -o /dev/null -X POST "${MM_URL}/api/v4/users" \
    -H "$AUTH" -H 'Content-Type: application/json' \
    -d "{\"email\":\"${email}\",\"username\":\"${user}\",\"password\":\"${pass}\"}" \
    -w '%{http_code}'
}

# Sender
sc="$(ensure_user "$SENDER_EMAIL" "$SENDER_USER" "$SENDER_PASS")"
if [ "$sc" != "201" ] && [ "$sc" != "400" ]; then
  echo "ERROR: sender create returned ${sc}" >&2; exit 1
fi
# Resolve sender id by username (works whether just-created or pre-existing).
sender_id="$(curl -s -H "$AUTH" "${MM_URL}/api/v4/users/username/${SENDER_USER}" \
  | sed -n 's/^.*"id":"\([^"]*\)".*$/\1/p')"
if [ -z "$sender_id" ]; then echo "ERROR: could not resolve sender id" >&2; exit 1; fi
# Sender personal access token (PAT).
sender_token="$(curl -s -X POST "${MM_URL}/api/v4/users/${sender_id}/tokens" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"description":"e2e sender PAT"}' \
  | sed -n 's/^.*"token":"\([^"]*\)".*$/\1/p')"
if [ -z "$sender_token" ]; then echo "ERROR: could not mint sender PAT" >&2; exit 1; fi
echo "Sender ready: ${SENDER_USER} (${sender_id})"

# ─── Create the bot account + bot access token ─────────────────────────────
extract_user_id() { sed -n 's/.*"user_id":"\([^"]*\)".*/\1/p'; }

bot_id="$(curl -s -X POST "${MM_URL}/api/v4/bots" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"username\":\"${BOT_USER}\",\"display_name\":\"${BOT_DISPLAY}\",\"description\":\"e2e bot\"}" \
  | extract_user_id)"
if [ -z "$bot_id" ]; then
  # Bot may already exist by username — look it up.
  bot_id="$(curl -s -H "$AUTH" "${MM_URL}/api/v4/bots/username/${BOT_USER}" \
    | extract_user_id)"
fi
if [ -z "$bot_id" ]; then echo "ERROR: could not create/resolve bot" >&2; exit 1; fi
# Mint a bot access token via the canonical bot-token endpoint (always allowed
# when bot creation is enabled). Falls back to a user PAT for the bot's
# underlying user if the bot-token endpoint is unavailable on this server.
bot_token="$(curl -s -X POST "${MM_URL}/api/v4/bots/${bot_id}/tokens" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"description":"e2e bot token"}' \
  | sed -n 's/^.*"token":"\([^"]*\)".*$/\1/p')"
if [ -z "$bot_token" ]; then
  bot_token="$(curl -s -X POST "${MM_URL}/api/v4/users/${bot_id}/tokens" \
    -H "$AUTH" -H 'Content-Type: application/json' \
    -d '{"description":"e2e bot token"}' \
    | sed -n 's/^.*"token":"\([^"]*\)".*$/\1/p')"
fi
if [ -z "$bot_token" ]; then echo "ERROR: could not mint bot token" >&2; exit 1; fi
echo "Bot ready: ${BOT_USER} (${bot_id})"

# ─── Emit credentials ───────────────────────────────────────────────────────
emit MATTERMOST_URL "$MM_URL"
emit MATTERMOST_BOT_TOKEN "$bot_token"
emit MATTERMOST_BOT_USER_ID "$bot_id"
emit MATTERMOST_SENDER_TOKEN "$sender_token"
emit MATTERMOST_SENDER_USER_ID "$sender_id"
echo "Provisioning complete."
