#!/usr/bin/env node
/**
 * Token management CLI for the webhook receiver.
 *
 *   node dist/cli.js register <channel> [accountId] [--token <t>]
 *       Register a new webhook token for a channel/account. Generates a random
 *       unguessable token if --token is omitted. Prints the full webhook URL
 *       and the binding — register THIS url with the IM platform, and configure
 *       the bridge to poll it.
 *
 *   node dist/cli.js list
 *       Print all registered tokens + bindings + the poll url.
 *
 *   node dist/cli.js revoke <token>
 *       Remove a token binding.
 *
 *   node dist/cli.js poll <token> [--bearer <t>]
 *       Drain buffered messages for a token (debug helper). Prints the JSON
 *       poll response.
 *
 * Tokens are persisted to wh-tokens.json (chmod 600) alongside the server.
 * On serverless, use env (WH_TOKENS) instead — the filesystem is read-only.
 */

import { TokenRegistry } from "./core.js";
import { PersistentTokenRegistry } from "./token-store.js";

function main(): void {
  const [cmd, ...rest] = process.argv.slice(2);
  const tokens = new PersistentTokenRegistry();

  switch (cmd) {
    case "register": {
      const channel = rest[0];
      if (!channel) die("usage: register <channel> [accountId] [--token <t>]");
      const accountId = rest[1] && !rest[1].startsWith("--") ? rest[1] : "default";
      const tokenFlagIdx = rest.indexOf("--token");
      const token = tokenFlagIdx >= 0 ? rest[tokenFlagIdx + 1] : TokenRegistry.generate();
      tokens.register(token, { channel, accountId });
      const base = process.env.WH_BASE_URL ?? `http://localhost:${process.env.PORT ?? "9301"}`;
      console.log("Registered webhook token:");
      console.log(JSON.stringify({ token, channel, accountId }, null, 2));
      console.log("");
      console.log(`Inbound URL (give to IM platform):  ${base}/webhook/${token}`);
      console.log(`Poll URL     (bridge polls this):   ${base}/webhook/${token}/poll`);
      break;
    }
    case "list": {
      const list = tokens.listWithBindings();
      if (list.length === 0) { console.log("(no tokens registered)"); break; }
      const base = process.env.WH_BASE_URL ?? `http://localhost:${process.env.PORT ?? "9301"}`;
      for (const { token, binding } of list) {
        console.log(`${token}  ->  ${binding.channel}:${binding.accountId}`);
        console.log(`  inbound: ${base}/webhook/${token}`);
        console.log(`  poll:    ${base}/webhook/${token}/poll`);
      }
      break;
    }
    case "revoke": {
      const token = rest[0];
      if (!token) die("usage: revoke <token>");
      // PersistentTokenRegistry stores on register; revoke = re-save without it.
      tokens.revoke(token);
      console.log("Revoked", token);
      break;
    }
    default:
      die(`unknown command "${cmd ?? ""}". commands: register | list | revoke | poll`);
  }
}

function die(msg: string): never {
  console.error(msg);
  process.exit(1);
}

void main();
