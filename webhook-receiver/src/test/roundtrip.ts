/**
 * End-to-end round-trip test for the webhook receiver.
 *
 * Starts the core Receiver in-process with an in-memory store (no network),
 * then drives it with Web Fetch Requests to prove the full flow:
 *   1. register a token -> POST an IM event -> poll it back -> ack -> poll empty
 *   2. HMAC signature verification (reject bad sig)
 *   3. poll-bearer auth (reject unauthed poll)
 *
 * Run: node dist/test/roundtrip.js
 */

import {
  Receiver,
  TokenRegistry,
  InMemoryMessageStore,
  computeHmac,
  type StoredMessage,
} from "../core.js";

interface TestResult { name: string; pass: boolean; detail?: string }
const results: TestResult[] = [];
function assert(name: string, cond: boolean, detail?: string): void {
  results.push({ name, pass: cond, detail });
  console.log(`${cond ? "PASS" : "FAIL"}  ${name}${detail ? "  — " + detail : ""}`);
}

function req(method: string, path: string, opts: { body?: string; headers?: Record<string, string> } = {}): Request {
  return new Request(`http://receiver.test${path}`, {
    method,
    headers: opts.headers ?? {},
    body: opts.body,
  });
}

async function main(): Promise<void> {
  // ── Test 1: basic round-trip (no HMAC, no poll bearer) ──────────────────
  {
    const tokens = new TokenRegistry();
    const store = new InMemoryMessageStore();
    const r = new Receiver(store, tokens);
    const token = "tok-basic";
    tokens.register(token, { channel: "msteams", accountId: "default" });

    // IM posts an event
    const postRes = await r.handle(req("POST", `/webhook/${token}`, {
      body: JSON.stringify({ text: "hello teams", from: { id: "u1", name: "Alice" }, conversation: { id: "c1" } }),
      headers: { "content-type": "application/json" },
    }));
    const postBody = await postRes.json() as { ok: boolean; id: string };
    assert("1a POST returns 200 + ok", postRes.status === 200 && postBody.ok === true, `status=${postRes.status}`);

    // Bridge polls
    const pollRes = await r.handle(req("GET", `/webhook/${token}/poll`));
    const pollBody = await pollRes.json() as { channel: string; accountId: string; messages: StoredMessage[] };
    assert("1b poll returns 1 message", pollBody.messages.length === 1, `got ${pollBody.messages.length}`);
    assert("1c message normalized", pollBody.messages[0].text === "hello teams"
      && pollBody.messages[0].channel === "msteams"
      && pollBody.messages[0].senderId === "u1"
      && pollBody.messages[0].senderName === "Alice"
      && pollBody.messages[0].chatId === "c1", JSON.stringify(pollBody.messages[0]));
    const firstId = pollBody.messages[0].id;

    // Second event before ack — both should still be present (FIFO buffer)
    await r.handle(req("POST", `/webhook/${token}`, {
      body: JSON.stringify({ text: "second" }), headers: { "content-type": "application/json" },
    }));
    const poll2Res = await r.handle(req("GET", `/webhook/${token}/poll`));
    const poll2Body = await poll2Res.json() as { messages: StoredMessage[] };
    assert("1d poll returns both (not yet acked)", poll2Body.messages.length === 2, `got ${poll2Body.messages.length}`);

    // Ack the first id — only oldest <= id should be trimmed
    const poll3Res = await r.handle(req("GET", `/webhook/${token}/poll?ack=${firstId}`));
    const poll3Body = await poll3Res.json() as { messages: StoredMessage[] };
    assert("1e after ack(first) only 1 remains", poll3Body.messages.length === 1, `got ${poll3Body.messages.length}`);
    const secondId = poll3Body.messages[0].id;

    // Ack the rest
    const poll4Res = await r.handle(req("GET", `/webhook/${token}/poll?ack=${secondId}`));
    const poll4Body = await poll4Res.json() as { messages: StoredMessage[] };
    assert("1f after ack(rest) queue is empty", poll4Body.messages.length === 0, `got ${poll4Body.messages.length}`);

    // Unknown token is 404 (not enumerable)
    const unk = await r.handle(req("GET", "/webhook/does-not-exist/poll"));
    assert("1g unknown token -> 404", unk.status === 404, `status=${unk.status}`);
  }

  // ── Test 2: HMAC signature verification ─────────────────────────────────
  {
    const tokens = new TokenRegistry();
    const store = new InMemoryMessageStore();
    const secret = "shh";
    const r = new Receiver(store, tokens, { sharedSecret: secret, signatureHeader: "x-signature" });
    const token = "tok-hmac";
    tokens.register(token, { channel: "slack", accountId: "default" });

    const body = JSON.stringify({ text: "signed" });
    const goodSig = await computeHmac(secret, body, "hex");

    // Bad signature rejected
    const badRes = await r.handle(req("POST", `/webhook/${token}`, {
      body, headers: { "content-type": "application/json", "x-signature": "deadbeef" },
    }));
    assert("2a bad signature -> 401", badRes.status === 401, `status=${badRes.status}`);

    // Missing signature rejected
    const missRes = await r.handle(req("POST", `/webhook/${token}`, {
      body, headers: { "content-type": "application/json" },
    }));
    assert("2b missing signature -> 401", missRes.status === 401, `status=${missRes.status}`);

    // Good signature accepted
    const okRes = await r.handle(req("POST", `/webhook/${token}`, {
      body, headers: { "content-type": "application/json", "x-signature": goodSig },
    }));
    assert("2c good signature -> 200", okRes.status === 200, `status=${okRes.status}`);
  }

  // ── Test 3: poll-bearer auth ─────────────────────────────────────────────
  {
    const tokens = new TokenRegistry();
    const store = new InMemoryMessageStore();
    const r = new Receiver(store, tokens, { pollBearerToken: "poll-secret" });
    const token = "tok-bearer";
    tokens.register(token, { channel: "x", accountId: "default" });

    // No bearer -> 401
    const noAuth = await r.handle(req("GET", `/webhook/${token}/poll`));
    assert("3a poll without bearer -> 401", noAuth.status === 401, `status=${noAuth.status}`);

    // Wrong bearer -> 401
    const wrongAuth = await r.handle(req("GET", `/webhook/${token}/poll`, {
      headers: { authorization: "Bearer wrong" },
    }));
    assert("3b poll wrong bearer -> 401", wrongAuth.status === 401, `status=${wrongAuth.status}`);

    // Right bearer -> 200
    const okAuth = await r.handle(req("GET", `/webhook/${token}/poll`, {
      headers: { authorization: "Bearer poll-secret" },
    }));
    assert("3c poll right bearer -> 200", okAuth.status === 200, `status=${okAuth.status}`);

    // Right bearer via query -> 200
    const okQuery = await r.handle(req("GET", `/webhook/${token}/poll?token=poll-secret`));
    assert("3d poll bearer via query -> 200", okQuery.status === 200, `status=${okQuery.status}`);
  }

  // ── Test 4: healthz + banner ─────────────────────────────────────────────
  {
    const r = new Receiver(new InMemoryMessageStore(), new TokenRegistry());
    const h = await r.handle(req("GET", "/healthz"));
    const hb = (await h.json()) as { ok?: boolean };
    assert("4a /healthz -> 200 ok", h.status === 200 && hb.ok === true);
  }

  const failed = results.filter((x) => !x.pass);
  console.log("");
  console.log(`${results.length - failed.length}/${results.length} passed`);
  if (failed.length > 0) {
    console.error("FAILURES:");
    for (const f of failed) console.error(`  - ${f.name}${f.detail ? "  — " + f.detail : ""}`);
    process.exit(1);
  }
  console.log("All round-trip tests passed.");
}

void main();
