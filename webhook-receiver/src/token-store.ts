/**
 * File-persisted token registry for the standalone Node server.
 *
 * On serverless you load tokens from env (storage.ts:loadTokenBindings) because
 * the filesystem is read-only. For the self-hosted Node server we persist to
 * wh-tokens.json so `cli.js register` survives restarts.
 */

import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { resolve } from "node:path";
import type { TokenBinding } from "./core.js";
import { TokenRegistry } from "./core.js";

interface RegistryFile {
  recordBindings: Record<string, TokenBinding>;
}

/**
 * Wraps TokenRegistry with disk persistence. Records the token's cleartext
 * value alongside its binding — these are bearer secrets, so the file should be
 * chmod 600 and never committed (see .gitignore).
 */
export class PersistentTokenRegistry extends TokenRegistry {
  private filePath: string;
  private recordBindings: Record<string, TokenBinding> = {};

  constructor(dir = process.cwd()) {
    super();
    this.filePath = resolve(dir, "wh-tokens.json");
    this.load();
  }

  private load(): void {
    if (!existsSync(this.filePath)) return;
    try {
      const data = JSON.parse(readFileSync(this.filePath, "utf-8")) as RegistryFile;
      this.recordBindings = data.recordBindings ?? {};
      for (const [token, binding] of Object.entries(this.recordBindings)) {
        super.register(token, binding);
      }
    } catch (err) {
      console.warn("[webhook-receiver] failed to load wh-tokens.json:", err);
    }
  }

  private flush(): void {
    try {
      writeFileSync(this.filePath, JSON.stringify({ recordBindings: this.recordBindings }, null, 2), {
        mode: 0o600,
      });
    } catch (err) {
      console.error("[webhook-receiver] failed to write wh-tokens.json:", err);
    }
  }

  override register(token: string, binding: TokenBinding): void {
    super.register(token, binding);
    this.recordBindings[token] = binding;
    this.flush();
  }

  listWithBindings(): Array<{ token: string; binding: TokenBinding }> {
    return Object.entries(this.recordBindings).map(([token, binding]) => ({ token, binding }));
  }

  /** Remove a token binding (persisted immediately). */
  revoke(token: string): boolean {
    if (!(token in this.recordBindings)) return false;
    delete this.recordBindings[token];
    this.flush();
    return true;
  }
}
