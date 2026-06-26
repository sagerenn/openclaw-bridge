/**
 * Contact store — persists known user IDs from inbound messages.
 *
 * When the bridge receives a message from a new sender, it records the
 * sender's user ID, channel, and account. On restart, the server can
 * send an "online" notification to all persisted contacts.
 *
 * Storage: contacts.json in the working directory (or config directory).
 */

import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("contacts");

// ─── Types ────────────────────────────────────────────────────────────────────

export interface ContactEntry {
  /** User ID on the backend channel */
  userId: string;
  /** Channel ID (e.g. "liangzimixin") */
  channel: string;
  /** Account ID within the channel */
  accountId: string;
  /** Display name (if available) */
  displayName?: string;
  /** First seen timestamp (ms since epoch) */
  firstSeenAt: number;
  /** Last seen timestamp (ms since epoch) */
  lastSeenAt: number;
}

export interface ContactsFile {
  /** Map of "channel:accountId:userId" -> ContactEntry */
  contacts: Record<string, ContactEntry>;
}

// ─── Contact Store ────────────────────────────────────────────────────────────

export class ContactStore {
  private filePath: string;
  private contacts: Record<string, ContactEntry> = {};
  private dirty = false;
  private flushTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(configPath?: string) {
    // Store contacts.json alongside config.json
    const dir = configPath ? dirname(configPath) : process.cwd();
    this.filePath = resolve(dir, "contacts.json");
    this.load();
  }

  /** Load contacts from disk */
  private load(): void {
    if (!existsSync(this.filePath)) {
      log.info("No contacts file found, starting fresh", { path: this.filePath });
      return;
    }

    try {
      const raw = readFileSync(this.filePath, "utf-8");
      const data: ContactsFile = JSON.parse(raw);
      this.contacts = data.contacts ?? {};
      log.info("Loaded contacts", { count: Object.keys(this.contacts).length, path: this.filePath });
    } catch (err) {
      log.warn("Failed to load contacts file, starting fresh", { path: this.filePath, error: String(err) });
      this.contacts = {};
    }
  }

  /** Flush contacts to disk (debounced) */
  private scheduleFlush(): void {
    if (this.flushTimer) clearTimeout(this.flushTimer);
    this.flushTimer = setTimeout(() => this.flush(), 1000);
  }

  /** Force-write contacts to disk */
  flush(): void {
    if (!this.dirty) return;
    try {
      const data: ContactsFile = { contacts: this.contacts };
      writeFileSync(this.filePath, JSON.stringify(data, null, 2), "utf-8");
      this.dirty = false;
      log.debug("Flushed contacts to disk", { count: Object.keys(this.contacts).length });
    } catch (err) {
      log.error("Failed to flush contacts", { error: String(err) });
    }
  }

  /** Record a contact from an inbound message */
  recordContact(params: {
    userId: string;
    channel: string;
    accountId: string;
    displayName?: string;
  }): boolean {
    const key = `${params.channel}:${params.accountId}:${params.userId}`;
    const now = Date.now();
    const existing = this.contacts[key];

    if (existing) {
      // Update last seen and display name
      existing.lastSeenAt = now;
      if (params.displayName && !existing.displayName) {
        existing.displayName = params.displayName;
        this.dirty = true;
      }
      return false; // Not new
    }

    // New contact
    this.contacts[key] = {
      userId: params.userId,
      channel: params.channel,
      accountId: params.accountId,
      displayName: params.displayName,
      firstSeenAt: now,
      lastSeenAt: now,
    };
    this.dirty = true;
    this.scheduleFlush();
    log.info("New contact recorded", { channel: params.channel, accountId: params.accountId, userId: params.userId });
    return true; // New contact
  }

  /** Get all contacts */
  getAllContacts(): ContactEntry[] {
    return Object.values(this.contacts);
  }

  /** Get contacts for a specific channel account */
  getContactsForAccount(channel: string, accountId: string): ContactEntry[] {
    return Object.values(this.contacts).filter(
      (c) => c.channel === channel && c.accountId === accountId
    );
  }

  /** Get all unique channel:account pairs that have contacts */
  getActiveAccounts(): Array<{ channel: string; accountId: string }> {
    const seen = new Set<string>();
    const result: Array<{ channel: string; accountId: string }> = [];
    for (const contact of Object.values(this.contacts)) {
      const key = `${contact.channel}:${contact.accountId}`;
      if (!seen.has(key)) {
        seen.add(key);
        result.push({ channel: contact.channel, accountId: contact.accountId });
      }
    }
    return result;
  }

  /** Get the file path */
  get path(): string {
    return this.filePath;
  }
}
