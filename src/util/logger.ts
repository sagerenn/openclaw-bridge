/**
 * Structured logger for the bridge server.
 */

export type LogLevel = "debug" | "info" | "warn" | "error";

const LEVEL_PRIORITY: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

export class Logger {
  private minLevel: number;
  private prefix: string;

  constructor(prefix: string, level: LogLevel = "info") {
    this.prefix = prefix;
    this.minLevel = LEVEL_PRIORITY[level];
  }

  private log(level: LogLevel, msg: string, data?: Record<string, unknown>): void {
    if (LEVEL_PRIORITY[level] < this.minLevel) return;
    const ts = new Date().toISOString();
    const dataStr = data ? " " + JSON.stringify(data) : "";
    const line = `[${ts}] [${level.toUpperCase()}] [${this.prefix}] ${msg}${dataStr}`;
    if (level === "error") {
      process.stderr.write(line + "\n");
    } else {
      process.stdout.write(line + "\n");
    }
  }

  debug(msg: string, data?: Record<string, unknown>): void { this.log("debug", msg, data); }
  info(msg: string, data?: Record<string, unknown>): void { this.log("info", msg, data); }
  warn(msg: string, data?: Record<string, unknown>): void { this.log("warn", msg, data); }
  error(msg: string, data?: Record<string, unknown>): void { this.log("error", msg, data); }

  child(suffix: string): Logger {
    const child = new Logger(`${this.prefix}/${suffix}`, "debug");
    child.minLevel = this.minLevel;
    return child;
  }
}

export const rootLogger = new Logger("bridge", "info");
