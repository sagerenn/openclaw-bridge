/**
 * Idempotent patch for the published `@larksuite/openclaw-lark` channel plugin.
 *
 * The shipped plugin (v2026.6.10) is CommonJS output that nonetheless uses
 * `import.meta.url` — an ESM-only construct — inside two CJS files:
 *
 *   - src/core/version.js   (unconditional import.meta.url)
 *   - src/core/token-store.js (import.meta.url as a fallback in a ternary)
 *
 * Under Node >= 22's syntax-based module detection, any `.js` file containing
 * an `import.meta` member expression is classified as an ES module. Once
 * treated as ESM, the `exports.x = ...` assignments in those files become
 * no-ops and the `require()` calls are illegal, so the plugin fails to load
 * with `exports is not defined in ES module scope` / `getUserAgent is not a
 * function`.
 *
 * This script removes the `import.meta` usages (CJS provides `__filename` /
 * `__dirname` natively), restoring correct CJS classification. It is safe to
 * run repeatedly and is a no-op if the plugin is absent or already patched.
 *
 * Invoked from `npm run patch:lark` and from the `postinstall` hook so the fix
 * survives `npm install` / reinstalls.
 */

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { rootLogger } from "./logger.js";

const log = rootLogger.child("patch-lark");

/** Patches applied verbatim (old -> new). Order matters: longest first. */
const PATCHES: Array<{ file: string; find: string; replace: string; label: string }> = [
  {
    file: "src/core/version.js",
    label: "version.js: drop import.meta.url, use native CJS __filename/__dirname",
    find:
      "const __filename = (0, node_url_1.fileURLToPath)(import.meta.url);\n        const __dirname = (0, node_path_1.dirname)(__filename);\n",
    replace:
      "// __filename / __dirname are provided natively by the CJS loader\n",
  },
  {
    file: "src/core/token-store.js",
    label: "token-store.js: drop import.meta.url fallback, use native CJS __filename",
    find:
      "const _require = (0, node_module_1.createRequire)(typeof __filename !== 'undefined' ? __filename : import.meta.url);",
    replace: "const _require = (0, node_module_1.createRequire)(__filename);",
  },
];

/**
 * Resolve the installed lark plugin root. Works whether invoked via
 * `node dist/...` (cwd = project root) or via the bin from anywhere.
 */
function resolvePluginRoot(): string | null {
  const here = dirname(fileURLToPath(import.meta.url));
  // dist/util/patch-lark-plugin.js -> project root is two levels up
  const projectRoot = dirname(dirname(here));
  const candidates = [
    join(projectRoot, "node_modules", "@larksuite", "openclaw-lark"),
    join(process.cwd(), "node_modules", "@larksuite", "openclaw-lark"),
  ];
  for (const c of candidates) {
    if (existsSync(join(c, "package.json"))) return c;
  }
  return null;
}

/** Apply one patch to one file. Returns true if the file was modified. */
function applyPatch(root: string, patch: (typeof PATCHES)[number]): boolean {
  const filePath = join(root, patch.file);
  if (!existsSync(filePath)) {
    log.debug("Patch target missing, skipping", { file: patch.file });
    return false;
  }
  const src = readFileSync(filePath, "utf-8");

  // Already patched (find gone, replace present) — idempotent no-op.
  if (!src.includes(patch.find)) {
    if (src.includes(patch.replace)) {
      log.debug("Already patched", { file: patch.file });
    } else {
      log.warn("Patch pattern not found and not already applied — plugin may have changed", {
        file: patch.file,
        label: patch.label,
      });
    }
    return false;
  }

  const updated = src.replace(patch.find, patch.replace);
  writeFileSync(filePath, updated, "utf-8");
  log.info("Patched lark plugin file", { file: patch.file, label: patch.label });
  return true;
}

/** Apply all patches. Returns the number of files modified. */
export function patchLarkPlugin(): number {
  const root = resolvePluginRoot();
  if (!root) {
    log.info("@larksuite/openclaw-lark not installed — nothing to patch");
    return 0;
  }

  let modified = 0;
  for (const patch of PATCHES) {
    if (applyPatch(root, patch)) modified++;
  }
  if (modified > 0) {
    log.info("Lark plugin patched successfully", { root, filesModified: modified });
  }
  return modified;
}

// CLI entry: run when invoked directly (`node dist/util/patch-lark-plugin.js`).
if (import.meta.url === `file://${process.argv[1]}`) {
  patchLarkPlugin();
}
