import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Incident 2026-10-14: Claude Code 2.1.299 started a Janet session, delivered
// the first <channel> message, and never gave the model the hermes-channel
// plugin's MCP server instructions ("your text output is NEVER delivered, only
// reply_close delivers"). The model answered in plain text, nothing reached the
// user, and the supervisor retired the session. The zeta daemon is immune
// because its launcher carries its reply rule in its own command line.
//
// So launcher.exp hands the same protocol to every session via
// --append-system-prompt. These tests run the REAL launcher.exp under REAL
// expect against a stand-in `claude` that records the argv it receives, so they
// check what the session would actually get: Tcl quoting, UTF-8, both spawn
// branches, and the TUI "confirm" dismissal loop that the longer command line
// could disturb.

const ACP_DIR = join(import.meta.dir, "..");
const LAUNCHER_PATH = join(ACP_DIR, "launcher.exp");
const PLUGIN_SERVER_PATH = join(
  ACP_DIR,
  "..",
  "marketplace",
  "external_plugins",
  "hermes-channel",
  "server.ts"
);

/** The binary the launcher hardcodes; the harness swaps it for a stub. */
const REAL_CLAUDE_PATH = "/Users/guillaume/.local/bin/claude";

const SESSION_NAME = "claude_hermes_testfixture";
const DEFAULT_ALLOWED_TOOLS = "mcp__plugin_hermes-channel_hermes-channel__* Read";
/** Number of TUI prompts the stub raises; the launcher dismisses up to three. */
const STUB_CONFIRM_BANNERS = 3;
/** Per-banner patience of the stub; keeps a broken run self-terminating. */
const STUB_READ_TIMEOUT_SECONDS = 8;
const HARNESS_TIMEOUT_MS = 60_000;

/**
 * The instructions string exactly as the hermes-channel MCP server hands it to
 * Claude Code, read from the plugin source (same technique as the plugin's own
 * server-instructions.test.ts: the array is not exported).
 */
function pluginInstructions(): string {
  const source = readFileSync(PLUGIN_SERVER_PATH, "utf8");
  const match = source.match(/instructions:\s*\[([\s\S]*?)\]\.join\('\\n'\)/);
  if (!match) throw new Error("could not locate instructions array in plugin server.ts");
  const lines = new Function(`return [${match[1]}]`)() as string[];
  return lines.join("\n");
}

interface LauncherRun {
  exitCode: number;
  output: string;
  /** argv the stand-in claude received, in order. */
  argv: string[];
  /** How many TUI banners were answered with an Enter before the stub exited. */
  entersReceived: number;
}

let workDir: string;
let stubPath: string;
let patchedLauncherPath: string;

/**
 * Copy of launcher.exp whose claude path points at the stub. Fails closed: if
 * the launcher stops hardcoding exactly the path we swap, nothing is spawned,
 * because running the unmodified launcher would start a real model session on
 * the account that runs this test.
 */
function buildPatchedLauncher(): string {
  const original = readFileSync(LAUNCHER_PATH, "utf8");
  const spawnSites = original.match(/^\s*spawn\s/gm)?.length ?? 0;
  const swaps = original.split(REAL_CLAUDE_PATH).length - 1;
  if (spawnSites === 0 || swaps !== spawnSites) {
    throw new Error(
      `harness cannot isolate launcher.exp: ${spawnSites} spawn site(s) but ${swaps} ` +
        `occurrence(s) of ${REAL_CLAUDE_PATH} — refusing to run (it could start a real claude)`
    );
  }
  const patched = original.split(REAL_CLAUDE_PATH).join(stubPath);
  if (patched.includes("/.local/bin/claude")) {
    throw new Error("patched launcher still references a real claude binary");
  }
  const path = join(workDir, "launcher.patched.exp");
  writeFileSync(path, patched);
  return path;
}

function writeStub(): string {
  const path = join(workDir, "claude-stub");
  writeFileSync(
    path,
    [
      "#!/bin/bash",
      '# Stand-in for claude: record argv NUL-delimited, then raise TUI prompts that',
      "# need one Enter each. Self-terminating: a missing Enter ends it, no kill needed.",
      ': > "$STUB_ARGV_FILE"',
      'for a in "$@"; do printf \'%s\\0\' "$a" >> "$STUB_ARGV_FILE"; done',
      "entered=0",
      `while [ $entered -lt ${STUB_CONFIRM_BANNERS} ]; do`,
      '  echo "Press Enter to confirm"',
      `  IFS= read -r -t ${STUB_READ_TIMEOUT_SECONDS} line || break`,
      "  entered=$((entered + 1))",
      "done",
      'echo "$entered" > "$STUB_ENTERS_FILE"',
      "exit 0",
      "",
    ].join("\n")
  );
  chmodSync(path, 0o755);
  return path;
}

async function runLauncher(withSettingsFile: boolean): Promise<LauncherRun> {
  const tag = withSettingsFile ? "with-settings" : "no-settings";
  const argvFile = join(workDir, `${tag}.argv`);
  const entersFile = join(workDir, `${tag}.enters`);
  const settingsFile = join(workDir, "claude-settings.json");

  // Deliberately minimal env, like a launchd job: no LANG / LC_*.
  const env: Record<string, string> = {
    PATH: "/usr/bin:/bin",
    HOME: workDir,
    HERMES_SESSION_CWD: workDir,
    HERMES_SESSION_NAME: SESSION_NAME,
    STUB_ARGV_FILE: argvFile,
    STUB_ENTERS_FILE: entersFile,
  };
  if (withSettingsFile) env["HERMES_CLAUDE_SETTINGS_FILE"] = settingsFile;

  const expectBin = Bun.which("expect") ?? "/usr/bin/expect";
  const proc = Bun.spawn([expectBin, "-f", patchedLauncherPath], {
    env,
    stdout: "pipe",
    stderr: "pipe",
    stdin: "ignore",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);

  const argvRaw = readFileSync(argvFile, "utf8");
  const argv = argvRaw.split("\0");
  argv.pop(); // trailing empty element after the last NUL
  let entersReceived = -1;
  try {
    entersReceived = Number(readFileSync(entersFile, "utf8").trim());
  } catch {
    // stays -1: the stub never reached its end
  }
  return { exitCode, output: stdout + stderr, argv, entersReceived };
}

/** Value following `flag` in argv, asserting the flag occurs exactly once. */
function valueOf(argv: string[], flag: string): string | undefined {
  const at = argv.indexOf(flag);
  if (at === -1) return undefined;
  expect(argv.indexOf(flag, at + 1)).toBe(-1);
  return argv[at + 1];
}

let runs: { withSettings: LauncherRun; withoutSettings: LauncherRun };

beforeAll(async () => {
  workDir = mkdtempSync(join(tmpdir(), "launcher-reply-protocol-"));
  writeFileSync(join(workDir, "claude-settings.json"), "{}");
  stubPath = writeStub();
  patchedLauncherPath = buildPatchedLauncher();
  const [withSettings, withoutSettings] = await Promise.all([
    runLauncher(true),
    runLauncher(false),
  ]);
  runs = { withSettings, withoutSettings };
}, HARNESS_TIMEOUT_MS);

afterAll(() => {
  if (workDir) rmSync(workDir, { recursive: true, force: true });
});

describe("launcher.exp hands the hermes-channel reply protocol to every session", () => {
  const branches = [
    { label: "without HERMES_CLAUDE_SETTINGS_FILE", pick: () => runs.withoutSettings },
    { label: "with HERMES_CLAUDE_SETTINGS_FILE", pick: () => runs.withSettings },
  ];

  for (const { label, pick } of branches) {
    describe(label, () => {
      test("the session's --append-system-prompt is exactly the plugin's server instructions", () => {
        const { argv } = pick();
        const received = valueOf(argv, "--append-system-prompt");

        expect(received).toBeDefined();
        // Byte-for-byte: any drift between launcher.exp and server.ts, or any
        // mangling by Tcl quoting / the system encoding, fails here.
        expect(received).toBe(pluginInstructions());
      });

      test("the rest of the spawn contract is unchanged", () => {
        const { argv } = pick();
        const withSettings = argv.includes("--settings");

        expect(valueOf(argv, "--dangerously-load-development-channels")).toBe(
          "plugin:hermes-channel@claude-channels-hermes"
        );
        expect(valueOf(argv, "--name")).toBe(SESSION_NAME);
        expect(valueOf(argv, "--allowedTools")).toBe(DEFAULT_ALLOWED_TOOLS);
        expect(valueOf(argv, "--permission-mode")).toBe("dontAsk");
        if (withSettings) {
          expect(valueOf(argv, "--settings")).toBe(join(workDir, "claude-settings.json"));
          expect(valueOf(argv, "--setting-sources")).toBe("project,local");
        } else {
          expect(argv).not.toContain("--setting-sources");
        }
      });

      test("every TUI confirmation is still dismissed and the launcher exits cleanly", () => {
        const { exitCode, entersReceived, output } = pick();

        if (exitCode !== 0 || entersReceived !== STUB_CONFIRM_BANNERS) {
          throw new Error(
            `launcher exit=${exitCode} enters=${entersReceived}/${STUB_CONFIRM_BANNERS}\n${output}`
          );
        }
      });
    });
  }

  test("the settings branch really is the one that passes --settings (the two runs differ)", () => {
    expect(runs.withSettings.argv).toContain("--settings");
    expect(runs.withoutSettings.argv).not.toContain("--settings");
  });
});

describe("the protocol text and the launcher's TUI dismissal loop", () => {
  // The loop fires on a bare /confirm/ anywhere in the stream, and expect echoes
  // the whole spawn command line (now including this text) into that stream.
  // A "confirm" inside the protocol would send Enters before the TUI exists —
  // the early-fire bug launcher.exp documents. Not detectable behaviourally
  // (a cooked pty buffers the early Enter), so pin the cause.
  test("contains no 'confirm' — it would trigger the loop from the spawn echo", () => {
    expect(pluginInstructions()).not.toMatch(/confirm/);
  });
});
