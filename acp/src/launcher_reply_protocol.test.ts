import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import {
  chmodSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
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
// --append-system-prompt. These tests run the REAL, unmodified launcher.exp
// under REAL expect against a stand-in `claude` that records the argv it
// receives, so they check what the session would actually get: Tcl quoting,
// UTF-8, both spawn branches, the binary/model resolution the launcher does
// before spawning, and the TUI dismissal loop that the longer command line
// could disturb.
//
// The stand-in is reached only through the launcher's own binary resolution
// (HERMES_CLAUDE_BIN, or $HOME/.local/bin/claude), never by editing the
// launcher. Before any launcher is run, a pre-flight evaluates the launcher's
// own `set claude_bin` statement under each scenario's env and refuses to go on
// unless it yields the stand-in, and unless nothing else in the file can change
// where a spawn lands: a launcher that stopped resolving its binary that way
// fails there, instead of starting a real model session on the account that
// runs the suite.

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

const SESSION_NAME = "claude_hermes_testfixture";
const DEFAULT_ALLOWED_TOOLS = "mcp__plugin_hermes-channel_hermes-channel__* Read";
const DEFAULT_MODEL = "haiku";
const OVERRIDDEN_MODEL = "sonnet";
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

/**
 * The pattern the launcher's TUI dismissal loop fires on, read from the
 * launcher itself so the "protocol must not trip the loop" check follows the
 * loop if its anchor ever changes.
 */
function dismissalLoopPattern(): RegExp {
  const source = readFileSync(LAUNCHER_PATH, "utf8");
  const match = source.match(/^\s*-re\s+\{([^}]+)\}/m);
  if (!match) throw new Error("could not locate the TUI dismissal loop pattern in launcher.exp");
  return new RegExp(match[1]!);
}

interface Scenario {
  label: string;
  /** How the launcher is told where claude is. */
  binaryVia: "HERMES_CLAUDE_BIN" | "HOME";
  /** HERMES_CLAUDE_MODEL, when the scenario sets it. */
  modelEnv?: string;
  expectedModel: string;
}

const SCENARIOS: Scenario[] = [
  {
    label: "binary from HERMES_CLAUDE_BIN, default model",
    binaryVia: "HERMES_CLAUDE_BIN",
    expectedModel: DEFAULT_MODEL,
  },
  {
    label: "binary from $HOME/.local/bin/claude, model from HERMES_CLAUDE_MODEL",
    binaryVia: "HOME",
    modelEnv: OVERRIDDEN_MODEL,
    expectedModel: OVERRIDDEN_MODEL,
  },
];

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

/** Where the launcher must end up spawning in a scenario: always the stand-in. */
function standInFor(scenario: Scenario): string {
  return scenario.binaryVia === "HERMES_CLAUDE_BIN"
    ? stubPath
    : join(workDir, ".local", "bin", "claude");
}

/**
 * Env a scenario runs the launcher under. Deliberately minimal, like a launchd
 * job: no LANG / LC_*. HOME is the scratch dir, so the launcher's $HOME
 * fallback can only ever find the stand-in.
 */
function scenarioEnv(scenario: Scenario): Record<string, string> {
  const env: Record<string, string> = {
    PATH: "/usr/bin:/bin",
    HOME: workDir,
    HERMES_SESSION_CWD: workDir,
    HERMES_SESSION_NAME: SESSION_NAME,
  };
  if (scenario.binaryVia === "HERMES_CLAUDE_BIN") env["HERMES_CLAUDE_BIN"] = stubPath;
  if (scenario.modelEnv !== undefined) env["HERMES_CLAUDE_MODEL"] = scenario.modelEnv;
  return env;
}

/**
 * Pre-flight, before any launcher is run: refuse unless the launcher can only
 * ever spawn the stand-in under every scenario.
 *
 * Checked against the launcher's own text, not against a copy of its logic:
 *  - the only non-comment mentions of `claude_bin` are one `set claude_bin`
 *    statement and spawns of `$claude_bin`, so nothing reassigns or bypasses it;
 *  - every `spawn` in the file is of `$claude_bin`;
 *  - that one `set` statement, evaluated by real expect under each scenario's
 *    env, yields the stand-in's path.
 */
function assertLauncherCanOnlySpawnTheStandIn(scenarios: Scenario[]): void {
  const lines = readFileSync(LAUNCHER_PATH, "utf8").split("\n");
  const code = lines.filter((line) => !line.trimStart().startsWith("#"));
  const assignments = code.filter((line) => /^\s*set\s+claude_bin\b/.test(line));
  const spawns = code.filter((line) => /^\s*spawn\b/.test(line));
  const spawnsOfResolved = spawns.filter((line) => /^\s*spawn\s+\$claude_bin(\s|$)/.test(line));
  const otherMentions = code.filter(
    (line) =>
      line.includes("claude_bin") &&
      !assignments.includes(line) &&
      !spawnsOfResolved.includes(line)
  );
  if (
    assignments.length !== 1 ||
    spawns.length === 0 ||
    spawnsOfResolved.length !== spawns.length ||
    otherMentions.length > 0
  ) {
    throw new Error(
      "harness cannot isolate launcher.exp — refusing to run (it could start a real claude): " +
        `${assignments.length} 'set claude_bin' statement(s) (need exactly 1), ` +
        `${spawns.length} spawn site(s) of which ${spawnsOfResolved.length} spawn $claude_bin, ` +
        `${otherMentions.length} other mention(s) of claude_bin: ${otherMentions.join(" | ").trim()}`
    );
  }

  const expectBin = Bun.which("expect") ?? "/usr/bin/expect";
  for (const scenario of scenarios) {
    const probe = Bun.spawnSync(
      [expectBin, "-c", `${assignments[0]}; puts $claude_bin; exit 0`],
      { env: scenarioEnv(scenario), stdout: "pipe", stderr: "pipe", stdin: "ignore" }
    );
    const resolved = probe.stdout.toString().replace(/\n$/, "");
    if (probe.exitCode !== 0 || resolved !== standInFor(scenario)) {
      throw new Error(
        "harness cannot isolate launcher.exp — refusing to run (it could start a real claude): " +
          `under "${scenario.label}" the launcher's claude_bin resolves to ` +
          `${JSON.stringify(resolved)} (expect exit ${probe.exitCode}), not the stand-in ` +
          `${JSON.stringify(standInFor(scenario))}. ${probe.stderr.toString()}`
      );
    }
  }
}

function writeStub(): string {
  const path = join(workDir, "claude-stub");
  writeFileSync(
    path,
    [
      "#!/bin/bash",
      "# Stand-in for claude: record argv NUL-delimited, then raise TUI prompts that",
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

/** The location the launcher falls back to when HERMES_CLAUDE_BIN is unset. */
function installStubUnderHome(): void {
  const binDir = join(workDir, ".local", "bin");
  mkdirSync(binDir, { recursive: true });
  symlinkSync(stubPath, join(binDir, "claude"));
}

async function runLauncher(scenario: Scenario, withSettingsFile: boolean): Promise<LauncherRun> {
  const tag = `${withSettingsFile ? "with-settings" : "no-settings"}-${scenario.binaryVia}`;
  const argvFile = join(workDir, `${tag}.argv`);
  const entersFile = join(workDir, `${tag}.enters`);
  const settingsFile = join(workDir, "claude-settings.json");

  const env: Record<string, string> = {
    ...scenarioEnv(scenario),
    STUB_ARGV_FILE: argvFile,
    STUB_ENTERS_FILE: entersFile,
  };
  if (withSettingsFile) env["HERMES_CLAUDE_SETTINGS_FILE"] = settingsFile;

  const expectBin = Bun.which("expect") ?? "/usr/bin/expect";
  const proc = Bun.spawn([expectBin, "-f", LAUNCHER_PATH], {
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

  let argvRaw: string;
  try {
    argvRaw = readFileSync(argvFile, "utf8");
  } catch {
    throw new Error(
      `the stub claude never ran (binary via ${scenario.binaryVia}, ` +
        `${withSettingsFile ? "with" : "no"} settings file); launcher exit=${exitCode}\n` +
        stdout + stderr
    );
  }
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

/**
 * argv as flag -> value, asserting every flag occurs exactly once and has a
 * value. Order-insensitive on purpose: the contract is which flags the session
 * gets, not the sequence they are written in.
 */
function flagsOf(argv: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < argv.length; i += 2) {
    const flag = argv[i]!;
    expect(flag.startsWith("--")).toBe(true);
    expect(flags).not.toHaveProperty(flag);
    expect(i + 1).toBeLessThan(argv.length);
    flags[flag] = argv[i + 1]!;
  }
  return flags;
}

const runs = new Map<string, LauncherRun>();
const runKey = (scenario: Scenario, withSettingsFile: boolean) =>
  `${scenario.label}|${withSettingsFile}`;

beforeAll(async () => {
  workDir = mkdtempSync(join(tmpdir(), "launcher-reply-protocol-"));
  writeFileSync(join(workDir, "claude-settings.json"), "{}");
  stubPath = writeStub();
  installStubUnderHome();
  assertLauncherCanOnlySpawnTheStandIn(SCENARIOS);

  const pending: Promise<void>[] = [];
  for (const scenario of SCENARIOS) {
    for (const withSettingsFile of [false, true]) {
      pending.push(
        runLauncher(scenario, withSettingsFile).then((run) => {
          runs.set(runKey(scenario, withSettingsFile), run);
        })
      );
    }
  }
  await Promise.all(pending);
}, HARNESS_TIMEOUT_MS);

afterAll(() => {
  if (workDir) rmSync(workDir, { recursive: true, force: true });
});

describe("launcher.exp hands the hermes-channel reply protocol to every session", () => {
  for (const scenario of SCENARIOS) {
    describe(scenario.label, () => {
      for (const withSettingsFile of [false, true]) {
        const branch = withSettingsFile
          ? "with HERMES_CLAUDE_SETTINGS_FILE"
          : "without HERMES_CLAUDE_SETTINGS_FILE";
        const pick = () => runs.get(runKey(scenario, withSettingsFile))!;

        describe(branch, () => {
          test("the session's --append-system-prompt is exactly the plugin's server instructions", () => {
            const { argv } = pick();
            const received = flagsOf(argv)["--append-system-prompt"];

            expect(received).toBeDefined();
            // Byte-for-byte: any drift between launcher.exp and server.ts, or
            // any mangling by Tcl quoting / the system encoding, fails here.
            expect(received).toBe(pluginInstructions());
          });

          test("the rest of the spawn contract is unchanged", () => {
            const { argv } = pick();
            const { "--append-system-prompt": _protocol, ...contract } = flagsOf(argv);

            expect(contract).toEqual({
              "--dangerously-load-development-channels":
                "plugin:hermes-channel@claude-channels-hermes",
              "--name": SESSION_NAME,
              "--permission-mode": "dontAsk",
              "--model": scenario.expectedModel,
              "--allowedTools": DEFAULT_ALLOWED_TOOLS,
              ...(withSettingsFile
                ? {
                    "--setting-sources": "project,local",
                    "--settings": join(workDir, "claude-settings.json"),
                  }
                : {}),
            });
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
    });
  }

  test("the settings branch really is the one that passes --settings (the two runs differ)", () => {
    for (const scenario of SCENARIOS) {
      expect(runs.get(runKey(scenario, true))!.argv).toContain("--settings");
      expect(runs.get(runKey(scenario, false))!.argv).not.toContain("--settings");
    }
  });
});

describe("the protocol text and the launcher's TUI dismissal loop", () => {
  // The loop fires on a bare pattern anywhere in the stream, and expect echoes
  // the whole spawn command line (including the protocol) into that stream.
  // A match inside the protocol would send Enters before the TUI exists — the
  // early-fire bug launcher.exp documents. Not detectable behaviourally (a
  // cooked pty buffers the early Enter), so pin the cause: check the text the
  // launcher actually passed against the pattern the launcher actually uses.
  test("does not match the pattern the dismissal loop fires on", () => {
    const received = flagsOf(runs.get(runKey(SCENARIOS[0]!, false))!.argv)["--append-system-prompt"];

    expect(received).toBeDefined();
    expect(received).not.toMatch(dismissalLoopPattern());
  });
});
