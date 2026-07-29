/**
 * plugin_orphan_sweep.test.ts — JA-41.
 *
 * Every test mocks ps/lsof/kill — NO real process is ever inspected or
 * signalled. The fixtures are transcriptions of a real census taken on tanuki
 * (2026-07-29): the argv shapes, the path-free `bun server.ts` leaf, the
 * `bun run --cwd ... --shell=bun --silent start` wrapper, and the unrelated
 * `claude-channel-slack` project running under a DIFFERENT user account are
 * all copied from that snapshot rather than invented.
 *
 * Mirrors the discover/verify/kill shape asserted by
 * session_pool.test.ts's `reconcileOrphans (gap b)` block.
 */

import { describe, it, expect } from "bun:test";

import {
  PluginOrphanSweeper,
  parseEtimeMs,
  parsePsLine,
  isSweepEnabled,
  ORPHAN_GRACE_MS,
} from "./plugin_orphan_sweep.ts";
import type { ProcessSnapshot, SweepDeps } from "./plugin_orphan_sweep.ts";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const OUR_UID = 502;
const OTHER_UID = 503; // Bad Janet (gduquesnay) — same box, different account.
const LAUNCHER = "/Users/guillaume/.janet-test/acp/launcher.exp";

const PLUGIN_DIR =
  "/Users/guillaume/dev/nestor/janet-test/plugins/claude-channels-hermes/marketplace/external_plugins/hermes-channel";

/** Comfortably past the grace window. */
const OLD_MS = ORPHAN_GRACE_MS * 10;

function proc(over: Partial<ProcessSnapshot> & { pid: number }): ProcessSnapshot {
  return {
    ppid: 1,
    uid: OUR_UID,
    ageMs: OLD_MS,
    command: "/opt/homebrew/bin/bun server.ts",
    ...over,
  };
}

interface Recorder {
  kills: Array<{ pid: number; signal: string }>;
  deps: Partial<SweepDeps>;
}

/**
 * Build deps from a process table. `cwds` supplies lsof answers; anything
 * absent is reported as unavailable (the fail-closed case). By default every
 * signalled process dies on SIGTERM, so escalation noise never pollutes the
 * assertions of the tests that are not about escalation.
 */
function makeDeps(
  processes: ProcessSnapshot[],
  cwds: Record<number, string> = {},
  over: Partial<SweepDeps> = {}
): Recorder {
  const kills: Array<{ pid: number; signal: string }> = [];
  const byPid = new Map(processes.map((p) => [p.pid, p]));
  return {
    kills,
    deps: {
      listProcesses: () => processes,
      getPpid: (pid) => byPid.get(pid)?.ppid,
      getCommandLine: (pid) => byPid.get(pid)?.command,
      getCwd: (pid) => cwds[pid],
      killPid: (pid, signal) => { kills.push({ pid, signal }); },
      isPidAlive: () => false,
      ownUid: () => OUR_UID,
      ...over,
    },
  };
}

// ---------------------------------------------------------------------------
// Discovery + sweep of genuine plugin orphans
// ---------------------------------------------------------------------------

describe("PluginOrphanSweeper — genuine plugin orphans", () => {
  it("sweeps a ppid=1 plugin-bun leaf, identified by its cwd (its argv carries no path)", async () => {
    const ORPHAN = 13019;
    const { kills, deps } = makeDeps(
      [proc({ pid: ORPHAN })],
      { [ORPHAN]: PLUGIN_DIR }
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    const swept = await sweeper.sweep();

    expect(swept.map((c) => c.pid)).toEqual([ORPHAN]);
    expect(swept[0]!.klass).toBe("plugin-leaf");
    expect(kills).toEqual([{ pid: ORPHAN, signal: "SIGTERM" }]);
  });

  it("sweeps the plugin's `bun run --cwd <plugin dir> ... start` wrapper straight from its argv", async () => {
    const WRAPPER = 34483;
    const { kills, deps } = makeDeps([
      proc({
        pid: WRAPPER,
        command: `bun run --cwd ${PLUGIN_DIR} --shell=bun --silent start`,
      }),
    ]);
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    const swept = await sweeper.sweep();

    expect(swept.map((c) => c.klass)).toEqual(["plugin-wrapper"]);
    expect(kills).toEqual([{ pid: WRAPPER, signal: "SIGTERM" }]);
  });

  it("recognises the server-v3.ts variant of the leaf", async () => {
    const ORPHAN = 38659;
    const { kills, deps } = makeDeps(
      [proc({ pid: ORPHAN, command: "/opt/homebrew/bin/bun server-v3.ts" })],
      { [ORPHAN]: `${PLUGIN_DIR}-spike` }
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect((await sweeper.sweep()).map((c) => c.pid)).toEqual([ORPHAN]);
    expect(kills).toEqual([{ pid: ORPHAN, signal: "SIGTERM" }]);
  });

  it("kills only by exact PID — every signal targets a discovered candidate", async () => {
    const ORPHAN = 26369;
    const BYSTANDER = 26370;
    const { kills, deps } = makeDeps(
      [
        proc({ pid: ORPHAN }),
        proc({ pid: BYSTANDER, ppid: 4242, command: "/opt/homebrew/bin/bun server.ts" }),
      ],
      { [ORPHAN]: PLUGIN_DIR, [BYSTANDER]: PLUGIN_DIR }
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    await sweeper.sweep();

    expect(kills.every((k) => k.pid === ORPHAN)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// What must NEVER be swept
// ---------------------------------------------------------------------------

describe("PluginOrphanSweeper — refusals", () => {
  it("spares a LIVE plugin process (ppid != 1) even though its argv and cwd match exactly", async () => {
    const LIVE_LEAF = 34492;
    const { kills, deps } = makeDeps(
      [proc({ pid: LIVE_LEAF, ppid: 34483 })],
      { [LIVE_LEAF]: PLUGIN_DIR }
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect(await sweeper.sweep()).toEqual([]);
    expect(kills).toEqual([]);
  });

  it("spares an unrelated ppid=1 bun process whose cwd is not a hermes-channel plugin dir", async () => {
    // Transcribed from the live census: a different project's server.ts,
    // same argv shape, orphaned all the same. The cwd is the only thing that
    // tells the two apart.
    const STRANGER = 392;
    const { kills, deps } = makeDeps(
      [proc({ pid: STRANGER, command: "bun server.ts" })],
      { [STRANGER]: "/Users/jasquier/dev/nestor/claude-channel-slack" }
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect(await sweeper.sweep()).toEqual([]);
    expect(kills).toEqual([]);
  });

  it("spares a plugin orphan owned by ANOTHER account (Bad Janet's — his instance sweeps his own)", async () => {
    const THEIRS = 32057;
    const { kills, deps } = makeDeps(
      [proc({ pid: THEIRS, uid: OTHER_UID })],
      { [THEIRS]: "/Users/gduquesnay/.hermes/plugins/marketplace/external_plugins/hermes-channel" }
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect(await sweeper.sweep()).toEqual([]);
    expect(kills).toEqual([]);
  });

  it("spares a plugin orphan younger than the grace window (early boot, not yet settled)", async () => {
    const YOUNG = 99001;
    const { kills, deps } = makeDeps(
      [proc({ pid: YOUNG, ageMs: ORPHAN_GRACE_MS - 1000 })],
      { [YOUNG]: PLUGIN_DIR }
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect(await sweeper.sweep()).toEqual([]);
    expect(kills).toEqual([]);
  });

  it("spares a leaf whose cwd cannot be read at all (fail closed, never fail open)", async () => {
    const UNKNOWN = 47356;
    const { kills, deps } = makeDeps([proc({ pid: UNKNOWN })], {}); // no cwd answer
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect(await sweeper.sweep()).toEqual([]);
    expect(kills).toEqual([]);
  });

  it("spares a candidate that got re-parented between discovery and the kill (live ppid re-check)", async () => {
    const RACY = 65514;
    const { kills, deps } = makeDeps(
      [proc({ pid: RACY })],
      { [RACY]: PLUGIN_DIR },
      { getPpid: () => 4242 } // adopted by a live process after discovery
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect(sweeper.discover().map((c) => c.pid)).toEqual([RACY]); // it WAS a candidate
    expect(await sweeper.sweep()).toEqual([]); // ...and is refused at kill time
    expect(kills).toEqual([]);
  });

  it("spares a recycled PID whose command line changed between discovery and the kill", async () => {
    const RECYCLED = 74525;
    const { kills, deps } = makeDeps(
      [proc({ pid: RECYCLED })],
      { [RECYCLED]: PLUGIN_DIR },
      { getCommandLine: () => "/usr/bin/vim notes.txt" }
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect(await sweeper.sweep()).toEqual([]);
    expect(kills).toEqual([]);
  });

  it("spares a process that vanished between discovery and the kill", async () => {
    const GONE = 83647;
    const { kills, deps } = makeDeps(
      [proc({ pid: GONE })],
      { [GONE]: PLUGIN_DIR },
      { getPpid: () => undefined, getCommandLine: () => undefined }
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect(await sweeper.sweep()).toEqual([]);
    expect(kills).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Stuck mid-spawn (the class reconcileOrphans cannot see: 0 sockets, 0 children)
// ---------------------------------------------------------------------------

describe("PluginOrphanSweeper — stuck mid-spawn", () => {
  it("sweeps an orphaned launcher with zero children past the grace window", async () => {
    const STUCK = 55001;
    const { kills, deps } = makeDeps([
      proc({ pid: STUCK, command: `/usr/bin/expect -f ${LAUNCHER}` }),
    ]);
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    const swept = await sweeper.sweep();

    expect(swept.map((c) => c.klass)).toEqual(["stuck-spawn"]);
    expect(kills).toEqual([{ pid: STUCK, signal: "SIGTERM" }]);
  });

  it("sweeps an orphaned pool-named claude with zero children", async () => {
    const STUCK = 55002;
    const { kills, deps } = makeDeps([
      proc({
        pid: STUCK,
        command:
          "/Users/guillaume/.local/bin/claude --name claude_hermes_deadbeef --permission-mode dontAsk",
      }),
    ]);
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect((await sweeper.sweep()).map((c) => c.klass)).toEqual(["stuck-spawn"]);
    expect(kills).toEqual([{ pid: STUCK, signal: "SIGTERM" }]);
  });

  it("spares an orphaned launcher that DOES have a child — that subtree is reconcileOrphans' business", async () => {
    const EXPECT_PID = 55003;
    const { kills, deps } = makeDeps([
      proc({ pid: EXPECT_PID, command: `/usr/bin/expect -f ${LAUNCHER}` }),
      proc({
        pid: 55004,
        ppid: EXPECT_PID,
        command: "/Users/guillaume/.local/bin/claude --name claude_hermes_deadbeef",
      }),
    ]);
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect(await sweeper.sweep()).toEqual([]);
    expect(kills).toEqual([]);
  });

  it("spares a launcher belonging to a DIFFERENT instance's deployed copy", async () => {
    // The launcher path is the one genuinely per-instance signature there is:
    // prod's expect must never be selected by janet-test's sweep.
    const PROD_EXPECT = 55005;
    const { kills, deps } = makeDeps([
      proc({
        pid: PROD_EXPECT,
        command: "/usr/bin/expect -f /Users/guillaume/.hermes/acp/launcher.exp",
      }),
    ]);
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect(await sweeper.sweep()).toEqual([]);
    expect(kills).toEqual([]);
  });

  it("spares an unrelated orphaned expect script from another repo", async () => {
    const ZETA = 92486;
    const { kills, deps } = makeDeps([
      proc({
        pid: ZETA,
        command:
          "/usr/bin/expect -f /Users/guillaume/dev/nestor/claude-zeta-agent/scripts/zeta-launcher.exp",
      }),
    ]);
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    expect(await sweeper.sweep()).toEqual([]);
    expect(kills).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Kill escalation
// ---------------------------------------------------------------------------

describe("PluginOrphanSweeper — signal escalation", () => {
  it("escalates SIGTERM -> SIGKILL, by exact PID, when the orphan survives the grace period", async () => {
    const STUBBORN = 95219;
    const { kills, deps } = makeDeps(
      [proc({ pid: STUBBORN })],
      { [STUBBORN]: PLUGIN_DIR },
      { isPidAlive: () => true }
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    await sweeper.sweep();

    expect(kills).toEqual([
      { pid: STUBBORN, signal: "SIGTERM" },
      { pid: STUBBORN, signal: "SIGKILL" },
    ]);
  });

  it("drops a tick that lands while the previous sweep is still running", async () => {
    const ORPHAN = 42380;
    const { kills, deps } = makeDeps(
      [proc({ pid: ORPHAN })],
      { [ORPHAN]: PLUGIN_DIR }
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    const [first, second] = await Promise.all([sweeper.sweep(), sweeper.sweep()]);

    expect(first.map((c) => c.pid)).toEqual([ORPHAN]);
    expect(second).toEqual([]); // dropped, not a second SIGTERM to the same PID
    expect(kills).toEqual([{ pid: ORPHAN, signal: "SIGTERM" }]);
  });

  it("does not escalate when the orphan dies on SIGTERM", async () => {
    const POLITE = 15246;
    const { kills, deps } = makeDeps(
      [proc({ pid: POLITE })],
      { [POLITE]: PLUGIN_DIR }
    );
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    await sweeper.sweep();

    expect(kills.map((k) => k.signal)).toEqual(["SIGTERM"]);
  });
});

// ---------------------------------------------------------------------------
// discover() is pure
// ---------------------------------------------------------------------------

describe("PluginOrphanSweeper — census", () => {
  it("discover() classifies a whole mixed table without signalling anything", async () => {
    const table = [
      proc({ pid: 13019 }), // plugin leaf
      proc({ pid: 34483, command: `bun run --cwd ${PLUGIN_DIR} --shell=bun --silent start` }),
      proc({ pid: 55001, command: `/usr/bin/expect -f ${LAUNCHER}` }),
      proc({ pid: 392, command: "bun server.ts" }), // other project
      proc({ pid: 32057, uid: OTHER_UID }), // other account
      proc({ pid: 34492, ppid: 34483 }), // live child
      proc({ pid: 1, ppid: 0, command: "/sbin/launchd" }),
    ];
    const { kills, deps } = makeDeps(table, {
      13019: PLUGIN_DIR,
      392: "/Users/jasquier/dev/nestor/claude-channel-slack",
      32057: "/Users/gduquesnay/.hermes/marketplace/external_plugins/hermes-channel",
      34492: PLUGIN_DIR,
    });
    const sweeper = new PluginOrphanSweeper(LAUNCHER, deps);

    const found = sweeper.discover();

    expect(
      found.map((c) => `${c.pid}:${c.klass}`).sort()
    ).toEqual(["13019:plugin-leaf", "34483:plugin-wrapper", "55001:stuck-spawn"]);
    expect(kills).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// ps parsing — silent mis-parsing would disable the grace window invisibly
// ---------------------------------------------------------------------------

describe("ps output parsing", () => {
  it("parses every etime shape ps emits", () => {
    expect(parseEtimeMs("12:35")).toBe(12 * 60_000 + 35_000);
    expect(parseEtimeMs("01:30:05")).toBe((3600 + 30 * 60 + 5) * 1000);
    expect(parseEtimeMs("25-02:14:12")).toBe(
      ((25 * 24 + 2) * 3600 + 14 * 60 + 12) * 1000
    );
    expect(parseEtimeMs("  02-13:13:52 ")).toBe(
      ((2 * 24 + 13) * 3600 + 13 * 60 + 52) * 1000
    );
  });

  it("treats an unparsable etime as age 0, so the grace window spares it", () => {
    expect(parseEtimeMs("???")).toBe(0);
    expect(parseEtimeMs("")).toBe(0);
  });

  it("parses a real ps line, keeping the full command with its spaces", () => {
    const line =
      "  34483     1   502 02-20:53:05 bun run --cwd /some/dir --shell=bun --silent start";
    expect(parsePsLine(line)).toEqual({
      pid: 34483,
      ppid: 1,
      uid: 502,
      ageMs: ((2 * 24 + 20) * 3600 + 53 * 60 + 5) * 1000,
      command: "bun run --cwd /some/dir --shell=bun --silent start",
    });
  });

  it("ignores a header or malformed line rather than inventing a process", () => {
    expect(parsePsLine("PID PPID UID ELAPSED COMMAND")).toBeUndefined();
    expect(parsePsLine("")).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Kill switch
// ---------------------------------------------------------------------------

describe("isSweepEnabled", () => {
  it("is on by default — a sweep that must be switched on per instance is never on where it matters", () => {
    expect(isSweepEnabled({})).toBe(true);
  });

  it("can be switched off for one instance without a redeploy", () => {
    expect(isSweepEnabled({ HERMES_PLUGIN_ORPHAN_SWEEP: "0" })).toBe(false);
    expect(isSweepEnabled({ HERMES_PLUGIN_ORPHAN_SWEEP: "false" })).toBe(false);
  });
});
