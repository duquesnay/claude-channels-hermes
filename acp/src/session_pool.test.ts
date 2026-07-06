/**
 * session_pool.test.ts — Unit tests for ChannelsSessionPool.
 *
 * All tests mock spawn/kill/createClient — NO real claude sessions launched.
 * Verifies:
 *   - socketPathForKey / sessionNameForKey determinism and length constraint
 *   - getOrCreate: create, reuse, stale respawn
 *   - ACP to session_key mapping survives eviction
 *   - Capacity cap: evicts LRU idle → only refusal when ALL busy
 *   - Graceful drain: waits pendingCount===0 before kill
 *   - Kill by exact PID, NEVER generic glob
 *   - Generation token: present on creation, changes on respawn
 *   - Idle eviction: scanAndEvictIdle respects TTL
 */

import {
  describe,
  it,
  expect,
  beforeEach,
  afterEach,
  mock,
} from "bun:test";
import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:net";
import { unlinkSync, existsSync, rmSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";

import {
  ChannelsSessionPool,
  PoolFullError,
  socketPathForKey,
  sessionNameForKey,
} from "./session_pool.ts";
import type { PoolDeps, SessionState } from "./session_pool.ts";
import type { HermesChannelClientInterface } from "./hermes_channel_client.ts";

// ---------------------------------------------------------------------------
// Fake socket helpers
// ---------------------------------------------------------------------------

function tempSocketPath(): string {
  return join(tmpdir(), `pool-test-${randomUUID().slice(0, 8)}.sock`);
}

function startFakeSocket(socketPath: string): Promise<Server> {
  return new Promise((resolve, reject) => {
    const server = createServer(() => {});
    server.on("error", reject);
    server.listen(socketPath, () => resolve(server));
  });
}

function stopFakeSocket(server: Server, socketPath: string): Promise<void> {
  return new Promise((resolve) => {
    server.close(() => {
      if (existsSync(socketPath)) {
        try { unlinkSync(socketPath); } catch { /* ignore */ }
      }
      resolve();
    });
  });
}

// ---------------------------------------------------------------------------
// Mock client factory
// ---------------------------------------------------------------------------

function makeMockClient(pendingCount = 0): HermesChannelClientInterface {
  let _pendingCount = pendingCount;
  return {
    sendPrompt: async () => "mock",
    close: () => {},
    get pendingCount() { return _pendingCount; },
  };
}

/** Client whose pendingCount decrements to 0 after a delay. */
function makeDrainingClient(initialPending: number, drainAfterMs: number): HermesChannelClientInterface {
  let count = initialPending;
  const timer = setTimeout(() => { count = 0; }, drainAfterMs);
  return {
    sendPrompt: async () => "mock",
    close: () => { clearTimeout(timer); },
    get pendingCount() { return count; },
  };
}

// ---------------------------------------------------------------------------
// Pool factory with injected deps
// ---------------------------------------------------------------------------

interface FakeDeps {
  pool: ChannelsSessionPool;
  killPidCalls: Array<{ pid: number; signal: string }>;
  killSocketHoldersCalls: string[];
  spawnedNames: string[];
  fakeServers: Map<string, Server>;
  now: { value: number };
  cleanup: () => Promise<void>;
}

async function makePool(opts: {
  maxSessions?: number;
  pendingCount?: number;
  socketExists?: boolean;
}): Promise<FakeDeps> {
  const { pendingCount = 0 } = opts;
  const killPidCalls: Array<{ pid: number; signal: string }> = [];
  const killSocketHoldersCalls: string[] = [];
  const spawnedNames: string[] = [];
  const fakeServers = new Map<string, Server>();
  const nowValue = { value: Date.now() };

  const deps: Partial<PoolDeps> = {
    spawnLauncher(_launcherExpPath, env) {
      const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
      const name = env["HERMES_SESSION_NAME"]!;
      spawnedNames.push(name);
      const pid = 99000 + spawnedNames.length; // synthetic PID (increments per spawn)
      // Start a real fake socket so waitForSocket can resolve
      void startFakeSocket(socketPath).then((server) => {
        fakeServers.set(socketPath, server);
      });
      return pid;
    },
    createClient(_socketPath) {
      return makeMockClient(pendingCount);
    },
    killPid(pid, signal) {
      killPidCalls.push({ pid, signal });
    },
    killSocketHolders(socketPath) {
      killSocketHoldersCalls.push(socketPath);
    },
    now() { return nowValue.value; },
    isPidAlive(_pid) { return true; }, // synthetic PIDs are always "alive" in tests
    killByName(_name) {}, // no-op in tests; pattern asserted separately
  };

  const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);

  return {
    pool,
    killPidCalls,
    killSocketHoldersCalls,
    spawnedNames,
    fakeServers,
    now: nowValue,
    async cleanup() {
      for (const [socketPath, server] of fakeServers) {
        await stopFakeSocket(server, socketPath);
      }
    },
  };
}

// ---------------------------------------------------------------------------
// socketPathForKey / sessionNameForKey
// ---------------------------------------------------------------------------

describe("socketPathForKey", () => {
  it("is deterministic for the same key", () => {
    const key = "janet-user-42";
    expect(socketPathForKey(key)).toBe(socketPathForKey(key));
  });

  it("differs for different keys", () => {
    expect(socketPathForKey("key-a")).not.toBe(socketPathForKey("key-b"));
  });

  it("produces a path shorter than 104 bytes", () => {
    const path = socketPathForKey("some-user-session-key");
    expect(Buffer.byteLength(path, "utf8")).toBeLessThan(104);
  });

  it("ends with .sock", () => {
    expect(socketPathForKey("x")).toMatch(/\.sock$/);
  });
});

describe("sessionNameForKey", () => {
  it("starts with claude_hermes_", () => {
    expect(sessionNameForKey("key")).toMatch(/^claude_hermes_/);
  });

  it("is deterministic for the same key", () => {
    const key = "deterministic";
    expect(sessionNameForKey(key)).toBe(sessionNameForKey(key));
  });

  it("differs for different keys", () => {
    expect(sessionNameForKey("a")).not.toBe(sessionNameForKey("b"));
  });

  it("hash suffix is exactly 8 hex chars", () => {
    const name = sessionNameForKey("test-key");
    const suffix = name.replace("claude_hermes_", "");
    expect(suffix).toMatch(/^[0-9a-f]{8}$/);
  });
});

// ---------------------------------------------------------------------------
// getOrCreate: create and reuse
// ---------------------------------------------------------------------------

describe("ChannelsSessionPool.getOrCreate", () => {
  let fake: FakeDeps;

  afterEach(async () => { await fake?.cleanup(); });

  it("creates a new session on first call", async () => {
    fake = await makePool({});
    const state = await fake.pool.getOrCreate("user-1");
    expect(state).toBeDefined();
    expect(state.generation).toBeTruthy();
    expect(fake.pool.size).toBe(1);
  });

  it("returns the same state on second call (reuse)", async () => {
    fake = await makePool({});
    const s1 = await fake.pool.getOrCreate("user-1");
    const s2 = await fake.pool.getOrCreate("user-1");
    expect(s1).toBe(s2);
    expect(fake.pool.size).toBe(1);
  });

  it("generation token is a non-empty string", async () => {
    fake = await makePool({});
    const state = await fake.pool.getOrCreate("user-2");
    expect(typeof state.generation).toBe("string");
    expect(state.generation.length).toBeGreaterThan(0);
  });

  it("different keys produce different socket paths", async () => {
    fake = await makePool({});
    const s1 = await fake.pool.getOrCreate("key-a");
    const s2 = await fake.pool.getOrCreate("key-b");
    expect(s1.socketPath).not.toBe(s2.socketPath);
  });

  it("session name contains hash suffix (scoped, not generic)", async () => {
    fake = await makePool({});
    const state = await fake.pool.getOrCreate("user-3");
    expect(state.sessionName).toMatch(/^claude_hermes_[0-9a-f]{8}$/);
  });
});

// ---------------------------------------------------------------------------
// Generation token: changes on respawn
// ---------------------------------------------------------------------------

describe("generation token on respawn", () => {
  it("generation changes after stale session is replaced", async () => {
    const killPidCalls: Array<{ pid: number; signal: string }> = [];
    const fakeServers = new Map<string, Server>();

    let callCount = 0;
    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return 99000 + callCount++;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(pid, signal) { killPidCalls.push({ pid, signal }); },
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);

    const key = "respawn-test";
    const s1 = await pool.getOrCreate(key);
    const gen1 = s1.generation;

    // Force a stale state: evict directly, then getOrCreate should respawn.
    await pool.evict(key, false);
    const s2 = await pool.getOrCreate(key);
    const gen2 = s2.generation;

    expect(gen2).not.toBe(gen1);

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });
});

// ---------------------------------------------------------------------------
// getOrCreate reuse decision must use a LIVE-listener check (gap d)
//
// isSocket() (file-type check) says "present" even for a dangling socket
// file left behind by a process killed without graceful close(). Reusing
// such a session hands the caller a dead connection. getOrCreate() must
// detect this and respawn instead of reusing.
// ---------------------------------------------------------------------------

describe("getOrCreate reuse decision uses a live-listener check (gap d)", () => {
  it("does NOT reuse a session whose socket file is dangling (listener died without cleanup)", async () => {
    const fakeServers = new Map<string, Server>();
    let spawnCount = 0;
    let listenerProc: ReturnType<typeof Bun.spawn> | undefined;

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        spawnCount++;
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        if (spawnCount === 1) {
          // First spawn: a REAL subprocess holds the socket so the test can
          // kill JUST the listener (SIGKILL, no cleanup) independently of
          // the pool's own kill path.
          listenerProc = Bun.spawn(
            [
              "bun",
              "-e",
              `require("net").createServer(()=>{}).listen(${JSON.stringify(socketPath)})`,
            ],
            { stdout: "ignore", stderr: "ignore", stdin: "ignore" }
          );
        } else {
          void startFakeSocket(socketPath).then((srv) =>
            fakeServers.set(socketPath, srv)
          );
        }
        return 90000 + spawnCount;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; }, // the expect wrapper PID stays "alive" throughout
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    const key = "dangling-socket-key";

    const s1 = await pool.getOrCreate(key);

    // Wait for the real listener subprocess to actually bind before killing it.
    const deadline = Date.now() + 5000;
    while (!existsSync(s1.socketPath)) {
      if (Date.now() > deadline) throw new Error("listener socket never appeared");
      await new Promise((r) => setTimeout(r, 20));
    }

    // Kill it WITHOUT graceful close — the socket file survives on disk
    // (dangling), exactly the JA-25 shape.
    listenerProc!.kill("SIGKILL");
    await listenerProc!.exited;
    expect(existsSync(s1.socketPath)).toBe(true); // file still there, no listener

    // Second call for the SAME key: the old isSocket()-only check would
    // wrongly report "reuse" (file present + isPidAlive says proc alive).
    const s2 = await pool.getOrCreate(key);

    expect(s2.generation).not.toBe(s1.generation); // respawned, not reused
    expect(spawnCount).toBe(2);

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });
});

// ---------------------------------------------------------------------------
// ACP session mapping
// ---------------------------------------------------------------------------

describe("ACP to session_key mapping", () => {
  let fake: FakeDeps;

  afterEach(async () => { await fake?.cleanup(); });

  it("registerAcpSession / sessionKeyForAcp roundtrip", async () => {
    fake = await makePool({});
    fake.pool.registerAcpSession("acp-123", "user-42");
    expect(fake.pool.sessionKeyForAcp("acp-123")).toBe("user-42");
  });

  it("mapping survives eviction of the session", async () => {
    fake = await makePool({});
    await fake.pool.getOrCreate("user-42");
    fake.pool.registerAcpSession("acp-456", "user-42");

    await fake.pool.evict("user-42", false);

    // The ACP→key map is NOT cleared by eviction (only by unregisterAcpSession).
    expect(fake.pool.sessionKeyForAcp("acp-456")).toBe("user-42");
    expect(fake.pool.size).toBe(0);
  });

  it("unregisterAcpSession removes the mapping", async () => {
    fake = await makePool({});
    fake.pool.registerAcpSession("acp-789", "user-99");
    fake.pool.unregisterAcpSession("acp-789");
    expect(fake.pool.sessionKeyForAcp("acp-789")).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Capacity cap: full + idle session gets evicted to make room
// ---------------------------------------------------------------------------

describe("capacity cap", () => {
  it("evicts LRU idle when at MAX_SESSIONS", async () => {
    const maxSessions = 2;
    const originalMax = process.env["HERMES_CHANNELS_MAX_SESSIONS"];
    process.env["HERMES_CHANNELS_MAX_SESSIONS"] = String(maxSessions);

    const fakeServers = new Map<string, Server>();
    let callCount = 0;
    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return 99000 + callCount++;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);

    // Fill to max
    await pool.getOrCreate("key-1");
    await pool.getOrCreate("key-2");
    expect(pool.size).toBe(maxSessions);

    // Third session: should evict LRU idle and succeed
    const s3 = await pool.getOrCreate("key-3");
    expect(s3).toBeDefined();
    expect(pool.size).toBe(maxSessions);

    if (originalMax === undefined) {
      delete process.env["HERMES_CHANNELS_MAX_SESSIONS"];
    } else {
      process.env["HERMES_CHANNELS_MAX_SESSIONS"] = originalMax;
    }

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("throws PoolFullError when all sessions are busy", async () => {
    const maxSessions = 2;
    const originalMax = process.env["HERMES_CHANNELS_MAX_SESSIONS"];
    process.env["HERMES_CHANNELS_MAX_SESSIONS"] = String(maxSessions);

    const fakeServers = new Map<string, Server>();
    let callCount = 0;
    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return 99000 + callCount++;
      },
      createClient(_socketPath) { return makeMockClient(1); }, // all busy (pendingCount=1)
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);

    await pool.getOrCreate("busy-1");
    await pool.getOrCreate("busy-2");

    await expect(pool.getOrCreate("busy-3")).rejects.toBeInstanceOf(PoolFullError);

    if (originalMax === undefined) {
      delete process.env["HERMES_CHANNELS_MAX_SESSIONS"];
    } else {
      process.env["HERMES_CHANNELS_MAX_SESSIONS"] = originalMax;
    }

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });
});

// ---------------------------------------------------------------------------
// Graceful drain: evict waits for pendingCount===0
// ---------------------------------------------------------------------------

describe("graceful drain on evict", () => {
  it("waits for pendingCount to reach 0 before killing", async () => {
    const killPidCalls: Array<{ pid: number; signal: string }> = [];
    const fakeServers = new Map<string, Server>();

    // Client drains after 150ms
    const drainingClient = makeDrainingClient(1, 150);

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return 12345;
      },
      createClient(_socketPath) { return drainingClient; },
      killPid(pid, signal) { killPidCalls.push({ pid, signal }); },
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    await pool.getOrCreate("drain-test");

    // pendingCount=1 at eviction start
    expect(drainingClient.pendingCount).toBe(1);

    const start = Date.now();
    await pool.evict("drain-test", true);
    const elapsed = Date.now() - start;

    // Should have waited for drain (>= 100ms)
    expect(elapsed).toBeGreaterThanOrEqual(100);
    // Kill was called with the exact PID (12345), not a glob
    expect(killPidCalls.some((c) => c.pid === 12345)).toBe(true);

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });
});

// ---------------------------------------------------------------------------
// Kill by exact PID — never generic glob
// ---------------------------------------------------------------------------

describe("scoped kill assertion", () => {
  it("killPid receives the exact expectPid, not 0 or NaN", async () => {
    const killPidCalls: Array<{ pid: number; signal: string }> = [];
    const fakeServers = new Map<string, Server>();
    const EXPECTED_PID = 54321;

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return EXPECTED_PID;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(pid, signal) { killPidCalls.push({ pid, signal }); },
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    await pool.getOrCreate("scoped-kill-test");
    await pool.evict("scoped-kill-test", false);

    const pidKill = killPidCalls.find((c) => c.pid === EXPECTED_PID);
    expect(pidKill).toBeDefined();
    expect(pidKill!.pid).toBe(EXPECTED_PID);
    // Must NOT be 0, -1, or NaN
    expect(pidKill!.pid).not.toBe(0);
    expect(Number.isNaN(pidKill!.pid)).toBe(false);

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("session name used for last-resort pkill contains hash (not generic)", async () => {
    fake = await makePool({});
    const state = await fake.pool.getOrCreate("hash-kill-test");
    // The session name must match claude_hermes_<8 hex chars>
    // — NOT the bare "claude_hermes" generic pattern
    expect(state.sessionName).toMatch(/^claude_hermes_[0-9a-f]{8}$/);
    await fake.cleanup();
  });

  it("killByName is called with exact scoped name, never generic pattern", async () => {
    const killByNameCalls: string[] = [];
    const fakeServers = new Map<string, Server>();
    const EXPECTED_PID = 54322;

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return EXPECTED_PID;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(name) { killByNameCalls.push(name); },
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    await pool.getOrCreate("generic-kill-test");
    await pool.evict("generic-kill-test", false);

    // killByName MUST have been called
    expect(killByNameCalls.length).toBeGreaterThanOrEqual(1);
    // Every call MUST use the scoped hash name — NEVER bare "claude_hermes"
    for (const name of killByNameCalls) {
      expect(name).toMatch(/^claude_hermes_[0-9a-f]{8}$/);
      expect(name).not.toBe("claude_hermes");
      expect(name).not.toBe("claude_hermes_acp");
    }

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  // Satisfy TypeScript — afterEach reference
  let fake: FakeDeps;
  afterEach(async () => { await fake?.cleanup(); });
});

// ---------------------------------------------------------------------------
// OS-level orphan reconciliation (gap b)
//
// scanAndEvictIdle() only ever walks the pool's OWN in-memory Map. A parent
// process that crashed and got respawned starts with an EMPTY Map — any
// session it doesn't know about (an orphan surviving from before the crash)
// is invisible to it forever. reconcileOrphans() closes that blind spot by
// scanning THIS INSTANCE's own socket directory ($HERMES_HOME/run/channels)
// for untracked ".sock" files and sweeping them with the same scoped,
// exact-target primitives evict() already uses (killSocketHolders by exact
// path, killByName by the hash-derived scoped session name) — never a bare
// process-name pattern, and never anything outside this instance's own
// socket dir (so it can never reach a different HERMES_HOME, e.g. prod).
// ---------------------------------------------------------------------------

describe("reconcileOrphans (gap b)", () => {
  const ORIGINAL_HERMES_HOME = process.env["HERMES_HOME"];
  let scratchHome: string;

  beforeEach(() => {
    // NOTE: deliberately NOT under tmpdir() — macOS's per-user tmp path
    // (/var/folders/.../T/) is long enough that channels/<16hex>.sock would
    // blow the 104-byte AF_UNIX path limit the pool itself enforces.
    scratchHome = `/tmp/jt-${randomUUID().slice(0, 8)}`;
    process.env["HERMES_HOME"] = scratchHome;
  });

  afterEach(() => {
    if (ORIGINAL_HERMES_HOME === undefined) {
      delete process.env["HERMES_HOME"];
    } else {
      process.env["HERMES_HOME"] = ORIGINAL_HERMES_HOME;
    }
    if (existsSync(scratchHome)) {
      rmSync(scratchHome, { recursive: true, force: true });
    }
  });

  it("sweeps an untracked socket file (orphan from a crashed prior process) and leaves tracked sessions alone", async () => {
    const fakeServers = new Map<string, Server>();
    const killPidCalls: Array<{ pid: number; signal: string }> = [];
    const killByNameCalls: string[] = [];
    const ORPHAN_LEAF_PID = 64500;

    const deps: PoolDeps = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return 64001;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(pid, signal) { killPidCalls.push({ pid, signal }); },
      killSocketHolders(_socketPath) {
        throw new Error("reconcileOrphans must not call killSocketHolders anymore — use listSocketHolderPids + the ancestry walk");
      },
      now() { return Date.now(); },
      isPidAlive(_pid) { return false; }, // dies cleanly on SIGTERM — no escalation noise in this test
      killByName(name) { killByNameCalls.push(name); },
      listSocketHolderPids(_socketPath) { return [ORPHAN_LEAF_PID]; },
      getPpid(pid) { return pid === ORPHAN_LEAF_PID ? 1 : undefined; }, // leaf is already the top of the orphaned tree
      getCommandLine(_pid) { return "/opt/homebrew/bin/bun server.ts"; },
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);

    // One legitimately tracked session — must survive the sweep untouched.
    const tracked = await pool.getOrCreate("tracked-key");
    expect(existsSync(tracked.socketPath)).toBe(true);

    // An untracked orphan socket, in the SAME instance's channels dir, that
    // the pool never spawned in this process's lifetime (simulating a
    // surviving child from a prior, now-dead, supervisor process). The fake
    // socket file just needs to exist on disk for readdir()/existsSync() —
    // the actual "process" being cleaned up is fully mocked via
    // listSocketHolderPids/getPpid/getCommandLine above.
    const channelsDir = dirname(tracked.socketPath);
    const orphanSocketPath = join(channelsDir, "abcdef0123456789.sock");
    const orphanServer = await startFakeSocket(orphanSocketPath);

    await pool.reconcileOrphans();

    // The orphan's leaf PID was killed via the exact-PID ancestry walk.
    expect(killPidCalls).toContainEqual({ pid: ORPHAN_LEAF_PID, signal: "SIGTERM" });
    // Review item 2: killByName must NEVER be called from reconcileOrphans
    // — pkill -f matches by command line machine-wide, not scoped to this
    // instance's HERMES_HOME, so it's the one cross-instance kill risk
    // (e.g. a hash8 collision hitting a live prod session) with no
    // same-instance benefit (the ancestry walk already reaches every real
    // process in the chain by exact PID).
    expect(killByNameCalls.length).toBe(0);
    // Never swept via the tracked session's own PID.
    expect(killPidCalls.some((c) => c.pid === tracked.expectPid)).toBe(false);

    // Tracked session untouched.
    expect(pool.size).toBe(1);
    expect(existsSync(tracked.socketPath)).toBe(true);
    expect(existsSync(orphanSocketPath)).toBe(false); // stale file removed

    await stopFakeSocket(orphanServer, orphanSocketPath);
    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("kills the FULL live orphan subtree (leaf + plugin wrapper + claude + expect) via exact-PID ancestry walk", async () => {
    // Reproduces the actual shape observed live: a leaf process (the
    // plugin's own bun server.ts, found via lsof on the socket) is a
    // child of an intermediate wrapper (the plugin's "bun run ... start"
    // process, whose own argv carries no session-specific string), which
    // is a child of claude (--name claude_hermes_<hash8>), which is a
    // child of expect (-f <launcherExpPath>), reparented to PID 1.
    const LEAF_PID = 70501;
    const WRAPPER_PID = 70502;
    const CLAUDE_PID = 70503;
    const EXPECT_PID = 70504;
    const launcherExpPath = "/fake/launcher.exp";
    const sessionName = "claude_hermes_deadbeef";

    const ppidMap: Record<number, number> = {
      [LEAF_PID]: WRAPPER_PID,
      [WRAPPER_PID]: CLAUDE_PID,
      [CLAUDE_PID]: EXPECT_PID,
      [EXPECT_PID]: 1,
    };
    const cmdlineMap: Record<number, string> = {
      [LEAF_PID]: "/opt/homebrew/bin/bun server.ts",
      [WRAPPER_PID]: "bun run --cwd /some/marketplace/path --shell=bun --silent start",
      [CLAUDE_PID]: `/Users/x/.local/bin/claude --name ${sessionName} --permission-mode auto`,
      [EXPECT_PID]: `expect -f ${launcherExpPath}`,
    };

    const killPidCalls: Array<{ pid: number; signal: string }> = [];
    const fakeServers = new Map<string, Server>();

    const deps: PoolDeps = {
      spawnLauncher() { throw new Error("should not spawn in this test"); },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(pid, signal) { killPidCalls.push({ pid, signal }); },
      killSocketHolders(_socketPath) {
        throw new Error("reconcileOrphans must not call killSocketHolders anymore");
      },
      now() { return Date.now(); },
      isPidAlive(_pid) { return false; }, // each hop dies cleanly on SIGTERM
      killByName(_name) { throw new Error("reconcileOrphans must never call killByName"); },
      listSocketHolderPids(_socketPath) { return [LEAF_PID]; },
      getPpid(pid) { return ppidMap[pid]; },
      getCommandLine(pid) { return cmdlineMap[pid]; },
    };

    const pool = new ChannelsSessionPool(launcherExpPath, deps);

    // Untracked orphan socket whose filename hashes to "deadbeef" (first 8
    // hex chars), matching sessionName above.
    process.env["HERMES_HOME"] = scratchHome;
    const channelsDir = join(scratchHome, "run", "channels");
    mkdirSync(channelsDir, { recursive: true });
    const orphanSocketPath = join(channelsDir, "deadbeef01234567.sock");
    const orphanServer = await startFakeSocket(orphanSocketPath);

    await pool.reconcileOrphans();

    // Every hop in the real chain was killed by its exact PID.
    for (const pid of [LEAF_PID, WRAPPER_PID, CLAUDE_PID, EXPECT_PID]) {
      expect(killPidCalls.some((c) => c.pid === pid && c.signal === "SIGTERM")).toBe(true);
    }
    expect(existsSync(orphanSocketPath)).toBe(false);

    await stopFakeSocket(orphanServer, orphanSocketPath);
    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("identity guard stops the ancestry walk at an unrelated process instead of killing it", async () => {
    // The leaf (hop 0) is pre-verified by the exact-path lsof lookup that
    // found it. Its "parent" here is a totally unrelated process — no
    // session-specific string in its own argv, and ITS parent doesn't
    // match this session's signature either. The walk must stop there,
    // leaving that unrelated process untouched, rather than assuming
    // everything above a verified node is safe to kill.
    const LEAF_PID = 70601;
    const UNRELATED_PID = 70602;
    const launcherExpPath = "/fake/launcher.exp";
    const sessionName = "claude_hermes_c0ffee00";

    const ppidMap: Record<number, number> = {
      [LEAF_PID]: UNRELATED_PID,
      [UNRELATED_PID]: 1,
    };
    const cmdlineMap: Record<number, string> = {
      [LEAF_PID]: "/opt/homebrew/bin/bun server.ts",
      [UNRELATED_PID]: "some-totally-unrelated-process --foo --bar",
      // PID 1 deliberately has no entry — getCommandLine returns undefined.
    };

    const killPidCalls: Array<{ pid: number; signal: string }> = [];

    const deps: PoolDeps = {
      spawnLauncher() { throw new Error("should not spawn in this test"); },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(pid, signal) { killPidCalls.push({ pid, signal }); },
      killSocketHolders(_socketPath) {
        throw new Error("reconcileOrphans must not call killSocketHolders anymore");
      },
      now() { return Date.now(); },
      isPidAlive(_pid) { return false; },
      killByName(_name) { throw new Error("reconcileOrphans must never call killByName"); },
      listSocketHolderPids(_socketPath) { return [LEAF_PID]; },
      getPpid(pid) { return ppidMap[pid]; },
      getCommandLine(pid) { return cmdlineMap[pid]; },
    };

    const pool = new ChannelsSessionPool(launcherExpPath, deps);

    process.env["HERMES_HOME"] = scratchHome;
    const channelsDir = join(scratchHome, "run", "channels");
    mkdirSync(channelsDir, { recursive: true });
    const orphanSocketPath = join(channelsDir, "c0ffee0089abcdef.sock");
    const orphanServer = await startFakeSocket(orphanSocketPath);

    await pool.reconcileOrphans();

    // The pre-verified leaf WAS killed...
    expect(killPidCalls.some((c) => c.pid === LEAF_PID)).toBe(true);
    // ...but the walk stopped before touching the unrelated ancestor —
    // it matches neither `sessionName` nor `launcherExpPath`, and ITS OWN
    // parent (PID 1, no known cmdline) doesn't either.
    expect(killPidCalls.some((c) => c.pid === UNRELATED_PID)).toBe(false);
    void sessionName; // documents the fixture's derivation; not asserted on directly

    await stopFakeSocket(orphanServer, orphanSocketPath);
  });

  it("refuses to touch a subtree still owned by a LIVE SIBLING supervisor sharing this HERMES_HOME (review item 4)", async () => {
    // reconcileOrphans()'s whole premise is "untracked in THIS process's
    // Map == the owning supervisor is dead." That's false when two
    // supervisor processes share the same HERMES_HOME (observed live —
    // multiple concurrent acp_entrypoint subprocesses back distinct
    // Python-side avatar/API sessions under ONE gateway). Their Maps are
    // never synchronized, so instance A's reconcile tick can see a
    // perfectly live session it never spawned as "untracked." Here the
    // ancestry walk reaches a live process matching the acp_entrypoint
    // signature (the sibling's supervisor) INSTEAD of reaching PID 1 —
    // the walk must refuse the WHOLE subtree, not just skip that one
    // ancestor, and must NOT remove the socket file either (still legitimately
    // in use by the sibling).
    const LEAF_PID = 70701;
    const WRAPPER_PID = 70702;
    const CLAUDE_PID = 70703;
    const EXPECT_PID = 70704;
    const SIBLING_SUPERVISOR_PID = 70705; // ALIVE — expect's real parent, not PID 1
    const launcherExpPath = "/fake/launcher.exp";
    const sessionName = "claude_hermes_5ib1ing0";

    const ppidMap: Record<number, number> = {
      [LEAF_PID]: WRAPPER_PID,
      [WRAPPER_PID]: CLAUDE_PID,
      [CLAUDE_PID]: EXPECT_PID,
      [EXPECT_PID]: SIBLING_SUPERVISOR_PID, // NOT 1 — still has a live parent!
      [SIBLING_SUPERVISOR_PID]: 1,
    };
    const cmdlineMap: Record<number, string> = {
      [LEAF_PID]: "/opt/homebrew/bin/bun server.ts",
      [WRAPPER_PID]: "bun run --cwd /some/marketplace/path --shell=bun --silent start",
      [CLAUDE_PID]: `/Users/x/.local/bin/claude --name ${sessionName} --permission-mode auto`,
      [EXPECT_PID]: `expect -f ${launcherExpPath}`,
      // Matches the SAME acp_entrypoint.ts path this pool instance itself
      // uses — indistinguishable from "this instance's own supervisor",
      // which is exactly the point: a sibling running the identical code
      // under the identical HERMES_HOME looks identical by argv.
      [SIBLING_SUPERVISOR_PID]: "/opt/homebrew/bin/bun run /fake/src/acp_entrypoint.ts",
    };

    const killPidCalls: Array<{ pid: number; signal: string }> = [];

    const deps: PoolDeps = {
      spawnLauncher() { throw new Error("should not spawn in this test"); },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(pid, signal) { killPidCalls.push({ pid, signal }); },
      killSocketHolders(_socketPath) {
        throw new Error("reconcileOrphans must not call killSocketHolders anymore");
      },
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) { throw new Error("reconcileOrphans must never call killByName"); },
      listSocketHolderPids(_socketPath) { return [LEAF_PID]; },
      getPpid(pid) { return ppidMap[pid]; },
      getCommandLine(pid) { return cmdlineMap[pid]; },
    };

    const pool = new ChannelsSessionPool(launcherExpPath, deps);

    process.env["HERMES_HOME"] = scratchHome;
    const channelsDir = join(scratchHome, "run", "channels");
    mkdirSync(channelsDir, { recursive: true });
    const orphanSocketPath = join(channelsDir, "5ib1ing089abcdef.sock");
    const orphanServer = await startFakeSocket(orphanSocketPath);

    await pool.reconcileOrphans();

    // NOTHING was killed — not even the pre-verified leaf. A subtree with
    // ANY live parent above it (not positively reparented to PID 1) is not
    // a confirmed orphan, full stop.
    for (const pid of [LEAF_PID, WRAPPER_PID, CLAUDE_PID, EXPECT_PID, SIBLING_SUPERVISOR_PID]) {
      expect(killPidCalls.some((c) => c.pid === pid)).toBe(false);
    }
    // The socket file was left in place too — it may still be in active use.
    expect(existsSync(orphanSocketPath)).toBe(true);

    await stopFakeSocket(orphanServer, orphanSocketPath);
  });

  it("regression: a genuinely dead-owner orphan (reparented to PID 1) is still fully swept, sibling-guard notwithstanding", async () => {
    // Same 4-level chain as the "kills the FULL live orphan subtree" test,
    // re-asserted here explicitly as the regression guard for review item
    // 4 — the sibling-supervisor guard must NEVER cause a false negative
    // on the ordinary, most-common case: a subtree whose ancestry
    // positively reaches PID 1 (no live parent anywhere) must still be
    // fully killed, exactly as before this review round.
    const LEAF_PID = 70801;
    const WRAPPER_PID = 70802;
    const CLAUDE_PID = 70803;
    const EXPECT_PID = 70804;
    const launcherExpPath = "/fake/launcher.exp";
    const sessionName = "claude_hermes_dead0wne"; // first 8 chars of the 16-char socket hash below

    const ppidMap: Record<number, number> = {
      [LEAF_PID]: WRAPPER_PID,
      [WRAPPER_PID]: CLAUDE_PID,
      [CLAUDE_PID]: EXPECT_PID,
      [EXPECT_PID]: 1, // positively reparented to init — genuine orphan
    };
    const cmdlineMap: Record<number, string> = {
      [LEAF_PID]: "/opt/homebrew/bin/bun server.ts",
      [WRAPPER_PID]: "bun run --cwd /some/marketplace/path --shell=bun --silent start",
      [CLAUDE_PID]: `/Users/x/.local/bin/claude --name ${sessionName} --permission-mode auto`,
      [EXPECT_PID]: `expect -f ${launcherExpPath}`,
    };

    const killPidCalls: Array<{ pid: number; signal: string }> = [];

    const deps: PoolDeps = {
      spawnLauncher() { throw new Error("should not spawn in this test"); },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(pid, signal) { killPidCalls.push({ pid, signal }); },
      killSocketHolders(_socketPath) {
        throw new Error("reconcileOrphans must not call killSocketHolders anymore");
      },
      now() { return Date.now(); },
      isPidAlive(_pid) { return false; },
      killByName(_name) { throw new Error("reconcileOrphans must never call killByName"); },
      listSocketHolderPids(_socketPath) { return [LEAF_PID]; },
      getPpid(pid) { return ppidMap[pid]; },
      getCommandLine(pid) { return cmdlineMap[pid]; },
    };

    const pool = new ChannelsSessionPool(launcherExpPath, deps);

    process.env["HERMES_HOME"] = scratchHome;
    const channelsDir = join(scratchHome, "run", "channels");
    mkdirSync(channelsDir, { recursive: true });
    const orphanSocketPath = join(channelsDir, "dead0wne01234567.sock"); // 16 hex-ish chars -> hash8 = "dead0wne"
    const orphanServer = await startFakeSocket(orphanSocketPath);

    await pool.reconcileOrphans();

    for (const pid of [LEAF_PID, WRAPPER_PID, CLAUDE_PID, EXPECT_PID]) {
      expect(killPidCalls.some((c) => c.pid === pid && c.signal === "SIGTERM")).toBe(true);
    }
    expect(existsSync(orphanSocketPath)).toBe(false);

    await stopFakeSocket(orphanServer, orphanSocketPath);
  });

  it("does NOT sweep a session that is still mid-spawn (socket exists on disk, Map not yet updated) — review item 1", async () => {
    // The exact race: spawn() only calls this.sessions.set(...) AFTER
    // waitForSocket() resolves, but the socket FILE can appear on disk
    // well before that (the launcher/plugin creates it; waitForSocket just
    // polls for it). If reconcileOrphans() ticks in that window, the
    // in-flight session looks exactly like an orphan — same-Map-absence,
    // same disk-presence — and would get killed out from under a session
    // that is still legitimately being created.
    const fakeServers = new Map<string, Server>();
    const killPidCalls: Array<{ pid: number; signal: string }> = [];

    const deps: PoolDeps = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        // Delay the fake socket's creation so there's a real window where
        // the file exists on disk but getOrCreate() hasn't returned yet.
        setTimeout(() => {
          void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        }, 250);
        return 70001;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(pid, signal) { killPidCalls.push({ pid, signal }); },
      killSocketHolders(_socketPath) {
        throw new Error("reconcileOrphans must not call killSocketHolders anymore");
      },
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
      listSocketHolderPids(_socketPath) {
        throw new Error("must not even look for holders — the pendingSpawns/isTracked guard must skip this entry entirely");
      },
      getPpid(_pid) { return undefined; },
      getCommandLine(_pid) { return undefined; },
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    const key = "in-flight-key";

    const spawnPromise = pool.getOrCreate(key);

    // Land inside the (250ms socket-created, ~500ms default-poll-notices)
    // window: file exists, Map entry does not yet.
    await new Promise((r) => setTimeout(r, 350));
    await pool.reconcileOrphans();

    expect(killPidCalls.length).toBe(0);

    const state = await spawnPromise; // let the spawn complete normally
    expect(state).toBeDefined();
    expect(pool.size).toBe(1);

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("is a no-op when the channels directory does not exist yet", async () => {
    const deps: PoolDeps = {
      spawnLauncher() { throw new Error("should not spawn"); },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {
        throw new Error("should not be called — nothing to reconcile");
      },
      killSocketHolders(_socketPath) {
        throw new Error("should not be called — nothing to reconcile");
      },
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {
        throw new Error("should not be called — nothing to reconcile");
      },
      listSocketHolderPids(_socketPath) {
        throw new Error("should not be called — nothing to reconcile");
      },
      getPpid(_pid) { return undefined; },
      getCommandLine(_pid) { return undefined; },
    };
    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    await expect(pool.reconcileOrphans()).resolves.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Real-time child-exit detection (gap c)
//
// Previously Bun.spawn's return value (just a PID) was used with no
// onExit/child.exited wiring at all — the pool only ever discovered a dead
// child reactively, on the NEXT getOrCreate() call for that key (or never,
// if no further request for that key ever arrives). spawnLauncher now takes
// an optional onExit callback; the pool must react immediately: remove the
// Map entry and clean up the socket file, without waiting for another
// getOrCreate() call.
// ---------------------------------------------------------------------------

describe("real-time child-exit detection (gap c)", () => {
  it("removes the session from the pool as soon as the child exits unexpectedly, with no further call", async () => {
    const fakeServers = new Map<string, Server>();
    let capturedOnExit: ((code: number | null) => void) | undefined;
    const EXPECTED_PID = 62001;

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env, onExit) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        capturedOnExit = onExit;
        return EXPECTED_PID;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    const state = await pool.getOrCreate("real-time-exit-key");
    expect(pool.size).toBe(1);
    expect(capturedOnExit).toBeDefined();

    // Simulate the child dying unexpectedly (crash / OOM-kill / SIGKILL from
    // outside) — nobody called evict(), nobody called getOrCreate() again.
    capturedOnExit!(1);
    // The callback may itself be async internally; give it a tick.
    await new Promise((r) => setTimeout(r, 50));

    expect(pool.size).toBe(0); // reacted immediately, no further call needed
    expect(existsSync(state.socketPath)).toBe(false); // socket file cleaned up

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("guards by generation, not PID — a reused PID from an older spawn must not clobber a newer session (discretionary item A, ABA hardening)", async () => {
    const fakeServers = new Map<string, Server>();
    const capturedOnExits: Array<(code: number | null) => void> = [];
    const REUSED_PID = 65001; // returned for EVERY spawn — simulates OS PID reuse across respawns

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env, onExit) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        if (onExit) capturedOnExits.push(onExit);
        return REUSED_PID;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    const key = "aba-key";

    const s1 = await pool.getOrCreate(key);
    await pool.evict(key, false); // legitimate respawn path
    const s2 = await pool.getOrCreate(key);

    // Confirms the ABA setup: same PID, but a genuinely different session.
    expect(s1.expectPid).toBe(s2.expectPid);
    expect(s2.generation).not.toBe(s1.generation);

    // The FIRST spawn's onExit fires late. A PID-only guard would see
    // "expectPid matches the current Map entry" (true — same reused PID!)
    // and wrongly tear down s2. Must be a no-op: identity is the
    // generation nonce, not the OS-recycled PID number.
    capturedOnExits[0]!(1);
    await new Promise((r) => setTimeout(r, 50));

    expect(pool.size).toBe(1); // s2 untouched
    expect(existsSync(s2.socketPath)).toBe(true);

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("does NOT clobber a NEWER respawned session under the same key (stale exit from an old spawn)", async () => {
    const fakeServers = new Map<string, Server>();
    const capturedOnExits: Array<(code: number | null) => void> = [];
    let spawnCount = 0;

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env, onExit) {
        spawnCount++;
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        if (onExit) capturedOnExits.push(onExit);
        return 63000 + spawnCount;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    const key = "respawn-race-key";

    const s1 = await pool.getOrCreate(key);
    await pool.evict(key, false); // legitimate respawn path
    const s2 = await pool.getOrCreate(key);
    expect(s2.generation).not.toBe(s1.generation);
    expect(pool.size).toBe(1);

    // The FIRST spawn's onExit fires late (e.g. delayed process-table
    // cleanup) — it must be a no-op now, since the Map holds s2, not s1.
    capturedOnExits[0]!(1);
    await new Promise((r) => setTimeout(r, 50));

    expect(pool.size).toBe(1); // s2 untouched
    expect(existsSync(s2.socketPath)).toBe(true); // s2's socket untouched

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });
});

// ---------------------------------------------------------------------------
// evict() escalation (gap a)
//
// Previously evict() fired SIGTERM + killSocketHolders, waited a fixed
// 500ms, then ALWAYS called killByName regardless of whether the process
// actually died. It never checked isPidAlive after signaling. Now it must:
//   - skip killByName when the PID is confirmed dead after SIGTERM
//   - escalate to SIGKILL when the PID survives SIGTERM
//   - fall back to killByName only if the PID survives even SIGKILL
// ---------------------------------------------------------------------------

describe("evict() escalation (gap a)", () => {
  it("does NOT call killByName when the process died cleanly after SIGTERM", async () => {
    const killPidCalls: Array<{ pid: number; signal: string }> = [];
    const killByNameCalls: string[] = [];
    const fakeServers = new Map<string, Server>();
    const EXPECTED_PID = 61001;

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return EXPECTED_PID;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(pid, signal) { killPidCalls.push({ pid, signal }); },
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return false; }, // confirmed dead right after SIGTERM
      killByName(name) { killByNameCalls.push(name); },
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    await pool.getOrCreate("clean-death-key");
    await pool.evict("clean-death-key", false);

    expect(killPidCalls).toEqual([{ pid: EXPECTED_PID, signal: "SIGTERM" }]); // no SIGKILL needed
    expect(killByNameCalls.length).toBe(0); // process confirmed dead — no last resort needed

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("escalates to SIGKILL when the process survives SIGTERM, then stops (confirmed dead)", async () => {
    const killPidCalls: Array<{ pid: number; signal: string }> = [];
    const killByNameCalls: string[] = [];
    const fakeServers = new Map<string, Server>();
    const EXPECTED_PID = 61002;

    let sigtermSent = false;
    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return EXPECTED_PID;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(pid, signal) {
        killPidCalls.push({ pid, signal });
        if (signal === "SIGTERM") sigtermSent = true;
      },
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      // Survives SIGTERM (still alive right after it), confirmed dead once SIGKILL has been sent.
      isPidAlive(_pid) { return sigtermSent && !killPidCalls.some((c) => c.signal === "SIGKILL"); },
      killByName(name) { killByNameCalls.push(name); },
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    await pool.getOrCreate("survives-sigterm-key");
    await pool.evict("survives-sigterm-key", false);

    expect(killPidCalls).toEqual([
      { pid: EXPECTED_PID, signal: "SIGTERM" },
      { pid: EXPECTED_PID, signal: "SIGKILL" },
    ]);
    expect(killByNameCalls.length).toBe(0); // SIGKILL confirmed it dead — no last resort needed

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("falls back to killByName only when the process survives even SIGKILL", async () => {
    const killPidCalls: Array<{ pid: number; signal: string }> = [];
    const killByNameCalls: string[] = [];
    const fakeServers = new Map<string, Server>();
    const EXPECTED_PID = 61003;

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return EXPECTED_PID;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(pid, signal) { killPidCalls.push({ pid, signal }); },
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; }, // never dies — worst case (e.g. zombie/defunct)
      killByName(name) { killByNameCalls.push(name); },
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    const state = await pool.getOrCreate("never-dies-key");
    await pool.evict("never-dies-key", false);

    expect(killPidCalls).toEqual([
      { pid: EXPECTED_PID, signal: "SIGTERM" },
      { pid: EXPECTED_PID, signal: "SIGKILL" },
    ]);
    expect(killByNameCalls).toEqual([state.sessionName]); // last resort, scoped name only

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });
});

// ---------------------------------------------------------------------------
// Idle eviction: scanAndEvictIdle respects TTL
// ---------------------------------------------------------------------------

describe("idle eviction", () => {
  it("does not evict a session that is still within TTL", async () => {
    const fake = await makePool({});
    await fake.pool.getOrCreate("idle-key");
    // now = createdAt, so idleMs=0
    await fake.pool.scanAndEvictIdle(fake.now.value);
    expect(fake.pool.size).toBe(1);
    await fake.cleanup();
  });

  it("evicts a session that exceeded TTL and has pendingCount===0", async () => {
    const IDLE_TTL_MS_ORIG = process.env["HERMES_CHANNELS_IDLE_TTL_MS"];
    process.env["HERMES_CHANNELS_IDLE_TTL_MS"] = "1000"; // 1s TTL for test

    const fakeServers = new Map<string, Server>();
    const nowValue = { value: Date.now() };

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return 77777;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return nowValue.value; },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    await pool.getOrCreate("idle-expire-key");

    // Advance clock past TTL
    nowValue.value += 2000;
    await pool.scanAndEvictIdle(nowValue.value);
    expect(pool.size).toBe(0);

    if (IDLE_TTL_MS_ORIG === undefined) {
      delete process.env["HERMES_CHANNELS_IDLE_TTL_MS"];
    } else {
      process.env["HERMES_CHANNELS_IDLE_TTL_MS"] = IDLE_TTL_MS_ORIG;
    }

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("does not evict a busy session even if past TTL", async () => {
    const IDLE_TTL_MS_ORIG = process.env["HERMES_CHANNELS_IDLE_TTL_MS"];
    process.env["HERMES_CHANNELS_IDLE_TTL_MS"] = "1000";

    const fakeServers = new Map<string, Server>();
    const nowValue = { value: Date.now() };

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return 77778;
      },
      createClient(_socketPath) { return makeMockClient(1); }, // busy!
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return nowValue.value; },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    await pool.getOrCreate("busy-idle-key");

    nowValue.value += 2000;
    await pool.scanAndEvictIdle(nowValue.value);
    expect(pool.size).toBe(1); // still alive

    if (IDLE_TTL_MS_ORIG === undefined) {
      delete process.env["HERMES_CHANNELS_IDLE_TTL_MS"];
    } else {
      process.env["HERMES_CHANNELS_IDLE_TTL_MS"] = IDLE_TTL_MS_ORIG;
    }

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });
});

// ---------------------------------------------------------------------------
// HERMES_CHANNELS_MIN_WARM: protects N most-recently-active sessions from idle eviction
// ---------------------------------------------------------------------------

describe("HERMES_CHANNELS_MIN_WARM", () => {
  it("MIN_WARM unset (0): existing idle eviction behavior unchanged", async () => {
    const IDLE_TTL_MS_ORIG = process.env["HERMES_CHANNELS_IDLE_TTL_MS"];
    const MIN_WARM_ORIG = process.env["HERMES_CHANNELS_MIN_WARM"];
    process.env["HERMES_CHANNELS_IDLE_TTL_MS"] = "1000";
    delete process.env["HERMES_CHANNELS_MIN_WARM"];

    const fakeServers = new Map<string, Server>();
    const nowValue = { value: Date.now() };

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return 91000 + fakeServers.size;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return nowValue.value; },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);

    // Create 3 sessions with distinct lastActivityAt by staggering the clock.
    await pool.getOrCreate("warm-none-1"); nowValue.value += 100;
    await pool.getOrCreate("warm-none-2"); nowValue.value += 100;
    await pool.getOrCreate("warm-none-3"); nowValue.value += 100;
    expect(pool.size).toBe(3);

    // Advance clock past TTL — all sessions should be evicted (MIN_WARM=0).
    nowValue.value += 2000;
    await pool.scanAndEvictIdle(nowValue.value);
    expect(pool.size).toBe(0);

    if (IDLE_TTL_MS_ORIG === undefined) {
      delete process.env["HERMES_CHANNELS_IDLE_TTL_MS"];
    } else {
      process.env["HERMES_CHANNELS_IDLE_TTL_MS"] = IDLE_TTL_MS_ORIG;
    }
    if (MIN_WARM_ORIG === undefined) {
      delete process.env["HERMES_CHANNELS_MIN_WARM"];
    } else {
      process.env["HERMES_CHANNELS_MIN_WARM"] = MIN_WARM_ORIG;
    }

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("MIN_WARM=2: the 2 most-recently-active sessions survive past idle TTL", async () => {
    const IDLE_TTL_MS_ORIG = process.env["HERMES_CHANNELS_IDLE_TTL_MS"];
    const MIN_WARM_ORIG = process.env["HERMES_CHANNELS_MIN_WARM"];
    process.env["HERMES_CHANNELS_IDLE_TTL_MS"] = "1000";
    process.env["HERMES_CHANNELS_MIN_WARM"] = "2";

    const fakeServers = new Map<string, Server>();
    const nowValue = { value: 1_000_000 }; // fixed start — deterministic

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return 92000 + fakeServers.size;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return nowValue.value; },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);

    // Create sessions with distinct lastActivityAt by staggering the clock.
    // "warm-old" is oldest (lowest lastActivityAt), "warm-new2" is newest.
    await pool.getOrCreate("warm-old");  // lastActivityAt = 1_000_000
    nowValue.value += 100;
    await pool.getOrCreate("warm-mid");  // lastActivityAt = 1_000_100
    nowValue.value += 100;
    await pool.getOrCreate("warm-new1"); // lastActivityAt = 1_000_200
    nowValue.value += 100;
    await pool.getOrCreate("warm-new2"); // lastActivityAt = 1_000_300
    expect(pool.size).toBe(4);

    // Advance clock past TTL for all sessions.
    nowValue.value += 5000;
    await pool.scanAndEvictIdle(nowValue.value);

    // Only the 2 most recent (warm-new1, warm-new2) should survive.
    expect(pool.size).toBe(2);
    // The oldest two should be gone.
    expect(pool["sessions"].has("warm-old")).toBe(false);
    expect(pool["sessions"].has("warm-mid")).toBe(false);
    // The newest two should be protected.
    expect(pool["sessions"].has("warm-new1")).toBe(true);
    expect(pool["sessions"].has("warm-new2")).toBe(true);

    if (IDLE_TTL_MS_ORIG === undefined) {
      delete process.env["HERMES_CHANNELS_IDLE_TTL_MS"];
    } else {
      process.env["HERMES_CHANNELS_IDLE_TTL_MS"] = IDLE_TTL_MS_ORIG;
    }
    if (MIN_WARM_ORIG === undefined) {
      delete process.env["HERMES_CHANNELS_MIN_WARM"];
    } else {
      process.env["HERMES_CHANNELS_MIN_WARM"] = MIN_WARM_ORIG;
    }

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });
});

// ---------------------------------------------------------------------------
// HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS: excludes claude.ai account connectors
// ---------------------------------------------------------------------------

describe("HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS", () => {
  it("injects ENABLE_CLAUDEAI_MCP_SERVERS=0 when env var is set", async () => {
    const origEnv = process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"];
    process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"] = "1";

    const fakeServers = new Map<string, Server>();
    const spawnedEnvs: Record<string, string>[] = [];

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        spawnedEnvs.push(env);
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return 88001;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    await pool.getOrCreate("connector-exclude-key");

    expect(spawnedEnvs.length).toBe(1);
    expect(spawnedEnvs[0]["ENABLE_CLAUDEAI_MCP_SERVERS"]).toBe("0");

    if (origEnv === undefined) {
      delete process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"];
    } else {
      process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"] = origEnv;
    }

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("does NOT inject ENABLE_CLAUDEAI_MCP_SERVERS when env var is absent", async () => {
    const origEnv = process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"];
    delete process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"];

    const fakeServers = new Map<string, Server>();
    const spawnedEnvs: Record<string, string>[] = [];

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        spawnedEnvs.push(env);
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return 88002;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    await pool.getOrCreate("connector-default-key");

    expect(spawnedEnvs.length).toBe(1);
    expect(spawnedEnvs[0]["ENABLE_CLAUDEAI_MCP_SERVERS"]).toBeUndefined();

    if (origEnv !== undefined) {
      process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"] = origEnv;
    }

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });

  it("does NOT inject ENABLE_CLAUDEAI_MCP_SERVERS when env var is '0'", async () => {
    const origEnv = process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"];
    process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"] = "0";

    const fakeServers = new Map<string, Server>();
    const spawnedEnvs: Record<string, string>[] = [];

    const deps: Partial<PoolDeps> = {
      spawnLauncher(_launcherExpPath, env) {
        spawnedEnvs.push(env);
        const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
        void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
        return 88003;
      },
      createClient(_socketPath) { return makeMockClient(0); },
      killPid(_pid, _signal) {},
      killSocketHolders(_socketPath) {},
      now() { return Date.now(); },
      isPidAlive(_pid) { return true; },
      killByName(_name) {},
    };

    const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);
    await pool.getOrCreate("connector-off-key");

    expect(spawnedEnvs.length).toBe(1);
    expect(spawnedEnvs[0]["ENABLE_CLAUDEAI_MCP_SERVERS"]).toBeUndefined();

    if (origEnv === undefined) {
      delete process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"];
    } else {
      process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"] = origEnv;
    }

    for (const [socketPath, server] of fakeServers) {
      await stopFakeSocket(server, socketPath);
    }
  });
});
