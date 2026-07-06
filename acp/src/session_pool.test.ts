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
import { unlinkSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

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

  const deps: PoolDeps = {
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
    const deps: PoolDeps = {
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
    const deps: PoolDeps = {
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
    const deps: PoolDeps = {
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

    const deps: PoolDeps = {
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

    const deps: PoolDeps = {
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

    const deps: PoolDeps = {
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

    const deps: PoolDeps = {
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

    const deps: PoolDeps = {
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

    const deps: PoolDeps = {
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

    const deps: PoolDeps = {
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

    const deps: PoolDeps = {
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

    const deps: PoolDeps = {
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

    const deps: PoolDeps = {
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
