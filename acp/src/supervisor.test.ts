/**
 * Unit tests for supervisor.ts — pure bits only.
 *
 * Does NOT spawn claude, expect, or any live socket session.
 * Tests:
 *   - resolveSocketPath: env override and default path.
 *   - sessionDecision: all three branches.
 *   - waitForSocket: resolves when socket appears, rejects on timeout.
 *   - isSocket: detects a real socket vs non-socket vs missing path.
 *
 * Socket detection uses a real temporary AF_UNIX server created inline
 * (no expect, no claude — just Node.js `net.createServer`).
 */

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { createServer, type Server } from "node:net";
import { unlinkSync, existsSync, writeFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  DEFAULT_SOCKET_PATH,
  resolveSocketPath,
  sessionDecision,
  waitForSocket,
  isSocket,
} from "./supervisor.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function tempSocketPath(): string {
  return join(tmpdir(), `sup-test-${randomBytes(6).toString("hex")}.sock`);
}

function tempFilePath(): string {
  return join(tmpdir(), `sup-test-${randomBytes(6).toString("hex")}.txt`);
}

/** Start a real AF_UNIX server so isSocket() sees a live socket. */
function startTempSocket(socketPath: string): Promise<Server> {
  return new Promise((resolve, reject) => {
    const server = createServer(() => {});
    server.on("error", reject);
    server.listen(socketPath, () => resolve(server));
  });
}

function stopServer(server: Server, socketPath: string): Promise<void> {
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
// resolveSocketPath
// ---------------------------------------------------------------------------

describe("resolveSocketPath", () => {
  const ORIGINAL_ENV = process.env["HERMES_CHANNEL_SOCKET"];

  afterEach(() => {
    if (ORIGINAL_ENV === undefined) {
      delete process.env["HERMES_CHANNEL_SOCKET"];
    } else {
      process.env["HERMES_CHANNEL_SOCKET"] = ORIGINAL_ENV;
    }
  });

  it("returns DEFAULT_SOCKET_PATH when env var is not set", () => {
    delete process.env["HERMES_CHANNEL_SOCKET"];
    expect(resolveSocketPath()).toBe(DEFAULT_SOCKET_PATH);
  });

  it("returns the env var value when HERMES_CHANNEL_SOCKET is set", () => {
    process.env["HERMES_CHANNEL_SOCKET"] = "/tmp/custom.sock";
    expect(resolveSocketPath()).toBe("/tmp/custom.sock");
  });

  it("DEFAULT_SOCKET_PATH contains .hermes/run/hermes-channel.sock", () => {
    expect(DEFAULT_SOCKET_PATH).toMatch(/\.hermes\/run\/hermes-channel\.sock$/);
  });
});

// ---------------------------------------------------------------------------
// sessionDecision
// ---------------------------------------------------------------------------

describe("sessionDecision", () => {
  it("returns 'reuse' when socket is present and proc is running", () => {
    expect(sessionDecision(true, true)).toBe("reuse");
  });

  it("returns 'stale' when socket is present but proc is not running", () => {
    expect(sessionDecision(true, false)).toBe("stale");
  });

  it("returns 'launch' when socket is absent and proc is not running", () => {
    expect(sessionDecision(false, false)).toBe("launch");
  });

  it("returns 'launch' when socket is absent even if proc is somehow running", () => {
    // Socket absent + proc running = unusual (e.g. proc starting up) → treat as launch.
    expect(sessionDecision(false, true)).toBe("launch");
  });
});

// ---------------------------------------------------------------------------
// isSocket
// ---------------------------------------------------------------------------

describe("isSocket", () => {
  let server: Server;
  let socketPath: string;

  beforeEach(async () => {
    socketPath = tempSocketPath();
    server = await startTempSocket(socketPath);
  });

  afterEach(async () => {
    await stopServer(server, socketPath);
  });

  it("returns true for a live Unix socket", () => {
    expect(isSocket(socketPath)).toBe(true);
  });

  it("returns false for a path that does not exist", () => {
    expect(isSocket("/tmp/does-not-exist-supervisor-test.sock")).toBe(false);
  });

  it("returns false for a regular file", () => {
    const filePath = tempFilePath();
    writeFileSync(filePath, "not a socket");
    try {
      expect(isSocket(filePath)).toBe(false);
    } finally {
      try { unlinkSync(filePath); } catch { /* ignore */ }
    }
  });
});

// ---------------------------------------------------------------------------
// waitForSocket — against a real temp socket
// ---------------------------------------------------------------------------

describe("waitForSocket", () => {
  it("resolves immediately when the socket already exists", async () => {
    const socketPath = tempSocketPath();
    const server = await startTempSocket(socketPath);
    try {
      // Should resolve quickly — socket is already up.
      await expect(waitForSocket(socketPath, 2000, 50)).resolves.toBeUndefined();
    } finally {
      await stopServer(server, socketPath);
    }
  });

  it("rejects with timeout error when socket never appears", async () => {
    const socketPath = tempSocketPath(); // never created
    await expect(waitForSocket(socketPath, 100, 20)).rejects.toThrow("did not appear");
  });

  it("resolves when socket appears after a delay", async () => {
    const socketPath = tempSocketPath();
    let server: Server | undefined;

    // Create the socket after 150ms.
    const createDelay = setTimeout(async () => {
      server = await startTempSocket(socketPath);
    }, 150);

    try {
      await expect(waitForSocket(socketPath, 2000, 50)).resolves.toBeUndefined();
    } finally {
      clearTimeout(createDelay);
      if (server) await stopServer(server, socketPath);
    }
  });
});
