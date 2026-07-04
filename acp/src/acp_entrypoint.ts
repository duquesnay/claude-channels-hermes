/**
 * acp_entrypoint.ts — stdio ACP server entry point with session pool lifecycle.
 *
 * On startup:
 *   1. Instantiates the ChannelsSessionPool (lazy — sessions spawn on first use).
 *   2. Starts the idle-eviction loop.
 *   3. Starts serving ACP over stdin/stdout.
 *
 * On SIGTERM:
 *   - pool.shutdown() drains all sessions and kills their processes (scoped).
 *   - process.exit(0)
 *
 * ALL logging goes to STDERR — a single stray byte on stdout before
 * the connection is established wedges Janet's NDJSON reader.
 *
 * Wiring canonical from dist/examples/agent.js:
 *   const stream = ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin));
 *   new AgentSideConnection(createAgent, stream);
 *
 * Note: in ndJsonStream the first arg is the WRITE sink (stdout) and the
 * second arg is the READ source (stdin) — the variable names in the SDK
 * example are misleading but the positional order is correct.
 */

import { Readable, Writable } from "node:stream";
import { ndJsonStream, AgentSideConnection } from "@agentclientprotocol/sdk";
import { createAgent, resolveLauncherExpPath } from "./acp_server.ts";
import { ChannelsSessionPool } from "./session_pool.ts";

// ---------------------------------------------------------------------------
// Instantiate the session pool (sessions spawn lazily on first getOrCreate)
// ---------------------------------------------------------------------------

const launcherExpPath = resolveLauncherExpPath();
const pool = new ChannelsSessionPool(launcherExpPath);
pool.startIdleEviction();

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

process.on("SIGTERM", () => {
  void (async () => {
    process.stderr.write("claude-channels-acp-server: SIGTERM received, shutting down pool...\n");
    await pool.shutdown();
    process.exit(0);
  })();
});

// ---------------------------------------------------------------------------
// Start the ACP server
// ---------------------------------------------------------------------------

process.stderr.write("claude-channels-acp-server: ready (lazy session pool)\n");

const stream = ndJsonStream(
  Writable.toWeb(process.stdout), // write sink (agent → client)
  Readable.toWeb(process.stdin)   // read source (client → agent)
);

new AgentSideConnection(
  (conn) => createAgent(conn, pool),
  stream
);
