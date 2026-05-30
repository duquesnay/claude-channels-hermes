/**
 * acp_entrypoint.ts — stdio ACP server entry point with supervisor lifecycle.
 *
 * On startup:
 *   1. Runs the supervisor: launches (or reuses) the claude --channels session
 *      and waits for the hermes-channel Unix socket to be ready (~75s budget).
 *   2. Connects the shared HermesChannelClient to that socket.
 *   3. Starts serving ACP over stdin/stdout.
 *
 * On SIGTERM:
 *   - Calls supervisor.shutdown() (async, awaited) to clean up the session.
 *   - Closes the hermes-channel client.
 *   - Exits cleanly.
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
import { createAgent } from "./acp_server.ts";
import { HermesChannelClient } from "./hermes_channel_client.ts";
import { ensureSession, shutdown, resolveSocketPath } from "./supervisor.ts";

// ---------------------------------------------------------------------------
// Startup: supervisor first, then ACP
// ---------------------------------------------------------------------------

const socketPath = resolveSocketPath();

process.stderr.write("claude-channels-acp-server: starting supervisor...\n");
try {
  await ensureSession(socketPath);
} catch (err) {
  process.stderr.write(
    `claude-channels-acp-server: supervisor failed to start: ${err}\n`
  );
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Shared hermes-channel client (one socket, multiplexed across all sessions)
// ---------------------------------------------------------------------------

const client = new HermesChannelClient(socketPath);

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

process.on("SIGTERM", () => {
  // Must be async — shutdown kills processes and waits for them.
  void (async () => {
    process.stderr.write("claude-channels-acp-server: SIGTERM received, shutting down...\n");
    await shutdown(socketPath);
    client.close();
    process.exit(0);
  })();
});

// ---------------------------------------------------------------------------
// Start the ACP server
// ---------------------------------------------------------------------------

process.stderr.write("claude-channels-acp-server: ready\n");

const stream = ndJsonStream(
  Writable.toWeb(process.stdout), // write sink (agent → client)
  Readable.toWeb(process.stdin)   // read source (client → agent)
);

new AgentSideConnection(
  (conn) => createAgent(conn, { client }),
  stream
);
