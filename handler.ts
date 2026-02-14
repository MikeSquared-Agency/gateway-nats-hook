import type { NatsConnection } from "nats";
import { connect, StringCodec } from "nats";

interface SessionBuffer {
  messages: unknown[];
  chunkIndex: number;
  lastActivity: number;
  idleTimer: ReturnType<typeof setTimeout> | null;
  periodicTimer: ReturnType<typeof setInterval> | null;
  metadata: Record<string, unknown>;
}

const IDLE_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes
const MAX_BUFFER_MESSAGES = 50;
const PERIODIC_FLUSH_MS = 15 * 60 * 1000; // 15 minutes
const SUBJECT = "swarm.gateway.session.chunk";

export function parseSessionKey(
  key: string,
  context?: Record<string, unknown>,
): Record<string, unknown> {
  // Session key format: {channel}:{participant}:{agent_id}
  const parts = key.split(":");
  const metadata: Record<string, unknown> = {};

  if (parts.length >= 3) {
    metadata.channel = parts[0];
    metadata.participant = parts.slice(1, -1).join(":");
    metadata.agent_id = parts[parts.length - 1];
  }

  // Merge any additional context metadata
  if (context?.channel) metadata.channel = context.channel;
  if (context?.agent_id) metadata.agent_id = context.agent_id;
  if (context?.participant) metadata.participant = context.participant;

  return metadata;
}

interface HandlerState {
  nc: NatsConnection | null;
  buffers: Map<string, SessionBuffer>;
}

async function fetchTokenFromAlexandria(): Promise<string | undefined> {
  const alexandriaUrl = process.env.ALEXANDRIA_URL || "http://127.0.0.1:8500";
  const maxAttempts = 3;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const res = await fetch(`${alexandriaUrl}/api/v1/secrets/NATS_TOKEN`, {
        headers: { "X-Agent-ID": "gateway" },
      });
      if (!res.ok) return undefined;
      const body = (await res.json()) as { data?: { value?: string } };
      return body?.data?.value;
    } catch {
      if (attempt < maxAttempts) {
        console.warn(
          `[nats-publisher] Alexandria attempt ${attempt}/${maxAttempts} failed, retrying in 1s...`,
        );
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  }
  return undefined;
}

export interface Handler {
  (event: Record<string, unknown>): Promise<void>;
  /** Exposed for testing -- the session buffers map. */
  buffers: Map<string, SessionBuffer>;
  /** Flush a specific session (exported for tests / manual use). */
  flushSession: (key: string, reason: string, isFinal: boolean) => Promise<void>;
}

export function createHandler(): Handler {
  const sc = StringCodec();
  const state: HandlerState = {
    nc: null,
    buffers: new Map<string, SessionBuffer>(),
  };

  async function initNats() {
    if (state.nc && !state.nc.isClosed()) return;
    const url = process.env.NATS_URL || "nats://127.0.0.1:4222";
    const token = (await fetchTokenFromAlexandria()) || process.env.NATS_TOKEN;

    const opts: { servers: string; token?: string } = { servers: url };
    if (token) {
      opts.token = token;
    } else {
      console.warn(
        "[nats-publisher] no NATS_TOKEN from Alexandria or env -- connecting without auth",
      );
    }

    try {
      state.nc = await connect(opts);
      console.log(`[nats-publisher] connected to ${url}`);

      // Reset nc on unexpected disconnect so subsequent calls re-init
      state.nc.closed().then(() => {
        console.warn("[nats-publisher] NATS connection closed, will reconnect on next use");
        state.nc = null;
      });
    } catch (err) {
      console.error("[nats-publisher] failed to connect:", err);
      state.nc = null;
    }
  }

  function cleanupSession(key: string) {
    const buf = state.buffers.get(key);
    if (!buf) return;
    if (buf.idleTimer) clearTimeout(buf.idleTimer);
    if (buf.periodicTimer) clearInterval(buf.periodicTimer);
    state.buffers.delete(key);
  }

  async function flushSession(key: string, reason: string, isFinal: boolean) {
    const buf = state.buffers.get(key);
    if (!buf || buf.messages.length === 0) {
      if (isFinal) cleanupSession(key);
      return;
    }

    if (!state.nc) return;

    const chunk = {
      session_key: key,
      chunk_id: crypto.randomUUID(),
      chunk_index: buf.chunkIndex++,
      is_final: isFinal,
      messages: buf.messages,
      message_count: buf.messages.length,
      session_metadata: buf.metadata,
      flushed_at: new Date().toISOString(),
      flush_reason: reason,
    };

    try {
      state.nc.publish(SUBJECT, sc.encode(JSON.stringify(chunk)));
    } catch (err) {
      console.error(`[nats-publisher] publish failed for ${key}:`, err);
      return; // Don't clear buffer on publish failure
    }

    // Clear buffer but keep session tracking
    buf.messages = [];

    if (isFinal) {
      cleanupSession(key);
    }
  }

  async function bufferMessage(event: Record<string, unknown>) {
    // Ensure NATS is connected (lazy init if gateway:startup wasn't fired)
    if (!state.nc) await initNats();

    const key = event.sessionKey as string | undefined;
    if (!key) return;

    let buf = state.buffers.get(key);
    if (!buf) {
      buf = {
        messages: [],
        chunkIndex: 0,
        lastActivity: Date.now(),
        idleTimer: null,
        periodicTimer: null,
        metadata: parseSessionKey(key, event.context as Record<string, unknown> | undefined),
      };
      state.buffers.set(key, buf);

      // Start periodic flush for this session
      buf.periodicTimer = setInterval(() => {
        flushSession(key, "periodic", false);
      }, PERIODIC_FLUSH_MS);
    }

    // Extract messages from event context
    const ctx = event.context as Record<string, unknown> | undefined;
    if (ctx?.messages) {
      buf.messages.push(...(ctx.messages as unknown[]));
    }
    buf.lastActivity = Date.now();

    // Reset idle timer
    if (buf.idleTimer) clearTimeout(buf.idleTimer);
    buf.idleTimer = setTimeout(() => {
      flushSession(key, "idle_timeout", false);
    }, IDLE_TIMEOUT_MS);

    // Buffer full -- flush now
    if (buf.messages.length >= MAX_BUFFER_MESSAGES) {
      await flushSession(key, "buffer_full", false);
    }
  }

  async function handler(event: Record<string, unknown>) {
    switch (`${event.type}:${event.action}`) {
      case "gateway:startup":
        await initNats();
        break;
      case "session:after":
        await bufferMessage(event);
        break;
      case "command:stop":
        await flushSession(event.sessionKey as string, "session_stop", true);
        break;
    }
  }

  // Attach helpers for testing and external use
  handler.buffers = state.buffers;
  handler.flushSession = flushSession;

  return handler as Handler;
}

// Default instance for backward compatibility
const defaultHandler = createHandler();
export default defaultHandler;

// Re-export the default instance's buffers for backward compatibility
export const _buffers = defaultHandler.buffers;
