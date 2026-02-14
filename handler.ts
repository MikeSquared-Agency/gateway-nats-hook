import nats from "nats";
const { connect, StringCodec } = nats;
type NatsConnection = ReturnType<typeof connect> extends Promise<infer T> ? T : never;

let nc: NatsConnection | null = null;
const sc = StringCodec();
const buffers = new Map<string, SessionBuffer>();

interface SessionBuffer {
  messages: any[];
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

export default async function handler(event: any) {
  switch (`${event.type}:${event.action}`) {
    case "gateway:startup":
      await initNats();
      break;
    case "session:after":
      await bufferMessage(event);
      break;
    case "command:stop":
      await flushSession(event.sessionKey, "session_stop", true);
      break;
  }
}

async function fetchTokenFromAlexandria(): Promise<string | undefined> {
  const alexandriaUrl =
    process.env.ALEXANDRIA_URL || "http://127.0.0.1:8500";
  const maxAttempts = 3;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const res = await fetch(
        `${alexandriaUrl}/api/v1/secrets/NATS_TOKEN`,
        { headers: { "X-Agent-ID": "gateway" } }
      );
      if (!res.ok) return undefined;
      const body = (await res.json()) as { data?: { value?: string } };
      return body?.data?.value;
    } catch {
      if (attempt < maxAttempts) {
        console.warn(
          `[nats-publisher] Alexandria attempt ${attempt}/${maxAttempts} failed, retrying in 1s…`
        );
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  }
  return undefined;
}

async function initNats() {
  if (nc && !nc.isClosed()) return;
  const url = process.env.NATS_URL || "nats://127.0.0.1:4222";
  const token =
    (await fetchTokenFromAlexandria()) || process.env.NATS_TOKEN;

  const opts: { servers: string; token?: string } = { servers: url };
  if (token) {
    opts.token = token;
  } else {
    console.warn(
      "[nats-publisher] no NATS_TOKEN from Alexandria or env — connecting without auth"
    );
  }

  try {
    nc = await connect(opts);
    console.log(`[nats-publisher] connected to ${url}`);

    // Reset nc on unexpected disconnect so subsequent calls re-init
    nc.closed().then(() => {
      console.warn("[nats-publisher] NATS connection closed, will reconnect on next use");
      nc = null;
    });
  } catch (err) {
    console.error("[nats-publisher] failed to connect:", err);
    nc = null;
  }
}

async function bufferMessage(event: any) {
  // Ensure NATS is connected (lazy init if gateway:startup wasn't fired)
  if (!nc) await initNats();

  const key = event.sessionKey;
  if (!key) return;

  let buf = buffers.get(key);
  if (!buf) {
    buf = {
      messages: [],
      chunkIndex: 0,
      lastActivity: Date.now(),
      idleTimer: null,
      periodicTimer: null,
      metadata: parseSessionKey(key, event.context),
    };
    buffers.set(key, buf);

    // Start periodic flush for this session
    buf.periodicTimer = setInterval(() => {
      flushSession(key, "periodic", false);
    }, PERIODIC_FLUSH_MS);
  }

  // Extract messages from event context
  if (event.context?.messages) {
    buf.messages.push(...event.context.messages);
  }
  buf.lastActivity = Date.now();

  // Reset idle timer
  if (buf.idleTimer) clearTimeout(buf.idleTimer);
  buf.idleTimer = setTimeout(() => {
    flushSession(key, "idle_timeout", false);
  }, IDLE_TIMEOUT_MS);

  // Buffer full — flush now
  if (buf.messages.length >= MAX_BUFFER_MESSAGES) {
    await flushSession(key, "buffer_full", false);
  }
}

export function parseSessionKey(
  key: string,
  context?: Record<string, unknown>
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

export { buffers as _buffers };

export async function flushSession(
  key: string,
  reason: string,
  isFinal: boolean
) {
  const buf = buffers.get(key);
  if (!buf || buf.messages.length === 0) {
    if (isFinal) cleanupSession(key);
    return;
  }

  if (!nc) return;

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
    nc.publish(SUBJECT, sc.encode(JSON.stringify(chunk)));
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

function cleanupSession(key: string) {
  const buf = buffers.get(key);
  if (!buf) return;
  if (buf.idleTimer) clearTimeout(buf.idleTimer);
  if (buf.periodicTimer) clearInterval(buf.periodicTimer);
  buffers.delete(key);
}
