import { describe, it, before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { connect, StringCodec, type NatsConnection } from "nats";
import handler, {
  parseSessionKey,
  flushSession,
  _buffers as buffers,
} from "./handler.js";

// ── Unit tests ──────────────────────────────────────────────────────

describe("parseSessionKey", () => {
  it("extracts channel, participant, and agent_id from key", () => {
    const meta = parseSessionKey("whatsapp:+447444361435:main");
    assert.equal(meta.channel, "whatsapp");
    assert.equal(meta.participant, "+447444361435");
    assert.equal(meta.agent_id, "main");
  });

  it("handles participant with colons (e.g. slack user:channel)", () => {
    const meta = parseSessionKey("slack:U123:C456:kai");
    assert.equal(meta.channel, "slack");
    assert.equal(meta.participant, "U123:C456");
    assert.equal(meta.agent_id, "kai");
  });

  it("returns empty object for key with fewer than 3 parts", () => {
    const meta = parseSessionKey("invalid");
    assert.deepEqual(meta, {});
  });

  it("context fields override parsed key", () => {
    const meta = parseSessionKey("whatsapp:+447444361435:main", {
      channel: "web",
      agent_id: "scout",
      participant: "user-42",
    });
    assert.equal(meta.channel, "web");
    assert.equal(meta.agent_id, "scout");
    assert.equal(meta.participant, "user-42");
  });
});

// ── Integration tests (requires live NATS) ──────────────────────────

describe("handler integration", () => {
  const NATS_URL = "nats://127.0.0.1:4222";
  const SUBJECT = "swarm.gateway.session.chunk";
  const SESSION_KEY = "test:+440000000000:main";

  let sub: NatsConnection;
  const sc = StringCodec();
  const received: any[] = [];

  before(async () => {
    process.env.NATS_URL = NATS_URL;

    // Connect without token — works for both local (if no auth) and CI
    const connectOpts: { servers: string; token?: string } = {
      servers: NATS_URL,
    };
    if (process.env.NATS_TOKEN) {
      connectOpts.token = process.env.NATS_TOKEN;
    }

    sub = await connect(connectOpts);
    const subscription = sub.subscribe(SUBJECT);
    (async () => {
      for await (const msg of subscription) {
        received.push(JSON.parse(sc.decode(msg.data)));
      }
    })();

    // Boot the handler's NATS connection
    await handler({ type: "gateway", action: "startup" });
  });

  after(async () => {
    delete process.env.NATS_TOKEN;
    await sub?.drain();
  });

  beforeEach(() => {
    received.length = 0;
    buffers.clear();
  });

  it("buffers messages on session:after", async () => {
    await handler({
      type: "session",
      action: "after",
      sessionKey: SESSION_KEY,
      context: {
        messages: [
          { role: "user", content: "hello", timestamp: new Date().toISOString() },
        ],
      },
    });

    const buf = buffers.get(SESSION_KEY);
    assert.ok(buf, "session buffer should exist");
    assert.equal(buf.messages.length, 1);
    assert.equal(buf.messages[0].content, "hello");
  });

  it("accumulates messages across multiple turns", async () => {
    for (let i = 0; i < 3; i++) {
      await handler({
        type: "session",
        action: "after",
        sessionKey: SESSION_KEY,
        context: {
          messages: [
            { role: "user", content: `msg-${i}`, timestamp: new Date().toISOString() },
          ],
        },
      });
    }

    const buf = buffers.get(SESSION_KEY);
    assert.ok(buf);
    assert.equal(buf.messages.length, 3);
  });

  it("flushes on command:stop with is_final=true", async () => {
    await handler({
      type: "session",
      action: "after",
      sessionKey: SESSION_KEY,
      context: {
        messages: [
          { role: "assistant", content: "bye", timestamp: new Date().toISOString() },
        ],
      },
    });

    await handler({
      type: "command",
      action: "stop",
      sessionKey: SESSION_KEY,
    });

    await delay(100);

    assert.equal(received.length, 1, "should have received one chunk");
    const chunk = received[0];
    assert.equal(chunk.session_key, SESSION_KEY);
    assert.equal(chunk.is_final, true);
    assert.equal(chunk.flush_reason, "session_stop");
    assert.equal(chunk.message_count, 1);
    assert.equal(chunk.messages[0].content, "bye");
    assert.equal(chunk.chunk_index, 0);
    assert.ok(chunk.chunk_id, "should have a chunk_id UUID");
    assert.ok(chunk.flushed_at, "should have flushed_at timestamp");

    assert.equal(buffers.has(SESSION_KEY), false);
  });

  it("flushes when buffer exceeds MAX_BUFFER_MESSAGES (50)", async () => {
    const messages = Array.from({ length: 51 }, (_, i) => ({
      role: "user",
      content: `msg-${i}`,
      timestamp: new Date().toISOString(),
    }));

    await handler({
      type: "session",
      action: "after",
      sessionKey: SESSION_KEY,
      context: { messages },
    });

    await delay(100);

    assert.equal(received.length, 1, "should have auto-flushed");
    assert.equal(received[0].flush_reason, "buffer_full");
    assert.equal(received[0].is_final, false);
    assert.equal(received[0].message_count, 51);

    const buf = buffers.get(SESSION_KEY);
    assert.ok(buf, "session buffer should still exist");
    assert.equal(buf.messages.length, 0);
  });

  it("increments chunk_index across flushes", async () => {
    const batch = Array.from({ length: 50 }, (_, i) => ({
      role: "user",
      content: `a-${i}`,
      timestamp: new Date().toISOString(),
    }));

    await handler({
      type: "session",
      action: "after",
      sessionKey: SESSION_KEY,
      context: { messages: batch },
    });
    await delay(50);

    await handler({
      type: "session",
      action: "after",
      sessionKey: SESSION_KEY,
      context: {
        messages: [{ role: "user", content: "last", timestamp: new Date().toISOString() }],
      },
    });
    await handler({
      type: "command",
      action: "stop",
      sessionKey: SESSION_KEY,
    });
    await delay(100);

    assert.equal(received.length, 2);
    assert.equal(received[0].chunk_index, 0);
    assert.equal(received[1].chunk_index, 1);
  });

  it("parses session metadata from key", async () => {
    await handler({
      type: "session",
      action: "after",
      sessionKey: "whatsapp:+447444361435:kai",
      context: {
        messages: [
          { role: "user", content: "hi", timestamp: new Date().toISOString() },
        ],
      },
    });

    await handler({
      type: "command",
      action: "stop",
      sessionKey: "whatsapp:+447444361435:kai",
    });
    await delay(100);

    const chunk = received[0];
    assert.equal(chunk.session_metadata.channel, "whatsapp");
    assert.equal(chunk.session_metadata.participant, "+447444361435");
    assert.equal(chunk.session_metadata.agent_id, "kai");
  });

  it("command:stop on empty/nonexistent session is a no-op", async () => {
    await handler({
      type: "command",
      action: "stop",
      sessionKey: "nonexistent:session:key",
    });
    await delay(50);
    assert.equal(received.length, 0);
  });

  it("ignores events with no sessionKey", async () => {
    await handler({
      type: "session",
      action: "after",
      context: { messages: [{ role: "user", content: "lost" }] },
    });
    assert.equal(buffers.size, 0);
  });
});

// ── Alexandria token fetch ──────────────────────────────────────────

describe("Alexandria token fallback", () => {
  const originalFetch = globalThis.fetch;

  after(() => {
    globalThis.fetch = originalFetch;
  });

  it("handler still works when Alexandria is unreachable (env fallback)", async () => {
    process.env.NATS_TOKEN = "test-token";
    globalThis.fetch = (() =>
      Promise.reject(new Error("connection refused"))) as any;

    await handler({ type: "gateway", action: "startup" });
    delete process.env.NATS_TOKEN;
  });
});

function delay(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
