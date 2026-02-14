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

  it("returns empty object for two-part key", () => {
    const meta = parseSessionKey("channel:agent");
    assert.deepEqual(meta, {});
  });

  it("returns empty object for empty string", () => {
    const meta = parseSessionKey("");
    assert.deepEqual(meta, {});
  });

  it("handles exactly 3 parts", () => {
    const meta = parseSessionKey("web:user1:bot");
    assert.equal(meta.channel, "web");
    assert.equal(meta.participant, "user1");
    assert.equal(meta.agent_id, "bot");
  });

  it("applies partial context overrides", () => {
    const meta = parseSessionKey("whatsapp:+447444361435:main", {
      channel: "sms",
    });
    assert.equal(meta.channel, "sms");
    assert.equal(meta.participant, "+447444361435");
    assert.equal(meta.agent_id, "main");
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

  it("ignores unknown event types", async () => {
    await handler({
      type: "unknown",
      action: "something",
      sessionKey: SESSION_KEY,
    });
    assert.equal(buffers.size, 0);
    assert.equal(received.length, 0);
  });

  it("handles session:after with no context.messages", async () => {
    await handler({
      type: "session",
      action: "after",
      sessionKey: SESSION_KEY,
      context: {},
    });

    const buf = buffers.get(SESSION_KEY);
    assert.ok(buf, "session buffer should be created");
    assert.equal(buf.messages.length, 0, "no messages should be buffered");
  });

  it("handles session:after with empty messages array", async () => {
    await handler({
      type: "session",
      action: "after",
      sessionKey: SESSION_KEY,
      context: { messages: [] },
    });

    const buf = buffers.get(SESSION_KEY);
    assert.ok(buf, "session buffer should be created");
    assert.equal(buf.messages.length, 0);
  });

  it("isolates multiple concurrent sessions", async () => {
    const KEY_A = "web:alice:main";
    const KEY_B = "slack:bob:kai";

    await handler({
      type: "session",
      action: "after",
      sessionKey: KEY_A,
      context: {
        messages: [{ role: "user", content: "from alice", timestamp: new Date().toISOString() }],
      },
    });
    await handler({
      type: "session",
      action: "after",
      sessionKey: KEY_B,
      context: {
        messages: [
          { role: "user", content: "from bob 1", timestamp: new Date().toISOString() },
          { role: "user", content: "from bob 2", timestamp: new Date().toISOString() },
        ],
      },
    });

    assert.equal(buffers.get(KEY_A)!.messages.length, 1);
    assert.equal(buffers.get(KEY_B)!.messages.length, 2);

    // Stop only session A
    await handler({ type: "command", action: "stop", sessionKey: KEY_A });
    await delay(100);

    assert.equal(received.length, 1);
    assert.equal(received[0].session_key, KEY_A);
    assert.equal(received[0].message_count, 1);

    // Session B still active
    assert.ok(buffers.has(KEY_B));
    assert.equal(buffers.get(KEY_B)!.messages.length, 2);

    // Clean up session B
    await handler({ type: "command", action: "stop", sessionKey: KEY_B });
    await delay(100);
  });

  it("cleans up buffer on final flush with existing but empty buffer", async () => {
    // Create a session, then flush manually to empty it, then stop
    await handler({
      type: "session",
      action: "after",
      sessionKey: SESSION_KEY,
      context: {
        messages: [{ role: "user", content: "hi", timestamp: new Date().toISOString() }],
      },
    });

    // Manually flush (non-final) to empty the buffer
    await flushSession(SESSION_KEY, "manual", false);
    await delay(50);

    assert.ok(buffers.has(SESSION_KEY), "session should still be tracked");
    assert.equal(buffers.get(SESSION_KEY)!.messages.length, 0);

    // Now final stop on empty buffer — should clean up without publishing
    received.length = 0;
    await handler({ type: "command", action: "stop", sessionKey: SESSION_KEY });
    await delay(50);

    assert.equal(received.length, 0, "should not publish an empty chunk");
    assert.equal(buffers.has(SESSION_KEY), false, "session should be cleaned up");
  });

  it("chunk contains valid UUID and ISO timestamp", async () => {
    await handler({
      type: "session",
      action: "after",
      sessionKey: SESSION_KEY,
      context: {
        messages: [{ role: "user", content: "test", timestamp: new Date().toISOString() }],
      },
    });
    await handler({ type: "command", action: "stop", sessionKey: SESSION_KEY });
    await delay(100);

    const chunk = received[0];
    assert.match(
      chunk.chunk_id,
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
      "chunk_id should be a valid UUID"
    );
    assert.ok(
      !isNaN(Date.parse(chunk.flushed_at)),
      "flushed_at should be a valid ISO timestamp"
    );
  });

  it("gateway:startup is idempotent", async () => {
    // startup was already called in before() — calling again should not throw
    await handler({ type: "gateway", action: "startup" });
    // Verify handler still works by buffering + flushing
    await handler({
      type: "session",
      action: "after",
      sessionKey: SESSION_KEY,
      context: {
        messages: [{ role: "user", content: "after double startup", timestamp: new Date().toISOString() }],
      },
    });
    await handler({ type: "command", action: "stop", sessionKey: SESSION_KEY });
    await delay(100);

    assert.equal(received.length, 1);
    assert.equal(received[0].messages[0].content, "after double startup");
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
