---
name: nats-publisher
description: "Publishes session transcript chunks to NATS for Chronicle/Dredd pipeline"
metadata:
  {
    "openclaw":
      {
        "emoji": "📡",
        "events": ["gateway:startup", "session:after", "command:stop"],
      },
  }
---

# NATS Publisher Hook

Publishes session transcript chunks to NATS (`swarm.gateway.session.chunk`) for the decision pipeline. Chronicle stores them, Dredd processes them.

Buffers messages per session, flushes on:
- 5 minute idle timeout
- 50+ messages buffered
- Session end
- 15 minute periodic flush
