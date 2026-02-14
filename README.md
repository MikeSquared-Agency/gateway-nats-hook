# gateway-nats-hook

OpenClaw workspace hook that publishes session transcript chunks to NATS for the Chronicle/Dredd pipeline.

## What it does

Listens to gateway lifecycle events and buffers session messages in memory. Chunks are flushed to the NATS subject `swarm.gateway.session.chunk` when:

- The buffer reaches **50 messages**
- A session has been **idle for 5 minutes**
- A **periodic flush** fires every 15 minutes
- The session **stops** (`command:stop`)

## Install

```bash
git clone https://github.com/MikeSquared-Agency/gateway-nats-hook.git
cd gateway-nats-hook
npm install
```

Symlink into your OpenClaw workspace:

```bash
ln -sfn ~/gateway-nats-hook ~/.openclaw/workspace/hooks/nats-publisher
```

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `NATS_URL` | No | `nats://127.0.0.1:4222` | NATS server URL |
| `NATS_TOKEN` | No | — | Auth token (falls back to Alexandria lookup) |
| `ALEXANDRIA_URL` | No | `http://127.0.0.1:8500` | Alexandria secrets API for token resolution |

If neither Alexandria nor `NATS_TOKEN` provides a token, the hook connects without authentication.

## Hook events

Declared in `HOOK.md`:

- `gateway:startup` — initialise NATS connection
- `session:after` — buffer transcript messages
- `command:stop` — flush remaining buffer as final chunk

## Chunk schema

Each published message on `swarm.gateway.session.chunk` is JSON:

```json
{
  "session_key": "whatsapp:+447444361435:kai",
  "chunk_id": "uuid",
  "chunk_index": 0,
  "is_final": false,
  "messages": [{ "role": "user", "content": "hello", "timestamp": "..." }],
  "message_count": 1,
  "session_metadata": {
    "channel": "whatsapp",
    "participant": "+447444361435",
    "agent_id": "kai"
  },
  "flushed_at": "2026-02-14T12:00:00.000Z",
  "flush_reason": "buffer_full"
}
```

Session keys are parsed as `{channel}:{participant}:{agent_id}`.

## Testing

Requires a running NATS server:

```bash
# Start NATS (no auth for local dev)
docker run -d --name nats -p 4222:4222 nats:latest

# Run tests
npm test

# Type-check
npm run typecheck
```
