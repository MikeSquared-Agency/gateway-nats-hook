---
name: nats-publisher
description: Publishes session transcript chunks to NATS for Chronicle/Dredd pipeline
events:
  - session:after
  - command:stop
  - gateway:startup
requires:
  env:
    - NATS_URL
---
