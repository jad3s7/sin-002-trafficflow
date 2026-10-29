# common — Asynchronous Decoupling (MQ)

## Overview

Part of the [TrafficFlow](../README.md) project. Holds the ActiveMQ broker shared
by the services below — not a service itself, so it has no port of its own. Two
independent integration points share this one broker:

### Topic: `congestion-topic`

Routing Service becomes aware of congestion changes via an ActiveMQ Topic instead of querying Congestion Service directly.

- Producer: `congestion-service` (`../congestion-service`)
- Consumer(s): `routing-service` (`../routing-service`)

Broker URL and topic name are configured in each independent service's local
`MqConfig` (`BROKER_URL`, `CONGESTION_TOPIC`) using `MQ_BROKER_URL` and
`MQ_CONGESTION_TOPIC`. The copies are kept in sync because the Maven modules have
no shared parent project.
Congestion publishes persistent topic messages, and Routing uses a durable
subscription so changes made while it is offline are delivered when it reconnects.
Routing also reads the current congestion level once at startup to seed its cache.

### Queue: `intersection-heartbeat-queue`

Intersection Watchdog notices when Intersection Service goes down by watching for
missed heartbeats / dead-lettered messages instead of polling its `/health` endpoint.

- Producer: `intersection-service` (`../intersection-service`)
- Consumer(s): `intersection-watchdog` (`../intersection-watchdog`)

Broker URL and queue name are shared the same way, via each service's local
`MqConfig` (`BROKER_URL`, `HEARTBEAT_QUEUE`) using `MQ_BROKER_URL` and
`MQ_HEARTBEAT_QUEUE`.

## Project structure

```
common/
├── docker-compose.yml
└── README.md
```

This folder holds the broker config and notes only — the actual publish/subscribe
code belongs in the producer/consumer services listed above (their poms already
depend on `activemq-client`, and each has a local `MqConfig` class with the
constants for the topic/queue that service participates in. The Maven source
path differs for the watchdog copy, but all copies use the same Java package.

## Build

Nothing to build here directly — this folder just brings up the broker used by the
services listed above.

## Run

```
docker compose up -d
```

- Broker URL for clients: `tcp://localhost:61616`
- Web console: http://localhost:8161 (default admin/admin)

Then start the producer/consumer services as usual (`mvn package && java -jar ...`
from their own directories at the project root).

## Test

```
docker compose ps          # confirm the broker container is healthy
```

For an end-to-end smoke check, update congestion and confirm Routing reports the
new level in the `/route` response breakdown. Check `/alert` on Watchdog for the
latest heartbeat and inject a test heartbeat dead letter to confirm it alerts.

## Implemented message flows

- `congestion-service` publishes level changes to `congestion-topic`; failed
  publications are retried until the current level is delivered.
- `routing-service` consumes congestion messages using a durable subscription
  and has an HTTP mode for the required Stage 2 direct-call flow.
- `intersection-service` publishes a non-persistent heartbeat every five seconds
  by default (`MQ_HEARTBEAT_INTERVAL_SECONDS`).
- `intersection-watchdog` detects a missed heartbeat after fifteen seconds by
  default (`MQ_HEARTBEAT_TIMEOUT_SECONDS`) and observes heartbeat-related DLQ
  entries without consuming messages from the broker's dead-letter queue.
