# TrafficFlow demo guide

**Run time:** about 7 minutes (allow 5–10 minutes with questions)

## Demo goal

Show how TrafficFlow cleans a legacy intersection file, exposes a canonical
intersection directory, estimates routes, distributes congestion changes through
ActiveMQ, and reports failures through a watchdog.

The project README describes the current service flows; this guide follows the
implemented endpoints and message paths.

## Before presenting

Requirements: Java 17+, Maven 3.8+, Docker, and `curl`. Build all five independent
Maven modules from the project root:

```bash
for module in ingestion-service intersection-service congestion-service routing-service intersection-watchdog; do
  (cd "$module" && mvn -q package) || exit 1
done
```

Start the ActiveMQ broker:

```bash
cd common && docker compose up -d
```

In separate terminals, start these services from the project root:

```bash
cd ingestion-service && java -jar target/ingestion-service.jar
```

```bash
cd intersection-service && java -jar target/intersection-service.jar
```

```bash
cd congestion-service && java -jar target/congestion-service.jar
```

```bash
cd routing-service && java -jar target/routing-service.jar
```

```bash
cd intersection-watchdog && java -jar target/intersection-watchdog.jar
```

Start the broker first, then the services. Intersection service loads data from
ingestion at startup, so keep ingestion running. Watchdog heartbeat defaults are
5 seconds between beats and a 15 second timeout.

## Presenter flow

### 1. Set the scene (about 45 seconds)

TrafficFlow is a small traffic signal and routing backend built as independent
services. The demo follows one record from a deliberately messy CSV into a route
estimate. As city congestion changes, Routing receives updates asynchronously;
the watchdog observes the intersection service's heartbeat.

### 2. Show ingestion and data cleanup (about 1 minute)

```bash
curl -s http://localhost:7020/health
curl -s http://localhost:7020/intersections | python3 -m json.tool | head -80
```

Point out that IDs and signal types are normalized, whitespace is collapsed,
boolean variants become booleans, duplicate `INT-1005` rows collapse, and missing
or placeholder values are represented explicitly. For example, `INT-1007` has a
missing signal type. The ingestion endpoint is also the source used by
Intersection Service.

### 3. Show the canonical directory and validation (about 1 minute)

```bash
curl -s http://localhost:7021/health
curl -s http://localhost:7021/intersections/INT-1001 | python3 -m json.tool
curl -i http://localhost:7021/intersections/INT-9999
curl -s http://localhost:7021/districts | python3 -m json.tool
```

Explain that a known ID returns its normalized record and an unknown ID gets a
404. This service is the source of truth Routing calls before it estimates a
journey.

### 4. Estimate a route and demonstrate validation (about 1 minute)

```bash
curl -s 'http://localhost:7023/route?from=INT-1001&to=INT-1005' | python3 -m json.tool
curl -i 'http://localhost:7023/route?from=INT-1001&to=INT-9999'
curl -i 'http://localhost:7023/route?from=INT-1001&to=INT-1003'
```

The successful response breaks down base travel time, signal delays, and the
congestion multiplier. The second call shows the unknown-intersection path;
`INT-1003` is a distinct, inactive intersection, so the final call is rejected
with a conflict response. Routing obtains intersection records over HTTP and
computes a route estimate from those records and its current congestion
snapshot.

### 5. Change congestion and watch the topic update routing (about 1½ minutes)

First check the current level and route estimate:

```bash
curl -s http://localhost:7022/congestion | python3 -m json.tool
curl -s 'http://localhost:7023/route?from=INT-1001&to=INT-1005' | python3 -m json.tool
```

Set congestion to the maximum level (0–8):

```bash
curl -s -X PUT http://localhost:7022/congestion \
  -H 'Content-Type: application/json' \
  -d '{"level":8}' | python3 -m json.tool
sleep 1
curl -s 'http://localhost:7023/route?from=INT-1001&to=INT-1005' | python3 -m json.tool
```

Compare the estimate and `breakdown` values. Congestion Service publishes a
changed level to the `congestion-topic`; Routing subscribes and uses the latest
message rather than polling Congestion Service. The brief pause gives the topic
consumer time to receive the update. For a quick validation, try an out-of-range
level such as `9`; the update endpoint returns HTTP 400.

### 6. Inspect watchdog status and raise an alert (about 1½ minutes)

```bash
curl -s http://localhost:7021/heartbeat | python3 -m json.tool
curl -s http://localhost:7024/alert | python3 -m json.tool
curl -s http://localhost:7024/health
```

The intersection service sends periodic messages to
`intersection-heartbeat-queue`; Watchdog reports the last beat and current
health. For a visible alert without stopping a service, inject a test dead-letter
message:

```bash
curl -s -X POST http://localhost:7024/inject-dlq | python3 -m json.tool
sleep 1
curl -i http://localhost:7024/health
curl -s http://localhost:7024/alert | python3 -m json.tool
```

The health endpoint becomes HTTP 503 and `/alert` reports the DLQ count/reason.
This is a debug endpoint for exercising the DLQ watcher. A heartbeat timeout can
also be demonstrated by stopping Intersection Service and waiting over 15
seconds; restart it to restore the heartbeat stream.

### 7. Close (about 30 seconds)

Recap the path: CSV cleanup → intersection lookup → route estimate → asynchronous
congestion update → heartbeat/DLQ monitoring. The design keeps each service
independently buildable, while HTTP handles request/response validation and
ActiveMQ carries updates and health signals.

## Quick troubleshooting

- `503` from Intersection Service: confirm ingestion is up, then run
  `curl -X POST http://localhost:7021/refresh`.
- Route returns `502`: check that Intersection and Congestion services are
  running; confirm Routing can reach port 7021.
- Congestion update says publish failed, or watchdog cannot connect: check
  `docker compose ps` from `common/` and ensure the broker listens on port 61616.
- Route still shows the previous congestion level: wait a second and request it
  again; routing consumes the topic asynchronously.

To stop the broker after the demo: `cd common && docker compose down`.
