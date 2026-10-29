# RoutingServiceApp

## Overview

Provides estimated travel times based on congestion and intersection.

Part of the [TrafficFlow](../README.md) project. Independent Maven module, no
parent pom.

By default, this service subscribes to the ActiveMQ topic `congestion-topic` —
see [`../common/`](../common). Broker URL and topic name come from this module's
`co.wethinkcode.trafficflow.MqConfig`.

To run the required Stage 2 synchronous REST integration by itself, set
`TRAFFICFLOW_CONGESTION_SOURCE=http` when starting this service. In that mode,
each route request reads the current congestion from
`http://localhost:7022/congestion`. The default `mq` mode uses the Stage 3 topic
subscriber instead.

## Project structure

```
routing-service/
├── pom.xml
└── src/main/java/co/wethinkcode/trafficflow/
    ├── RoutingServiceApp.java
    └── mq/
        └── MqConfig.java
```

## Build

```
mvn package
```

## Run

```
java -jar target/routing-service.jar
```

For Stage 2 HTTP mode:

```
TRAFFICFLOW_CONGESTION_SOURCE=http java -jar target/routing-service.jar
```

Listens on port `7023`.

## Test

No automated tests yet. Manually verify it's up:

```
curl http://localhost:7023/health   # -> OK
```

To add real tests, add JUnit 5 + the Surefire plugin to `pom.xml`, put tests under
`src/test/java/co/wethinkcode/trafficflow/`, and run `mvn test`.
