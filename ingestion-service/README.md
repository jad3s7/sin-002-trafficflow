# IngestionServiceApp

## Overview

Parses and cleans `intersections-legacy.csv`, a messy legacy export of intersections, districts, and signal types data, and is the
first stop in the TrafficFlow pipeline. Independent Maven module, no parent pom.

Part of the [TrafficFlow](../README.md) project.

## Known data issues

`intersections-legacy.csv` is deliberately messy — cleaning it is the point of this service. Look
out for (and handle) at least:

- **Inconsistent casing** in IDs, names, and status/category values (`Active` /
  `active` / `ACTIVE`)
- **Padding** — leading/trailing spaces, and the occasional double space, inside
  fields
- **Duplicate records** for the same real-world entity, written with a different ID
  casing/format and/or slightly different field values
- **Inconsistent date formats** (`YYYY-MM-DD`, `MM/DD/YYYY`, `DD-MM-YYYY`, one- and
  two-digit months/days) and outright invalid dates
- **Missing / placeholder values** — blank fields, `N/A`, `n/a`, `TBD`, `unknown`,
  `-`, `NaN`
- **Invalid or non-numeric values** in numeric columns (negative counts, spelled-out
  numbers, unrealistic values)
- **Inconsistent boolean/flag representations** (`Y`/`N`, `yes`/`no`, `1`/`0`,
  `true`/`FALSE`)
- **Naming/spelling variants** for the same thing (e.g. regional spelling
  differences, synonyms)

## Worked example

A few raw rows from `intersections-legacy.csv`, and one reasonable cleaned shape for
them. Your field names/casing conventions don't need to match this exactly — the
point is normalizing consistently and handling the duplicate/missing cases, not
hitting this exact JSON.

Raw:

```csv
intersection_id,District ,signal_type,active_flag
INT-1001, Downtown ,4-way,Y
INT-1005,Downtown,Roundabout,true
int-1005,downtown ,ROUNDABOUT,TRUE
INT-1007,Eastside,,1
INT-1015,,4-way,Y
```

Cleaned:

```json
[
  { "id": "INT-1001", "district": "Downtown", "signalType": "4-way",       "active": true },
  { "id": "INT-1005", "district": "Downtown", "signalType": "roundabout",  "active": true },
  { "id": "INT-1007", "district": "Eastside", "signalType": null,          "active": true },
  { "id": "INT-1015", "district": null,       "signalType": "4-way",       "active": true }
]
```

What happened:
- `INT-1001`: trimmed the padded district (`" Downtown "` → `"Downtown"`); flag `Y` → `true`.
- `INT-1005` / `int-1005`: same real-world intersection under two ID casings and two
  signal-type casings — collapsed to a single record.
- `INT-1007`: signal type was blank in the source — kept as an explicit `null` rather
  than dropped or guessed, so downstream services can see it's missing.
- `INT-1015`: same idea for a missing district.

Duplicate matching uses the normalized intersection ID. IDs are uppercased and
common prefix/number separator variants are canonicalized (for example,
`INT1005`, `int_1005`, and `INT-1005` become `INT-1005`). Duplicate rows merge
missing values from later rows into the first row; conflicting populated values
keep the first value and are logged. Rows with no usable ID are kept separately,
because district and signal type alone do not uniquely identify an intersection.

## Project structure

```
ingestion-service/
├── pom.xml
└── src/main/
    ├── java/co/wethinkcode/trafficflow/IngestionServiceApp.java
    └── resources/intersections-legacy.csv
```

## Build

```
mvn package
```

## Run

```
java -jar target/ingestion-service.jar
```

Listens on port `7020`. `GET /health` reports whether the CSV loaded, and
`GET /intersections` returns the cleaned and deduplicated records as JSON.

## Test

No automated tests are currently included. Manually verify the service and data:

```
curl http://localhost:7020/health   # -> OK
curl http://localhost:7020/intersections
```

To add real tests, add JUnit 5 + the Surefire plugin to `pom.xml`, put tests under
`src/test/java/co/wethinkcode/trafficflow/`, and run `mvn test`.
