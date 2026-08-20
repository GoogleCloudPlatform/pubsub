# Archetype Validation Prober

A reference implementation of an **archetype validation gate** in front of Cloud Pub/Sub.

It demonstrates a single principle: **only what minimally fits the process should ever be
enqueued.** Everything that is deterministically invalid is rejected *synchronously at the gate* and
never published, so it can never bounce in the queue, never inflate the dead-letter store, and never
burn engine resources being retried.

The prober is provided as-is for demonstration purposes only, with no SLA. It is not meant to be run
as part of a production or critical workload.

## Why

A common failure mode of asynchronous messaging is treating a transport `200`/ack as if it meant the
message was *accepted*. It does not. A payload can be delivered and still be rejected on content, and
a message that never returns a positive functional ack **bounces forever** (or is silently dropped at
retention). At two messages that is noise; at two hundred thousand it is an outage.

The fix is to separate the concerns and validate the deterministic ones **before** enqueuing:

| Outcome | Nature | Where it is caught | Action |
| --- | --- | --- | --- |
| Conformant (functional ACK) | Success | — | Publish / `ack()` |
| Not JSON / wrong type / missing field / bad enum / bad pattern | **Deterministic** | **At the gate (this prober)** | **Reject synchronously, never enqueue** |
| Byte-different but semantically equal (charset / Unicode NFD) | False reject | At the gate (canonicalized) | Normalize to NFC, then accept |
| Downstream down (timeout, 5xx, connection) | Transient | Consumer | `nack()` → retry with backoff → dead-letter |
| State-dependent (unknown id, duplicate, business rule) | Unpredictable | Consumer | Route to quarantine / dead-letter |

The gate handles the deterministic and false-reject rows. The consumer
([`ClassifyingReceiver`](src/main/java/com/google/cloud/pubsub/archetype/ClassifyingReceiver.java))
handles the rest and, crucially, **only lets transient failures ride the redelivery machinery**;
functional rejects are `ack()`-ed and parked in quarantine so they never bounce.

## Components

- [`Archetype`](src/main/java/com/google/cloud/pubsub/archetype/Archetype.java) — the gate. Loads a
  versioned JSON Schema (the *archetype*) and, cheapest check first, canonicalizes (charset + Unicode
  NFC), then validates syntactically and structurally. Returns accept + canonical form, or reject +
  machine-readable reason codes. Never throws on bad input.
- [`ValidationResult`](src/main/java/com/google/cloud/pubsub/archetype/ValidationResult.java) — the
  terminal outcome (`ACCEPTED` with canonical payload, or `REJECTED` with reasons).
- [`ClassifyingReceiver`](src/main/java/com/google/cloud/pubsub/archetype/ClassifyingReceiver.java) —
  a subscriber that classifies each processing failure (accepted / transient / functional reject)
  and acts accordingly, keeping poison messages out of the retry loop.
- [`ArchetypeValidationGateway`](src/main/java/com/google/cloud/pubsub/archetype/ArchetypeValidationGateway.java)
  — the runnable entry point. With no arguments it runs a self-contained, offline demo of the gate
  over representative payloads.
- [`archetype.schema.json`](src/main/resources/archetype.schema.json) — the canonical contract used
  by the demo. Replace it with your own to model your payload.

## Build

These instructions assume [Maven](https://maven.apache.org/) 3 and Java 8.

```
cd archetype-validation-prober
mvn package
```

The resulting jar is at `target/pubsub-archetype-validation-prober.jar`.

## Run (offline demo)

```
java -jar target/pubsub-archetype-validation-prober.jar
```

Expected output (one line per sample): the conformant and the NFD-normalized payloads are
`ACCEPTED (enqueued)`; every deterministic failure is `REJECT` with its reason codes and is never
enqueued.

### Options

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `--archetype` | String | bundled `archetype.schema.json` | Path to the JSON Schema archetype to validate against. |
| `--charset` | String | `UTF-8` | Declared charset of incoming payloads, used for canonicalization. |
| `--help` | flag | — | Print usage and exit. |

## Test

```
mvn test
```

## Smoke test (real Pub/Sub or emulator)

The smoke tests are intentionally off by default. They validate the end-to-end
flow against either a local emulator or a real GCP project.

### Emulator

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
gcloud beta emulators pubsub start
mvn verify -Psmoke
```

### Real GCP

```bash
export GOOGLE_CLOUD_PROJECT=portfolioadvanced-llm
gcloud auth login
gcloud auth application-default login
mvn verify -Psmoke
```

The smoke suite covers:

- valid payload round-trip
- invalid payload never reaches publish
- encoding rejection before JSON parsing

For the higher-level GCP-native companion that demonstrates the same idea in
Python, see [PYTHON_COMPANION.md](PYTHON_COMPANION.md).
