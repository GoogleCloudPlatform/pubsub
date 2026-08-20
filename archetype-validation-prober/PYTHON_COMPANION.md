# Python Companion for the Archetype Gate

This repository keeps the Java prober as the upstream reference, but the same
idea can be implemented in a more compact and operationally friendly way with a
Python companion.

## What the companion adds

- `Archetype` gate implemented as a total function in Python
- `ValidationResult` and `ValidationError` as immutable value objects
- `ClassifyingReceiver` without a builder or singleton boilerplate
- `vertex_agent/` for ingestion, retrieval, and MCP exposure
- real GCP resources:
  - Pub/Sub topic and subscription
  - BigQuery dataset and table
  - Cloud Storage bucket for docs
- smoke validation with OAuth/ADC only

## Why this is useful

The Java prober proves the gate behavior. The Python companion shows how the
same architecture can be expanded into a full GCP-native workflow:

1. validate deterministically before publish
2. keep the retry path for transient failures only
3. store knowledge in BigQuery
4. expose the result through MCP for coding agents
5. validate everything against real infrastructure

## What was proven in practice

- `uv run pytest` passed
- `uv run python -m scripts.gcp_smoke` returned `smoke=ok`
- Pub/Sub publish/pull round-trip succeeded
- BigQuery insert/query succeeded

## Security note

No service account keys are stored in the repository. Real smoke tests use
developer-owned OAuth/ADC credentials only.

