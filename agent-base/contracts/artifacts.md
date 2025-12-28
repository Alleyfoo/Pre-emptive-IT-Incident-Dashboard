# Artifact Contract

## Core Concepts

- **run_id**: unique identifier for a single pipeline run (e.g., UUID or timestamp + slug).
- **artifact_key**: stable identifier for an artifact, preferred format:
  - `artifacts/{run_id}/{artifact_name}@{content_hash}`
  - or `artifacts/{run_id}/{artifact_name}/{content_hash}`
- **Immutability**: new facts must produce a new artifact key. Never overwrite an existing key with different content.

## Default Location (recommended)

Use a run-scoped folder and stable filenames:

```
artifacts/<run_id>/
  evidence_packet.json
  header_spec.json
  schema_spec.json
  transform_plan.json
  validation_report.json
  save_manifest.json
  shadow.jsonl
  output/
    clean.csv  (or clean.xlsx / clean.parquet)
```

## Minimal Required Artifacts

- `evidence_packet.json`
- `header_spec.json`
- `schema_spec.json`
- `transform_plan.json`
- `validation_report.json`
- `save_manifest.json`
- `shadow.jsonl`

## Required Metadata

Each artifact must include or be associated with:
- `run_id`
- `artifact_key`
- `created_at`
- `producer_role`

## Key Rules

- Agents exchange **keys, not payloads**.
- If evidence changes, generate a new key.
- Keys must be referenced in reports and logs for traceability.
