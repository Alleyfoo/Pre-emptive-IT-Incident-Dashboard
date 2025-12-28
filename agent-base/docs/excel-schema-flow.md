# Excel Schema Flow (Plan-Only)

This doc defines a tool-agnostic agent workflow to open an Excel file, read headers,
generate a schema, and save a cleaned output file. It does not implement a pipeline.

## Inputs
- Source file: local Excel file path (e.g., `data/samples/sample.xlsx`)
- Canonical schema (optional): SchemaSpec JSON for known target fields
- Output format: `xlsx` or `csv`

## Local Sample Policy
- Place local samples under `data/samples/` (gitignored by `.gitignore`).
- Do not commit samples; keep them for tests only.

## Artifact Location (recommended)

Use run-scoped folders so agents pass `run_id` plus filenames:

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

## Artifact Contract (keys, not payloads)
- `evidence_packet.json`: observation output (sheet names, preview rows, row/col counts)
- `header_spec.json`: header candidates and selected normalized headers
- `schema_spec.json`: schema mapping (core or adapter layer)
- `transform_plan.json`: casts/cleanup steps (optional)
- `validation_report.json`: row counts, anomalies (optional)
- `save_manifest.json`: saved output paths
- `shadow_log.jsonl`: decisions and confirmations

## Flow (Plan -> Delegate -> Review -> Merge)
1. Orchestrator gathers:
   - Goal, constraints, acceptance criteria
   - Files-to-touch (source file, output dir, artifact dir)
   - Output format (xlsx/csv)
2. Excel observe step (skill: `excel-observe`):
   - Read and emit `evidence_packet.json`
3. Header agent:
   - Input: preview rows from evidence packet
   - Output: `header_spec.json` with normalized headers
4. Schema agent:
   - If a canonical schema is provided, map headers to it (adapter layer)
   - If not provided, infer core schema from headers/samples
   - Output: `schema_spec.json`
5. Transform agent (optional):
   - Propose casts/cleanup and warnings
   - Output: `transform_plan.json`
6. Validation agent (optional):
   - Report missing required fields, anomaly summary
   - Output: `validation_report.json`
7. Save agent:
   - Select columns and rename based on `schema_spec.json`
   - Write output as xlsx/csv without transforming values
   - Output: `save_manifest.json`
8. Shadow agent:
   - Log decisions, alternatives, confirmations in `shadow_log.jsonl`

## Schema-First Mode (preferred for tests)
- Provide a canonical `schema_spec.json` to the reader step.
- Schema agent produces an adapter mapping only.
- Unmapped columns are recorded, not dropped unless required.

## Stop Conditions (abort and request human input)
- If HeaderAgent confidence < 0.70 -> require human choice among top candidates.
- If SchemaAgent cannot map `product_code` and `quantity` -> request more evidence or a canonical schema.
- If Validation reports missing `product_code` > 5% -> stop.

## Test Strategy (no code yet)
- Create local `data/samples/sample.xlsx` with:
  - Mixed types (text, numbers, dates)
  - One missing header row and one merged header row
  - One extra column not in schema
- Expected artifacts:
  - `header_spec.json` with selected header candidate
  - `schema_spec.json` adapter mapping to known fields
  - `save_manifest.json` pointing to output file
- Acceptance checks:
  - Row counts preserved
  - Required fields present or flagged in validation report
  - Unmapped columns listed in schema output
  - Output file matches requested format (xlsx or csv)
