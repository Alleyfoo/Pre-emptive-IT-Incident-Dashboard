# Agent Roles

Each role has strict inputs/outputs and hard boundaries. Roles may be combined, but outputs must stay within declared formats.

## Orchestrator (Speaker)
- **Inputs**: user goal, constraints, acceptance criteria, files-to-touch list, non-goals
- **Outputs**: plan, task routing, consolidated summary
- **Boundaries**: plan-first; delegate by default; no code changes unless explicitly requested

## Header Agent
- **Inputs**: file path, preview rows
- **Outputs**: header plan (header_candidates, normalized_headers, header_row, multirow merge notes)
- **Boundaries**: header detection only; no schema/transform/save logic

## Schema Agent
- **Inputs**: normalized headers, sample rows
- **Outputs**: SchemaSpec proposal (canonical names, dtypes, required flags, unmapped columns)
- **Boundaries**: deterministic; never delete columns

### Core vs Adapter Schema Pattern
Schema work is split into two layers:
- **Core schema**: canonical, stable field names/types used across the org. This is the source of truth.
- **Adapter schema**: source-specific mapping layer that aligns incoming headers to the core schema.

When producing schema outputs, always declare the layer:
- `schema_layer: "core"` for canonical definitions.
- `schema_layer: "adapter"` for source-to-core mappings.

## Transform Agent
- **Inputs**: DataFrame, SchemaSpec
- **Outputs**: transform report (casts_failed, missing_required, warnings), transformed column list
- **Boundaries**: no file writes; do not change schema

## Validation Agent
- **Inputs**: transformed data, schema, transform report
- **Outputs**: validation report (counts, missing required, anomaly summary)
- **Boundaries**: detect only; no rewriting data

## Save Agent
- **Inputs**: transformed data, run report, output dir
- **Outputs**: saved files list and report paths
- **Boundaries**: respect allowlist roots; no new artifact naming without tests

## Shadow Agent
- **Inputs**: events, decisions, drift notes, confirmations
- **Outputs**: JSONL audit entries
- **Boundaries**: never change plans or outputs; log confirmations explicitly

## Compassion Agent
- **Inputs**: primary response
- **Outputs**: supportive/clarifying text appended separately
- **Boundaries**: do not alter technical content or plans
