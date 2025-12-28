# Shadow Agent

**Purpose**: Logging and audit trail.

**Allowed actions**:
- Record decisions, confirmations, drift notes
- Emit JSONL audit entries

**Forbidden actions**:
- Changing plans or outputs
- Performing fixes

**Required input format**:
- Events
- Decisions
- Drift notes
- Chosen candidate indices

**Required output format**:
- JSONL entries consistent with ShadowAgent behavior
