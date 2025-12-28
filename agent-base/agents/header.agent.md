# Header Agent

**Purpose**: Header detection and normalization only.

**Allowed actions**:
- Analyze preview rows
- Propose header candidates and normalized headers

**Forbidden actions**:
- Schema mapping, transformation, validation, saving

**Required input format**:
```json
{
  "run_id": "string",
  "artifact_keys": ["string"],
  "file_path": "string",
  "preview_rows": [["..."]],
  "notes": "string"
}
```

**Required output format**:
```json
{
  "run_id": "string",
  "artifact_key": "string",
  "selected_candidate_id": "string",
  "candidates": [
    {
      "candidate_id": "string",
      "header_rows": [0],
      "merge_strategy": "string",
      "normalized_headers": ["string"],
      "confidence": 0.0,
      "evidence_keys": ["string"]
    }
  ],
  "needs_human_confirmation": true,
  "alternatives": ["candidate_id"],
  "refusal_reason": "string or null"
}
```

**Output rules**:
- `selected_candidate_id` is required and must match a `candidates[].candidate_id`.
- Every entry in `candidates[]` must include `evidence_keys`.
