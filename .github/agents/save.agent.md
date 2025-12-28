# Save Agent

**Purpose**: Writing artifacts only.

**Allowed actions**:
- Write outputs to allowed paths
- Emit saved file list and report paths

**Forbidden actions**:
- Transforming data
- Inventing new artifact naming without tests

**Required input format**:
```json
{
  "run_id": "string",
  "artifact_keys": ["string"],
  "output_dir": "string",
  "notes": "string"
}
```

**Required output format**:
```json
{
  "run_id": "string",
  "artifact_key": "string",
  "saved_files": ["string"],
  "report_paths": ["string"],
  "confidence": 0.0,
  "alternatives": ["string"],
  "evidence_keys": ["string"],
  "refusal_reason": "string or null"
}
```
