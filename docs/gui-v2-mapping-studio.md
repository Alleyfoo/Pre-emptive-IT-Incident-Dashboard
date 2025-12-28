# Mapping Studio (V2)

This Streamlit UI is a thin skin over the Puhemies workflow. It reads and writes artifacts under
`artifacts/<run_id>/` and never replaces the orchestration logic.

## Goals
- Resolve hard header/table cases with replayable artifacts.
- Produce adapter mapping artifacts that the runner can consume.
- Keep Puhemies as the only user-facing speaker.

## Run the app

```bash
python -m pip install -r demos/requirements-demo.txt
streamlit run demos/streamlit_mapping_studio.py
```

## Workflow

1) Start a run from the Runs Dashboard
- Upload an Excel/CSV file and click Run.
- The app calls Puhemies once and writes `evidence_packet.json` and `header_spec.json`.

2) Evidence Preview
- Inspect preview rows from `evidence_packet.json`.
- Confirm file path and sheet name (if available).

3) Header Picker
- Pick the correct header candidate.
- The app writes `human_confirmation.json` and resumes automatically.

4) Table Region (optional)
- Define `start_row`, `end_row`, and include/exclude columns.
- Saved as `table_region.json`.

5) Column Mapping (adapter schema)
- Map canonical fields to detected columns.
- Save `adapter_schema_spec.json` (schema_layer = adapter).
- Resume to generate output using the adapter mapping.

6) Validation Preview (lightweight)
- Displays missing required percentage based on the adapter mapping.
- Optional, but can be saved as `validation_report.json` if needed.

7) Output
- Shows output CSV path and preview.
- Provides a download button.

## Artifact formats

### adapter_schema_spec.json
- run_id
- schema_layer: "adapter"
- canonical_fields: list
- field_map: {canonical_field: source_column}
- types: {canonical_field: "string"|"number"|"date"}
- required_fields: list
- evidence_keys: list

### table_region.json
- sheet_name
- start_row
- end_row
- include_columns
- exclude_columns

## Notes
- All paths are repo-relative where possible.
- Manual choices are stored as artifacts and can be replayed.
- This UI does not add new orchestration logic. Puhemies still drives the flow.
