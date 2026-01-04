# Data Agents Demo

Agentic Excel/CSV cleaner: finds the right header row, normalizes fields, and writes a clean schema plus CSV output. Ships with a CLI, a TUI, and Streamlit UIs. GitHub home: https://github.com/Alleyfoo/Data-agents-demo

## Quickstart (CLI)

Prereqs: Python 3.11+, git, and pip. Commands assume PowerShell from the repo root.

```powershell
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
python -m pip install -r demos/requirements-demo.txt

# Try the included sample
python data_agents_cli.py run --input data/samples/sample_mini.csv --run-id demo --interactive

# Or point to your own file (prompts if header confirmation is needed)
python data_agents_cli.py run --input path\\to\\your.xlsx --run-id demo --interactive

# Or the two-step flow
python data_agents_cli.py run --input path\\to\\your.xlsx --run-id demo
python data_agents_cli.py confirm --run-id demo --choice row_1
python data_agents_cli.py resume --run-id demo
```

Outputs land in `artifacts/<run-id>/`, including `clean.csv`, `schema_spec.json`, and the shadow log. Use your own CSV/XLSX; drop it anywhere and point `--input` to the file. A small sample like `data/samples/sample_messy.xlsx` works if you already have it locally.

## UI options

- TUI: `python demos/tui_app.py --input path\\to\\your.xlsx --interactive`
- Streamlit demo: `streamlit run demos/streamlit_app.py`
- Streamlit mapping studio: `streamlit run demos/streamlit_mapping_studio.py`
- Convenience launchers (Windows): `demos/run_tui_demo.bat`, `demos/run_streamlit_demo.bat`

## Local vs Cloud

- Local dev: leave `ARTIFACTS_ROOT` unset to default to `./artifacts`; install with `pip install -r demos/requirements-demo.txt`; run `streamlit run demos/streamlit_app.py`.
- Cloud Storage: set `ARTIFACTS_ROOT=gs://your-bucket/artifacts` and run the CLI or Streamlit apps; artifacts are written via the storage abstraction instead of container paths.
- Cloud Run: build with the provided `Dockerfile` and deploy with `gcloud run deploy ... --set-env-vars ARTIFACTS_ROOT=gs://your-bucket/artifacts`; the app binds to `$PORT` on `0.0.0.0`.
- Inputs are copied into the artifact store, so resume checks don’t depend on container-local paths.

## What this demo does

- Detects likely header rows and asks for confirmation when ambiguous.
- Cleans and normalizes column names and data values.
- Writes reproducible artifacts (schema, evidence packet, shadow log, clean CSV) under `artifacts/`.
- Designed to plug in alternative UIs without changing the core runtime in `runtime/`.

## How it works (fast walk-through)

```
Your CSV/XLSX
   │
   ▼
runtime.excel_flow.puhemies_run_from_file
   ├─ Detect header candidates → evidence_packet.json
   ├─ If ambiguous → header_spec.json → CLI/TUI/Streamlit asks you
   ├─ You confirm → human_confirmation.json
   ├─ Orchestrator resumes → cleans/normalizes data
   └─ Writes outputs:
        • clean.csv
        • schema_spec.json (normalized headers/field types)
        • shadow.jsonl (trace log)
```

Why this is more than “just read Excel”:
- The pipeline treats header detection as a first-class decision, not a guess hidden inside a parser.
- Human confirmations are recorded, so runs are reproducible and auditable.
- UIs are thin shells; the orchestration and janitor live in `runtime/`, so you can swap interfaces without touching the core.
- Artifacts are structured (JSON + CSV) for downstream pipelines, not screenshots or ad-hoc prints.

## Under the hood (where to look)

- Orchestration: `runtime/excel_flow.py` — `puhemies_run_from_file` (initial detection), `puhemies_continue` (after confirmation), `_write_json` and `_append_shadow` (artifact + audit log writers).
- Header detection: `_normalize_header`, `_header_looks_like_data`, and candidate generation inside `excel_flow.py`; scores candidates, marks ambiguous cases, and serializes to `header_spec.json`.
- Human-in-the-loop: `data_agents_cli.py` (CLI), `demos/tui_app.py` (text UI), `demos/streamlit_app.py` & `demos/streamlit_mapping_studio.py` (web UI). All simply surface the same `header_spec.json` question and call `write_human_confirmation`.
- Data cleaning: `runtime/data_janitor.py` — `clean_value`, `clean_series` for stripping whitespace, normalizing numeric-ish strings, and handling nulls before writing `clean.csv`.
- Schema + evidence: `schema_spec.json` and `evidence_packet.json` come from the run; they capture normalized headers, confidence scores, and decisions so you can replay/debug.

### UI → runtime → artifacts (code map)

```
CLI/TUI/Streamlit
   │ calls
   ▼
runtime.excel_flow.puhemies_run_from_file
   ├─ _build_header_candidates → header_spec.json
   ├─ _append_shadow           → shadow.jsonl (event log)
   ├─ write_human_confirmation → human_confirmation.json (after you pick a header)
   ├─ puhemies_continue        → re-loads confirmation + cleans data
   ├─ _infer_dtype/clean_series → schema_spec.json, clean.csv
   └─ _write_json              → evidence_packet.json, save_manifest.json
```

## Agent philosophy (why it works this way)

- Decisions are explicit: the orchestrator asks for human confirmation when header confidence is low, then records that choice in artifacts so runs are reproducible.
- Separation of concerns: runtime logic lives in `runtime/` (detection, cleaning, orchestration) while UIs (CLI/TUI/Streamlit) are thin shells that just prompt and display.
- Traceability by default: every step writes to `shadow.jsonl` and structured specs so you can audit or replay without hidden state.
- Extensible roles: canonical agent/skill definitions live under `agent-base/` (mirrored in `.github/`) if you want to plug this into a larger multi-agent workflow.

## Repo layout

- `data_agents_cli.py` / `data-agents.ps1` — CLI entrypoint and PowerShell wrapper.
- `runtime/` — header detection, janitor, and orchestration logic.
- `demos/` — TUI and Streamlit front-ends plus demo requirements.
- `tests/` — basic flow coverage.
- `agent-base/` and `.github/` — shared agent definitions and templates.

## Example output

- Clean CSV from the bundled sample: `docs/example-output/sample_mini_clean.csv`

## Links

- Profile: https://github.com/Alleyfoo
- Related: https://github.com/Alleyfoo/Data-tool-demo
