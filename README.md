# Pre-emptive IT Incident Dashboard
Ops/Observability automation: endpoint-snapshot → incident-detect → priorising → raporting.
Artifact-first incident pipeline: ingest endpoint snapshots, detect incidents with deterministic rules, write reproducible artifacts, and surface a Streamlit dashboard (fleet + host). Runs locally or on GCP (Cloud Run service + job) with GCS-backed storage.

## Quickstart (local)

Prereqs: Python 3.11+, git, pip.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r demos/requirements-demo.txt

# Generate synthetic data, run worker, validate
python -m tools.generate_ticket_scenarios --run-id demo
python -m runtime.incident_flow --run-id demo
python -m tools.validate --run-id demo

# Dashboard
streamlit run demos/streamlit_incident_dashboard.py
```

Artifacts land in `artifacts/<run-id>/` (fleet_summary.json, host timelines/reports, run_status.json, latest_run.txt).

## Docker / Compose

Single image runs the dashboard by default. One-command local stack:

```bash
docker compose up --build
```

Worker uses the same image (override command/args) to generate scenarios, run incident flow, and validate. Bind `/artifacts` volume or set `ARTIFACTS_ROOT=gs://...`.

## Cloud Run (service + job)

- Dashboard (Cloud Run service, IAM-only): `ARTIFACTS_ROOT=gs://<bucket>/artifacts`.
- Worker (Cloud Run job): `ARTIFACTS_ROOT=gs://<bucket>/artifacts`, optional `--snapshot-root gs://<bucket>/snapshots` for real snapshots; writes latest_run.txt only on success, with run_status.json and retention purge.
- Scheduler: Cloud Scheduler trigger to execute the job on cadence.

Docs: `docs/DEPLOY_CLOUD_RUN.md` (quick) and `docs/DEPLOY_PRODUCTION.md` (IAM, lifecycle, scheduler).

## Ingest paths

- Synthetic: `tools/generate_ticket_scenarios.py`.
- Real snapshots: upload schema-compliant `snapshots/<host_id>/snapshot-<ts>.json`; run worker in snapshot mode.
- Reference collector: `collector/snapshot.ps1` (Windows event logs → snapshot.json).

## Security/ops defaults

- Schema validation on every run; redaction modes (`REDACTION_MODE=strict|balanced|off`) and evidence truncation.
- Run locking (GCS/local) to avoid overlap; retention purge respects pinned runs.
- Run status artifacts + latest run pointer for dashboard autodiscovery.
