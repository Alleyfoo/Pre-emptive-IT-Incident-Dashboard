# Pre-emptive IT Incident Dashboard

A small system that watches a fleet of computers, spots problems before they turn into outages, and shows what's happening on a single dashboard.

## The idea

When a company has lots of computers, things break: a disk fills up, a service crashes, a login fails too many times. Often the warning signs are visible *before* anything actually goes wrong — but only if someone is looking. This project does the looking automatically.

It works in three steps:

1. **Collect** — each computer regularly produces a "snapshot" (a small JSON file describing its current state: errors, services, recent events).
2. **Detect** — a worker reads those snapshots and applies a set of rules to flag anything that looks like a real or developing incident.
3. **Show** — a web dashboard summarises the whole fleet, and lets you drill into a single computer to see its timeline and the evidence behind each alert.

Every run also writes its results to disk as files (called *artifacts*), so you can re-open any past run and see exactly what was detected and why.

## What's in the repo

- `runtime/` — the worker that turns snapshots into incidents.
- `streamlit_app.py` — the dashboard UI (three themes: **Broadsheet**, **Meadow**, **Workshop**).
- `broadsheet.css` / `kevat.css` / `streamlit-overrides.css` — styling for the themes.
- `tools/` — helpers to generate fake test data and validate output.
- `collector/snapshot.ps1` — a small Windows script that produces a real snapshot from event logs.
- `docs/` — deployment guides.

## Run it on your laptop

You need Python 3.11 or newer.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Generate some fake data, run the worker on it, and check the output is valid:

```powershell
python -m tools.generate_ticket_scenarios --run-id demo
python -m runtime.incident_flow --run-id demo
python -m tools.validate --run-id demo
```

Open the dashboard:

```powershell
streamlit run streamlit_app.py
```

The dashboard ships with three interchangeable themes selectable from the sidebar:

- **Broadsheet** — editorial, no metaphor. KPI tiles, sparklines, ranked clusters, recent activity feed. The default.
- **Meadow** — clusters rendered as plants in a meadow; severity grows the bloom.
- **Workshop** — clusters rendered as machines in a workshop; severity tilts the monitor.

Results land in `artifacts/<run-id>/` — a fleet summary, per-host timelines and reports, and a status file that tells the dashboard which run to display.

## Run it with Docker

If you don't want to install Python locally, one command starts the whole thing:

```bash
docker compose up --build
```

The same image is used for the dashboard and the worker; the worker just gets a different command. Artifacts can be written to a local folder or to cloud storage by setting `ARTIFACTS_ROOT=gs://...`.

## Run it on Google Cloud

The system is designed to run on Google Cloud Run with two pieces:

- **Dashboard** — a Cloud Run *service* (always-on web app), reading artifacts from a Cloud Storage bucket. Locked down to IAM-authenticated users.
- **Worker** — a Cloud Run *job* (runs on demand), triggered on a schedule by Cloud Scheduler. It writes a `latest_run.txt` pointer only on success, and cleans up old runs while preserving any that have been pinned.

Two guides walk through it:

- `docs/DEPLOY_CLOUD_RUN.md` — minimal setup to get it running.
- `docs/DEPLOY_PRODUCTION.md` — production concerns: IAM, bucket lifecycle, scheduler config.

## Where the data comes from

Three options, depending on what you have:

- **Synthetic** — `tools/generate_ticket_scenarios.py` makes fake snapshots for development and demos.
- **Real snapshots** — drop schema-compliant files into `snapshots/<host_id>/snapshot-<ts>.json` and run the worker in snapshot mode.
- **Reference collector** — `collector/snapshot.ps1` builds a snapshot from Windows event logs as a starting point for a real agent. Run it on the target machine:

  ```powershell
  .\collector\snapshot.ps1 -HostId HOST-123 -OutputPath .\snapshots\HOST-123\snapshot-$((Get-Date).ToString('yyyyMMddHHmm')).json -HoursBack 24
  ```

  It reads the System and Application event logs for the last `-HoursBack` hours, redacts emails / file paths / IPs, and writes a schema-compliant `snapshot.json`. Schedule it via Task Scheduler to drop files into a folder that's uploaded to your artifacts root.

## Operational defaults

A few things worth knowing if you run this for real:

- **Schema validation** on every run, so malformed snapshots are rejected loudly rather than silently.
- **Redaction modes** (`REDACTION_MODE=strict|balanced|off`) and evidence truncation, so sensitive data doesn't leak into artifacts.
- **Run locking** (in GCS or locally) prevents two workers from clobbering each other.
- **Retention purge** removes old runs but respects pinned ones.
- **Run status + latest-run pointer** let the dashboard auto-discover the most recent successful run without any manual config.
