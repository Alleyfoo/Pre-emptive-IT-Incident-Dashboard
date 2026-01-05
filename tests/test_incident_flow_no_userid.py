import json
import os
import tempfile

from runtime.incident_flow import run_incident_flow
from runtime.schema_validate import validate_or_raise
from runtime.artifact_store import build_artifact_store


def test_run_without_user_id_passes_schema_validation():
    with tempfile.TemporaryDirectory() as tmpdir:
        artifacts_root = tmpdir
        run_id = "test-run"
        snapshot_dir = os.path.join(tmpdir, "snapshots", "HOST-001")
        os.makedirs(snapshot_dir, exist_ok=True)

        snapshot = {
            "schema_version": "1.0",
            "snapshot_id": "HOST-001-1",
            "host_id": "HOST-001",
            "generated_at": "2026-01-01T00:00:00Z",
            "window": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T12:00:00Z"},
            "events": [
                {
                    "ts": "2026-01-01T01:00:00Z",
                    "level": "Error",
                    "source": "WindowsEventLog:System",
                    "provider": "TestProvider",
                    "event_id": 1,
                    "message": "Test error",
                    "data": {},
                    "tags": ["bsod"],
                }
            ],
            "stats": {"event_count": 1, "critical_count": 0, "error_count": 1, "warning_count": 0},
        }

        snap_path = os.path.join(snapshot_dir, "snapshot-20260101T120000Z.json")
        with open(snap_path, "w", encoding="utf-8") as handle:
            json.dump(snapshot, handle)

        run_incident_flow(
            run_id=run_id,
            artifacts_root=artifacts_root,
            snapshot_root=artifacts_root,
            snapshot_prefix="",
            ticket_prefix=None,
            retention_hours=1,
            window_hours=24 * 365,
            select_mode="latest",
            max_hosts=None,
        )

        store = build_artifact_store(artifacts_root)
        validate_or_raise(store, run_id)

        fleet = json.loads(store.read_text(f"{run_id}/fleet_summary.json"))
        top_hosts = fleet.get("top_hosts", [])
        if top_hosts:
            assert "user_id" not in top_hosts[0]

        timeline = json.loads(store.read_text(f"{run_id}/hosts/HOST-001/timeline.json"))
        assert timeline.get("user_id") is None

        report = store.read_text(f"{run_id}/hosts/HOST-001/report.md")
        assert "2026-01-01T00:00:00Z" in report
        assert "2026-01-01T12:00:00Z" in report
