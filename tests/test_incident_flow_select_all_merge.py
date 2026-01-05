import json
import os
import tempfile

from runtime.incident_flow import run_incident_flow
from runtime.artifact_store import build_artifact_store


def test_select_mode_all_merges_snapshots_per_host():
    with tempfile.TemporaryDirectory() as tmpdir:
        artifacts_root = tmpdir
        run_id = "merge-run"
        snapshot_dir = os.path.join(tmpdir, "snapshots", "HOST-001")
        os.makedirs(snapshot_dir, exist_ok=True)

        snap1 = {
            "schema_version": "1.0",
            "snapshot_id": "HOST-001-1",
            "host_id": "HOST-001",
            "generated_at": "2026-01-01T00:00:00Z",
            "window": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T06:00:00Z"},
            "events": [
                {
                    "ts": "2026-01-01T01:00:00Z",
                    "level": "Error",
                    "source": "WindowsEventLog:System",
                    "provider": "BugCheck",
                    "event_id": 1001,
                    "message": "BugCheck 0x00000124",
                    "data": {},
                    "tags": ["bsod"],
                }
            ],
            "stats": {"event_count": 1, "critical_count": 0, "error_count": 1, "warning_count": 0},
        }

        snap2 = {
            "schema_version": "1.0",
            "snapshot_id": "HOST-001-2",
            "host_id": "HOST-001",
            "generated_at": "2026-01-01T12:00:00Z",
            "window": {"start": "2026-01-01T12:00:00Z", "end": "2026-01-01T18:00:00Z"},
            "events": [
                {
                    "ts": "2026-01-01T13:00:00Z",
                    "level": "Error",
                    "source": "WindowsEventLog:System",
                    "provider": "Disk",
                    "event_id": 7,
                    "message": "Disk failure imminent",
                    "data": {},
                    "tags": ["disk_full"],
                }
            ],
            "stats": {"event_count": 1, "critical_count": 0, "error_count": 1, "warning_count": 0},
        }

        with open(os.path.join(snapshot_dir, "snapshot-20260101T060000Z.json"), "w", encoding="utf-8") as handle:
            json.dump(snap1, handle)
        with open(os.path.join(snapshot_dir, "snapshot-20260101T180000Z.json"), "w", encoding="utf-8") as handle:
            json.dump(snap2, handle)

        run_incident_flow(
            run_id=run_id,
            artifacts_root=artifacts_root,
            snapshot_root=artifacts_root,
            snapshot_prefix="",
            ticket_prefix=None,
            retention_hours=1,
            window_hours=24 * 365,
            select_mode="all",
            max_hosts=None,
        )

        store = build_artifact_store(artifacts_root)
        timeline = json.loads(store.read_text(f"{run_id}/hosts/HOST-001/timeline.json"))

        assert timeline["window"]["start"] == "2026-01-01T00:00:00Z"
        assert timeline["window"]["end"] == "2026-01-01T18:00:00Z"
        assert len(timeline["events"]) == 2
        assert len(timeline["incidents"]) >= 2

        report = store.read_text(f"{run_id}/hosts/HOST-001/report.md")
        assert "2026-01-01T00:00:00Z" in report
        assert "Disk usage approaching capacity" in report or "disk_full" in report
