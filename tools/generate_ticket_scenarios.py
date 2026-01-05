import argparse
import json
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from runtime.artifact_store import build_artifact_store
from runtime.incident_flow import _append_shadow

DEFAULT_ARTIFACTS_ROOT = os.environ.get("ARTIFACTS_ROOT") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "artifacts",
)


@dataclass
class ScenarioConfig:
    run_id: str
    seed: int
    n_hosts: int
    days: int
    scenario_tags: List[str]


class ScenarioGenerator:
    def __init__(self, config: ScenarioConfig, artifacts_root: str):
        self.config = config
        self.artifacts_root = artifacts_root
        self.random = random.Random(config.seed)
        self.store = build_artifact_store(artifacts_root)
        self.base_start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def _event(
        self,
        ts: datetime,
        source: str,
        provider: str,
        event_id: int,
        level: str,
        message: str,
        tags: List[str],
    ) -> dict:
        return {
            "ts": ts.isoformat(),
            "source": source,
            "provider": provider,
            "event_id": event_id,
            "level": level,
            "message": message,
            "tags": tags,
            "data": {},
        }

    def _noise_events(self, start: datetime, end: datetime, count: int) -> List[dict]:
        events: List[dict] = []
        for _ in range(count):
            ts = self._random_ts(start, end)
            events.append(
                self._event(
                    ts,
                    source="WindowsEventLog:Application",
                    provider="Application",
                    event_id=1000,
                    level="Information",
                    message="Routine application telemetry",
                    tags=["info"],
                )
            )
        return events

    def _random_ts(self, start: datetime, end: datetime) -> datetime:
        delta = (end - start).total_seconds()
        offset = self.random.uniform(0, delta)
        return start + timedelta(seconds=offset)

    def _build_incident_events(self, host_id: str, start: datetime, end: datetime) -> Tuple[List[dict], List[str], int]:
        templates = {
            "bsod": [
                self._event(
                    self._random_ts(start, end),
                    source="WindowsEventLog:System",
                    provider="BugCheck",
                    event_id=1001,
                    level="Error",
                    message="BugCheck 0x0000007e after driver install",
                    tags=["bsod", "unexpected_shutdown"],
                )
            ],
            "disk_full": [
                self._event(
                    self._random_ts(start, end),
                    source="WindowsEventLog:System",
                    provider="Disk",
                    event_id=2013,
                    level="Warning",
                    message="Disk C: usage at 95% threshold",
                    tags=["disk_full"],
                )
            ],
            "service_crash": [
                self._event(
                    self._random_ts(start, end),
                    source="WindowsEventLog:System",
                    provider="Service Control Manager",
                    event_id=7034,
                    level="Error",
                    message="Service FooSvc terminated unexpectedly",
                    tags=["service_crash"],
                ),
                self._event(
                    self._random_ts(start, end),
                    source="WindowsEventLog:System",
                    provider="Service Control Manager",
                    event_id=7031,
                    level="Error",
                    message="Service FooSvc restarted after unexpected termination",
                    tags=["service_crash"],
                ),
            ],
            "network_instability": [
                self._event(
                    self._random_ts(start, end),
                    source="WindowsEventLog:System",
                    provider="e1cexpress",
                    event_id=10400,
                    level="Warning",
                    message="Network link disconnected unexpectedly",
                    tags=["network_reset"],
                ),
                self._event(
                    self._random_ts(start, end),
                    source="WindowsEventLog:System",
                    provider="DNS Client Events",
                    event_id=1014,
                    level="Warning",
                    message="Name resolution failure for critical service",
                    tags=["dns_failure"],
                ),
            ],
            "update_failure": [
                self._event(
                    self._random_ts(start, end),
                    source="WindowsEventLog:System",
                    provider="WindowsUpdateClient",
                    event_id=20,
                    level="Error",
                    message="Failed to install cumulative update KB999999",
                    tags=["update_failure"],
                )
            ],
        }
        incident_types = list(templates.keys())
        choice = self.random.choice(incident_types)
        severity_hint = {
            "bsod": 90,
            "disk_full": 75,
            "service_crash": 70,
            "network_instability": 65,
            "update_failure": 80,
        }
        return templates[choice], [choice], severity_hint.get(choice, 50)

    def _cluster_bsod_events(self, start: datetime, end: datetime) -> Tuple[List[dict], List[str], int]:
        ts = self._random_ts(start, end)
        events = [
            self._event(
                ts,
                source="WindowsEventLog:System",
                provider="BugCheck",
                event_id=1001,
                level="Error",
                message="BugCheck 0x0000007e after driver install",
                tags=["bsod", "unexpected_shutdown"],
            ),
            self._event(
                ts + timedelta(minutes=2),
                source="WindowsEventLog:System",
                provider="EventLog",
                event_id=6008,
                level="Error",
                message="Previous system shutdown was unexpected",
                tags=["bsod", "unexpected_shutdown"],
            ),
        ]
        return events, ["bsod"], 95

    def _ticket_for(self, host_id: str, incident_type: str, ts: datetime, idx: int) -> dict:
        symptom = {
            "bsod": "PC restarted unexpectedly",
            "disk_full": "Low disk space warnings",
            "service_crash_loop": "App keeps stopping",
            "network_instability": "VPN disconnects every hour",
            "update_failure": "Update keeps failing",
        }.get(incident_type, "User reported instability")
        return {
            "schema_version": "1.0",
            "ticket_id": f"TICKET-{idx:04d}",
            "source": "simulated",
            "created_at": ts.isoformat(),
            "host_id": host_id,
            "user_id": f"user{idx+1}",
            "subject": symptom,
            "body": f"{symptom} (simulated report)",
            "reported_window": None,
            "symptoms": [symptom],
            "truth": {},
        }

    def generate(self) -> Dict[str, object]:
        cfg = self.config
        start = self.base_start
        end = start + timedelta(days=cfg.days)
        host_ids = [f"HOST-{i:03d}" for i in range(1, cfg.n_hosts + 1)]
        scenario_tags = set(cfg.scenario_tags)
        # clean previous run artifacts for deterministic output
        self.store.delete_prefix(cfg.run_id)

        truth_types: set = set()
        expected_top: List[Tuple[str, int]] = []
        tickets: List[dict] = []

        cluster_size = max(2, cfg.n_hosts // 4) if "driver_rollout_wave" in scenario_tags else 2
        cluster_hosts = host_ids[:cluster_size]
        expects_clustered_outage = len(cluster_hosts) > 1

        hardware_host = host_ids[-1] if "single_host_hardware" in scenario_tags else None
        missing_host = host_ids[-2] if "missing_data" in scenario_tags else None

        for idx, host_id in enumerate(host_ids):
            events: List[dict] = self._noise_events(start, end, count=5)
            incident_types: List[str] = []
            severity_hint = 10
            time_skew = "time_skew" in scenario_tags and self.random.random() < 0.3

            if host_id == missing_host:
                events = []
                incident_types = []
                severity_hint = 0
            elif host_id == hardware_host:
                incident_events, incident_types, severity_hint = self._build_incident_events(host_id, start, end)
                incident_types = ["disk_full"]
                incident_events.append(
                    self._event(
                        self._random_ts(start, end),
                        source="WindowsEventLog:System",
                        provider="Disk",
                        event_id=2019,
                        level="Error",
                        message="Disk hardware error detected",
                        tags=["disk_full", "disk_error"],
                    )
                )
                severity_hint = 90
            elif host_id in cluster_hosts:
                incident_events, incident_types, severity_hint = self._cluster_bsod_events(start, end)
            else:
                incident_events, incident_types, severity_hint = self._build_incident_events(host_id, start, end)

            events.extend(incident_events)
            if "slow_burn" in scenario_tags:
                events.append(
                    self._event(
                        self._random_ts(start, end),
                        source="WindowsEventLog:Application",
                        provider="AppHealth",
                        event_id=4001,
                        level="Warning",
                        message="Repeated warning indicating degrading state",
                        tags=["warning"],
                    )
                )
            if "false_positive_noise" in scenario_tags:
                events.append(
                    self._event(
                        self._random_ts(start, end),
                        source="WindowsEventLog:Application",
                        provider="BenignSource",
                        event_id=2001,
                        level="Information",
                        message="Benign telemetry event",
                        tags=["info"],
                    )
                )
            if time_skew:
                # shuffle and add slight skew to timestamps
                self.random.shuffle(events)
                for e in events[:3]:
                    e["ts"] = (self._random_ts(start - timedelta(hours=1), end + timedelta(hours=1))).isoformat()
            truth_types.update(incident_types)
            expected_top.append((host_id, severity_hint))
            events.sort(key=lambda e: e["ts"])
            stats = {
                "event_count": len(events),
                "critical_count": sum(1 for e in events if e["level"] == "Critical"),
                "error_count": sum(1 for e in events if e["level"] == "Error"),
                "warning_count": sum(1 for e in events if e["level"] == "Warning"),
            }
            snapshot = {
                "schema_version": "1.0",
                "snapshot_id": f"{host_id}-{cfg.run_id}",
                "host_id": host_id,
                "user_id": f"user{idx+1}",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "window": {"start": start.isoformat(), "end": end.isoformat()},
                "device": {"hostname": host_id},
                "collector": {"name": "simulator", "method": "simulated"},
                "filters": {"levels": ["Critical", "Error", "Warning", "Information", "Verbose"], "providers_allowlist": []},
                "events": events,
                "stats": stats,
            }
            snapshot_key = f"{cfg.run_id}/snapshots/{host_id}.json"
            self.store.write_text(snapshot_key, json.dumps(snapshot, indent=2, ensure_ascii=True), content_type="application/json")

            ticket_type = incident_types[0] if incident_types else "other"
            ticket = self._ticket_for(host_id, ticket_type, self._random_ts(start, end), idx)
            tickets.append(ticket)
            ticket_key = f"{cfg.run_id}/tickets/{ticket['ticket_id']}.json"
            self.store.write_text(ticket_key, json.dumps(ticket, indent=2, ensure_ascii=True), content_type="application/json")

        expected_top.sort(key=lambda tup: tup[1], reverse=True)
        truth = {
            "run_id": cfg.run_id,
            "expects_incident_types": sorted(list(truth_types)),
            "expects_clustered_outage": expects_clustered_outage,
            "expected_top_hosts": [host for host, _ in expected_top[: min(5, len(expected_top))]],
            "scenario_tags": sorted(list(scenario_tags)),
        }
        truth_key = f"{cfg.run_id}/truth.json"
        manifest_key = f"{cfg.run_id}/run_manifest.json"
        self.store.write_text(truth_key, json.dumps(truth, indent=2, ensure_ascii=True), content_type="application/json")
        manifest = {
            "schema_version": "1.0",
            "run_id": cfg.run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "window": {"start": start.isoformat(), "end": end.isoformat()},
            "generator": {"name": "simulator", "version": "0.1", "seed": cfg.seed},
            "scenario_tags": sorted(list(scenario_tags)),
            "inputs": [],
            "outputs": [
                {"kind": "snapshots", "path": f"{cfg.run_id}/snapshots"},
                {"kind": "tickets", "path": f"{cfg.run_id}/tickets"},
                {"kind": "truth", "path": truth_key},
            ],
        }
        self.store.write_text(manifest_key, json.dumps(manifest, indent=2, ensure_ascii=True), content_type="application/json")
        _append_shadow(self.store, cfg.run_id, "scenario", "Generated synthetic snapshots", hosts=cfg.n_hosts)
        return {"truth": truth, "manifest": manifest, "tickets": tickets}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic synthetic endpoint snapshots and tickets.")
    parser.add_argument("--run-id", required=True, help="Run identifier (artifacts/<run-id>/...)")
    parser.add_argument("--seed", type=int, default=123, help="Deterministic seed.")
    parser.add_argument("--n-hosts", type=int, default=15, help="Number of hosts to simulate.")
    parser.add_argument("--days", type=int, default=1, help="Window size in days.")
    parser.add_argument(
        "--scenario-tags",
        nargs="*",
        default=["driver_rollout_wave", "slow_burn"],
        help="Scenario tags to include (e.g., driver_rollout_wave, slow_burn, single_host_hardware, false_positive_noise).",
    )
    parser.add_argument(
        "--artifacts-root",
        default=DEFAULT_ARTIFACTS_ROOT,
        help="Root for artifacts. Defaults to ARTIFACTS_ROOT or ./artifacts.",
    )
    args = parser.parse_args()
    config = ScenarioConfig(
        run_id=args.run_id, seed=args.seed, n_hosts=args.n_hosts, days=args.days, scenario_tags=args.scenario_tags
    )
    generator = ScenarioGenerator(config=config, artifacts_root=args.artifacts_root)
    generator.generate()


if __name__ == "__main__":
    main()
