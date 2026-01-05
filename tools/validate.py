import argparse
import json
import os
from typing import Dict, List, Set

from runtime import schema_validate
from runtime.artifact_store import build_artifact_store
from runtime.incident_flow import _append_shadow

DEFAULT_ARTIFACTS_ROOT = os.environ.get("ARTIFACTS_ROOT") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "artifacts",
)


def _load_json(store, key: str) -> dict:
    return json.loads(store.read_text(key))


def _truth_key(run_id: str) -> str:
    return f"{run_id}/truth.json"


def _fleet_summary_key(run_id: str) -> str:
    return f"{run_id}/fleet_summary.json"


def _collect_detected_types(store, run_id: str) -> Set[str]:
    detected: Set[str] = set()
    for key in store.list(f"{run_id}/hosts"):
        if not key.endswith("timeline.json"):
            continue
        payload = _load_json(store, key)
        for inc in payload.get("incidents", []):
            if inc.get("type"):
                detected.add(inc["type"])
    return detected


def _ranking_hits(top_hosts: List[dict], expected: List[str]) -> float:
    if not expected:
        return 1.0
    observed = [h["host_id"] for h in top_hosts][: len(expected)]
    hits = sum(1 for host in expected if host in observed)
    return hits / len(expected)


def _cluster_hit(clusters: List[dict]) -> bool:
    return any(cluster.get("affected_hosts", 0) >= 2 for cluster in clusters)


def _precision_recall(truth: Set[str], detected: Set[str]) -> Dict[str, float]:
    if not truth:
        return {"precision": 1.0, "recall": 1.0}
    tp = len(truth & detected)
    precision = tp / max(1, len(detected))
    recall = tp / max(1, len(truth))
    return {"precision": precision, "recall": recall}


def validate(run_id: str, artifacts_root: str, strict_scenario: bool = False) -> Dict[str, object]:
    store = build_artifact_store(artifacts_root)
    schema_errors = schema_validate.validate_run(store, run_id)

    truth = _load_json(store, _truth_key(run_id))
    fleet = _load_json(store, _fleet_summary_key(run_id))
    detected_types = _collect_detected_types(store, run_id)

    expected_types = set(truth.get("expects_incident_types", []))
    pr = _precision_recall(expected_types, detected_types)
    ranking_score = _ranking_hits(fleet.get("top_hosts", []), truth.get("expected_top_hosts", []))
    cluster_detected = _cluster_hit(fleet.get("clusters", []))
    scenario_tags = set(truth.get("scenario_tags", []))
    scenario_checks: List[str] = []
    if "driver_rollout_wave" in scenario_tags:
        if not any(c.get("affected_hosts", 0) >= 2 for c in fleet.get("clusters", [])):
            scenario_checks.append("expected clustered outage but none detected")
    if "missing_data" in scenario_tags:
        if fleet.get("host_count", 0) == 0:
            scenario_checks.append("missing_data scenario resulted in zero hosts (unexpected)")
    if "time_skew" in scenario_tags:
        # ensure pipeline didn't drop events: host_count should match snapshots count
        snapshot_hosts = {key.split("/")[-1].replace(".json", "") for key in store.list(f"{run_id}/snapshots") if key.endswith(".json")}
        if fleet.get("host_count", 0) != len(snapshot_hosts):
            scenario_checks.append("time_skew scenario host count mismatch")

    result = {
        "run_id": run_id,
        "incident_type_precision": pr["precision"],
        "incident_type_recall": pr["recall"],
        "ranking_score": ranking_score,
        "cluster_detected": cluster_detected,
        "schema_errors": schema_errors,
        "scenario_warnings": scenario_checks,
    }
    report = _render_report(truth, fleet, result)
    report_key = f"{run_id}/validation_report.md"
    store.write_text(report_key, report, content_type="text/markdown")
    summary_key = f"{run_id}/validation_summary.json"
    store.write_text(summary_key, json.dumps(result, indent=2, ensure_ascii=True), content_type="application/json")
    _append_shadow(store, run_id, "validation", "Validation complete", result=result)
    if schema_errors:
        raise SystemExit(f"Schema validation failed: {'; '.join(schema_errors)}")
    if strict_scenario and scenario_checks:
        raise SystemExit(f"Scenario checks failed: {'; '.join(scenario_checks)}")
    return result


def _render_report(truth: dict, fleet: dict, result: dict) -> str:
    lines = [
        f"# Validation report for run {result['run_id']}",
        "",
        "## Schema",
        f"- Schema errors: {len(result.get('schema_errors', []))}",
        "",
        "## Scores",
        f"- Incident type precision: {result['incident_type_precision']:.2f}",
        f"- Incident type recall: {result['incident_type_recall']:.2f}",
        f"- Ranking quality (hit rate): {result['ranking_score']:.2f}",
        f"- Cluster detected: {'yes' if result['cluster_detected'] else 'no'}",
        "",
        "## Truth labels",
        f"- Expected types: {', '.join(truth.get('expects_incident_types', []))}",
        f"- Expects clustered outage: {truth.get('expects_clustered_outage')}",
        f"- Expected top hosts: {', '.join(truth.get('expected_top_hosts', []))}",
        f"- Scenario tags: {', '.join(truth.get('scenario_tags', []))}",
        "",
        "## Fleet summary snapshot",
        f"- Host count: {fleet.get('host_count')}",
        f"- Incident count: {fleet.get('incident_count')}",
        f"- Clusters detected: {len(fleet.get('clusters', []))}",
        f"- Top hosts seen: {', '.join([h['host_id'] for h in fleet.get('top_hosts', [])])}",
        "",
        "## Notes",
        "These scores are deterministic for the given seed and snapshots. Expand validation as rules grow.",
    ]
    if result.get("schema_errors"):
        lines.append("")
        lines.append("## Schema errors")
        for err in result["schema_errors"]:
            lines.append(f"- {err}")
    if result.get("scenario_warnings"):
        lines.append("")
        lines.append("## Scenario warnings")
        for warn in result["scenario_warnings"]:
            lines.append(f"- {warn}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate incident detection outputs against synthetic truth labels.")
    parser.add_argument("--run-id", required=True, help="Run identifier used by generator/worker.")
    parser.add_argument(
        "--artifacts-root",
        default=DEFAULT_ARTIFACTS_ROOT,
        help="Root for artifacts. Defaults to ARTIFACTS_ROOT or ./artifacts.",
    )
    parser.add_argument(
        "--strict-scenario",
        action="store_true",
        help="Fail validation when scenario warnings are present (useful for CI).",
    )
    args = parser.parse_args()
    validate(run_id=args.run_id, artifacts_root=args.artifacts_root, strict_scenario=args.strict_scenario)


if __name__ == "__main__":
    main()
