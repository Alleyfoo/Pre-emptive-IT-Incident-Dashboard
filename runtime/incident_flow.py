import argparse
import hashlib
import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from runtime.artifact_store import ArtifactStore, build_artifact_store, is_gcs_uri, parse_gcs_uri
from runtime.schema_validate import validate_or_raise
from runtime.run_pointer import write_latest

DEFAULT_ARTIFACTS_ROOT = os.environ.get("ARTIFACTS_ROOT") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "artifacts",
)
DEFAULT_RETENTION_HOURS = int(os.environ.get("RETENTION_HOURS", "48"))
REDACTION_MODE = os.environ.get("REDACTION_MODE", "balanced").lower()
REDACTION_SALT = os.environ.get("REDACTION_SALT", "preemptive-it-salt")
LOCK_TTL_MINUTES = int(os.environ.get("LOCK_TTL_MINUTES", "30"))
LOCK_KEY = "locks/worker.lock"


@dataclass
class Incident:
    id: str
    type: str
    title: str
    severity: int
    confidence: float
    recommended_actions: List[str]
    evidence: List[dict]
    cluster_signature: str
    cluster_basis: Dict[str, object]
    start: str
    end: str
    summary: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def _normalize_message_template(message: str) -> str:
    normalized = message.lower().strip()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"\d+", "<n>", normalized)
    return normalized


def _redact_message(message: str) -> str:
    if not message or REDACTION_MODE == "off":
        return message or ""
    redacted = message
    secret_patterns = [
        r"password=\S+",
        r"secret\s*[:=]\s*\S+",
        r"token=\S+",
    ]
    for pattern in secret_patterns:
        redacted = re.sub(pattern, "[REDACTED]", redacted, flags=re.IGNORECASE)
    redacted = re.sub(r"[A-Za-z0-9+/=]{24,}", "[REDACTED]", redacted)
    redacted = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "[REDACTED_EMAIL]", redacted)
    redacted = re.sub(r"[A-Za-z]:\\\\[^\\s]+", "[REDACTED_PATH]", redacted)
    redacted = re.sub(r"[A-Za-z]:/[^\\s]+", "[REDACTED_PATH]", redacted)
    redacted = re.sub(r"\\\\[A-Za-z0-9_.-]+\\\\[^\\s]+", "[REDACTED_PATH]", redacted)
    ip_pattern = r"\b(\d{1,3}\.\d{1,3}\.\d{1,3})\.\d{1,3}\b"
    redacted = re.sub(ip_pattern, r"\1.0/24", redacted)
    if REDACTION_MODE == "strict":
        redacted = re.sub(r"\d{2}:\d{2}:\d{2}", "HH:MM:SS", redacted)
    return redacted


def _hash_user(value: Optional[str]) -> Optional[str]:
    if not value or REDACTION_MODE != "strict":
        return value
    digest = hashlib.sha256((REDACTION_SALT + value).encode("utf-8")).hexdigest()
    return f"user-{digest[:12]}"


def _lock_payload(run_id: str, ttl_minutes: int) -> dict:
    return {"run_id": run_id, "created_at": _utc_now_iso(), "ttl_minutes": ttl_minutes}


def _lock_stale(lock_data: dict, ttl_minutes: int) -> bool:
    try:
        created = _parse_ts(lock_data.get("created_at", ""))
    except Exception:
        return True
    return created < datetime.now(timezone.utc) - timedelta(minutes=ttl_minutes)


def _acquire_lock(store: ArtifactStore, run_id: str, ttl_minutes: int) -> Tuple[bool, bool]:
    payload = json.dumps(_lock_payload(run_id, ttl_minutes), indent=2, ensure_ascii=True).encode("utf-8")
    created = store.create_if_absent(LOCK_KEY, payload, content_type="application/json")
    if created:
        return True, False
    # If lock exists, check staleness
    try:
        lock_data = json.loads(store.read_text(LOCK_KEY))
    except Exception:
        lock_data = {}
    if _lock_stale(lock_data, ttl_minutes):
        store.delete_prefix(LOCK_KEY)
        created_after = store.create_if_absent(LOCK_KEY, payload, content_type="application/json")
        if created_after:
            return True, True
    return False, False


def _release_lock(store: ArtifactStore) -> None:
    store.delete_prefix(LOCK_KEY)


def _signature_for_event(provider: str, event_id: object, message: str) -> Tuple[str, Dict[str, object]]:
    template = _normalize_message_template(message)
    basis = {"provider": provider, "event_id": event_id, "message_template": template}
    text = f"{provider}|{event_id}|{template}"
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
    return digest, basis


def _signature_key(basis: Dict[str, object]) -> str:
    provider = basis.get("provider", "") or ""
    event_id = basis.get("event_id", "") or ""
    template = (basis.get("message_template", "") or "")[:200]
    return f"{provider}:{event_id}|{template}"


def _clean_evidence(events: List[dict]) -> List[dict]:
    cleaned: List[dict] = []
    for e in events:
        message = e.get("message", "") or ""
        if len(message) > 512:
            message = message[:509] + "..."
        record = {
            "ts": e.get("ts", ""),
            "provider": e.get("provider", ""),
            "level": e.get("level", ""),
            "message": message,
        }
        if e.get("event_id") is not None:
            record["event_id"] = e.get("event_id")
        if e.get("source") is not None:
            record["source"] = e.get("source")
        if e.get("record_id") is not None:
            record["record_id"] = e.get("record_id")
        cleaned.append(record)
    return cleaned


def _append_shadow(store: ArtifactStore, run_id: str, stage: str, message: str, **kwargs) -> None:
    event = {"ts": _utc_now_iso(), "stage": stage, "message": message}
    if kwargs:
        event["meta"] = kwargs
    key = f"{run_id}/shadow.jsonl"
    existing = ""
    if store.exists(key):
        try:
            existing = store.read_text(key)
        except Exception:
            existing = ""
    store.write_text(key, f"{existing}{json.dumps(event)}\n", content_type="application/json")


def _load_json(store: ArtifactStore, key: str) -> dict:
    return json.loads(store.read_text(key))


def _list_snapshot_keys(store: ArtifactStore, prefix: str) -> List[str]:
    keys = store.list_prefix(prefix)
    return [key for key in keys if key.endswith(".json")]


def _load_tickets(store: ArtifactStore, run_id: str, prefix: Optional[str]) -> Dict[str, List[dict]]:
    tickets: Dict[str, List[dict]] = {}
    base = prefix or f"{run_id}/tickets"
    for key in store.list_prefix(base):
        if not key.endswith(".json"):
            continue
        payload = _load_json(store, key)
        host_id = payload.get("host_id") or "unknown"
        tickets.setdefault(host_id, []).append(payload)
    return tickets


def _incident_id(host_id: str, idx: int) -> str:
    return f"{host_id}-incident-{idx+1}"


def _summarize_events(events: Iterable[dict]) -> Tuple[str, str]:
    timestamps: List[datetime] = []
    for event in events:
        try:
            timestamps.append(_parse_ts(event.get("ts", "")))
        except Exception:
            continue
    if not timestamps:
        now = _utc_now_iso()
        return now, now
    start_dt = min(timestamps)
    end_dt = max(timestamps)
    return start_dt.isoformat(), end_dt.isoformat()


def _recommended_actions(incident_type: str) -> List[str]:
    actions = {
        "bsod": [
            "Capture minidump and driver list before reboot loops clear them.",
            "Roll back or update the last installed driver/patch.",
        ],
        "disk_full": [
            "Clear temp folders and large caches.",
            "Expand disk or reassign user data to secondary volume.",
        ],
        "service_crash_loop": [
            "Review service logs for repeated stop codes.",
            "Restart service under supervisor and collect crash dumps.",
        ],
        "network_instability": [
            "Reset adapter and DNS cache, verify driver version.",
            "Check site switch/appliance for correlated resets.",
        ],
        "update_failure": [
            "Re-run updater with verbose logging enabled.",
            "Remove partially applied patches and retry.",
        ],
    }
    return actions.get(incident_type, ["Collect logs and escalate to tier 2."])


def _detect_bsod(events: List[dict]) -> Optional[Incident]:
    evidence = [e for e in events if "bsod" in e.get("tags", []) or "unexpected_shutdown" in e.get("tags", [])]
    if not evidence:
        return None
    severity = min(100, 85 + 5 * (len(evidence) - 1))
    confidence = 0.9 if len(evidence) > 1 else 0.75
    sig, basis = _signature_for_event(evidence[0].get("provider", ""), evidence[0].get("event_id"), evidence[0].get("message", ""))
    start, end = _summarize_events(evidence)
    return Incident(
        id="",
        type="bsod",
        title="Blue screen / unexpected shutdown",
        summary="Detected blue screen or unexpected shutdown events",
        severity=severity,
        confidence=confidence,
        recommended_actions=_recommended_actions("bsod"),
        evidence=_clean_evidence(evidence),
        cluster_signature=sig,
        cluster_basis=basis,
        start=start,
        end=end,
    )


def _detect_disk_full(events: List[dict]) -> Optional[Incident]:
    evidence = [e for e in events if "disk_full" in e.get("tags", []) or "disk" in e.get("source", "").lower()]
    if not evidence:
        return None
    severity = 70 + min(20, 5 * (len(evidence) - 1))
    confidence = 0.7 + 0.05 * len(evidence)
    sig, basis = _signature_for_event(evidence[0].get("provider", ""), evidence[0].get("event_id"), evidence[0].get("message", ""))
    start, end = _summarize_events(evidence)
    return Incident(
        id="",
        type="disk_full",
        title="Disk near capacity",
        summary="Disk usage approaching capacity",
        severity=min(95, severity),
        confidence=min(0.95, confidence),
        recommended_actions=_recommended_actions("disk_full"),
        evidence=_clean_evidence(evidence),
        cluster_signature=sig,
        cluster_basis=basis,
        start=start,
        end=end,
    )


def _detect_service_crash(events: List[dict]) -> Optional[Incident]:
    crash_events = [e for e in events if "service_crash" in e.get("tags", []) or "service control manager" in e.get("provider", "").lower()]
    if len(crash_events) < 2:
        return None
    severity = 65 + 5 * min(5, len(crash_events))
    confidence = 0.7 + 0.05 * len(crash_events)
    sig, basis = _signature_for_event(crash_events[0].get("provider", ""), crash_events[0].get("event_id"), crash_events[0].get("message", ""))
    start, end = _summarize_events(crash_events)
    return Incident(
        id="",
        type="service_crash_loop",
        title="Service crash loop detected",
        summary="Repeated service crashes detected",
        severity=min(90, severity),
        confidence=min(0.95, confidence),
        recommended_actions=_recommended_actions("service_crash"),
        evidence=_clean_evidence(crash_events),
        cluster_signature=sig,
        cluster_basis=basis,
        start=start,
        end=end,
    )


def _detect_network(events: List[dict]) -> Optional[Incident]:
    net_events = [e for e in events if any(tag in e.get("tags", []) for tag in ["network_reset", "dns_failure"])]
    if not net_events:
        return None
    severity = 55 + 5 * min(6, len(net_events))
    confidence = 0.6 + 0.05 * len(net_events)
    sig, basis = _signature_for_event(net_events[0].get("provider", ""), net_events[0].get("event_id"), net_events[0].get("message", ""))
    start, end = _summarize_events(net_events)
    return Incident(
        id="",
        type="network_instability",
        title="Network adapter resets / DNS failures",
        summary="Network instability detected",
        severity=min(85, severity),
        confidence=min(0.9, confidence),
        recommended_actions=_recommended_actions("network_instability"),
        evidence=_clean_evidence(net_events),
        cluster_signature=sig,
        cluster_basis=basis,
        start=start,
        end=end,
    )


def _detect_update_failure(events: List[dict]) -> Optional[Incident]:
    upd_events = [e for e in events if "update_failure" in e.get("tags", []) or "update" in e.get("source", "").lower()]
    if len(upd_events) < 1:
        return None
    severity = 65 + 5 * min(4, len(upd_events) - 1)
    confidence = 0.65 + 0.05 * len(upd_events)
    sig, basis = _signature_for_event(upd_events[0].get("provider", ""), upd_events[0].get("event_id"), upd_events[0].get("message", ""))
    start, end = _summarize_events(upd_events)
    return Incident(
        id="",
        type="update_failure",
        title="Update or install failure burst",
        summary="Repeated update or install failures",
        severity=min(90, severity),
        confidence=min(0.9, confidence),
        recommended_actions=_recommended_actions("update_failure"),
        evidence=_clean_evidence(upd_events),
        cluster_signature=sig,
        cluster_basis=basis,
        start=start,
        end=end,
    )


def detect_incidents_for_host(host_id: str, events: List[dict]) -> List[Incident]:
    detectors = [
        _detect_bsod,
        _detect_disk_full,
        _detect_service_crash,
        _detect_network,
        _detect_update_failure,
    ]
    incidents: List[Incident] = []
    for detector in detectors:
        incident = detector(events)
        if incident:
            incident.id = _incident_id(host_id, len(incidents))
            incidents.append(incident)
    return incidents


def _host_severity(incidents: List[Incident]) -> int:
    if not incidents:
        return 0
    return max(incident.severity for incident in incidents)


def _incident_record(host_id: str, window: Dict[str, str], incident: Incident) -> dict:
    signature_key = _signature_key(incident.cluster_basis)
    return {
        "schema_version": "1.0",
        "incident_id": incident.id,
        "host_id": host_id,
        "type": incident.type,
        "window": {"start": window.get("start"), "end": window.get("end")},
        "detected_at": _utc_now_iso(),
        "severity": incident.severity,
        "confidence": incident.confidence,
        "summary": incident.summary or incident.title,
        "signature": {
            "signature_key": signature_key,
            "signature_hash": incident.cluster_signature,
        },
        "recommended_actions": incident.recommended_actions,
        "evidence": incident.evidence,
        "tags": [],
    }


def _history_key() -> str:
    return "history"


def _load_history(store: ArtifactStore, limit: int = 7) -> List[dict]:
    if not store.exists(_history_key()):
        return []
    entries = []
    for key in sorted(store.list_prefix(_history_key())):
        if not key.endswith(".json"):
            continue
        try:
            entries.append(json.loads(store.read_text(key)))
        except Exception:
            continue
    return entries[-limit:]


def _append_history(store: ArtifactStore, summary: dict, limit: int = 7) -> None:
    history_entry = {
        "run_id": summary.get("run_id"),
        "generated_at": summary.get("generated_at"),
        "clusters": [
            {"signature_hash": c.get("signature_hash"), "affected_hosts": c.get("affected_hosts"), "severity": c.get("severity")}
            for c in summary.get("clusters", [])
        ],
        "top_hosts": [
            {"host_id": h.get("host_id"), "score": h.get("score")}
            for h in summary.get("top_hosts", [])
        ],
    }
    store.write_text(f"{_history_key()}/{summary.get('run_id')}.json", json.dumps(history_entry, indent=2, ensure_ascii=True), content_type="application/json")
    # Trim local history for LocalArtifactStore only (GCS lifecycle should handle retention)
    existing = sorted(store.list_prefix(_history_key()))
    if hasattr(store, "root_dir"):  # best-effort local trim
        extras = max(0, len(existing) - limit)
        for old in existing[:extras]:
            store.delete_prefix(old)


def _previous_summary(history: List[dict]) -> Optional[dict]:
    if not history:
        return None
    return history[-1]


def _latest_ts(events: List[dict]) -> str:
    if not events:
        return ""
    timestamps = []
    for e in events:
        try:
            timestamps.append(_parse_ts(e.get("ts", "")))
        except Exception:
            continue
    if not timestamps:
        return ""
    return max(timestamps).isoformat()


def build_host_timelines(
    store: ArtifactStore,
    run_id: str,
    snapshots: List[dict],
    ticket_prefix: Optional[str] = None,
) -> Dict[str, dict]:
    tickets = _load_tickets(store, run_id, ticket_prefix)
    timelines: Dict[str, dict] = {}
    for snap in snapshots:
        key = snap["key"]
        data = snap["data"]
        host_id = data.get("host_id") or os.path.splitext(os.path.basename(key))[0]
        window = data.get("window", {"start": data.get("window_start"), "end": data.get("window_end")})
        events = sorted(data.get("events", []), key=lambda e: e.get("ts", ""))
        for event in events:
            if "message" in event:
                event["message"] = _redact_message(event.get("message", ""))
        incidents = detect_incidents_for_host(host_id, events)
        for incident in incidents:
            incident.id = incident.id or _incident_id(host_id, len(incidents))
        timeline = {
            "schema_version": "1.0",
            "host_id": host_id,
            "user_id": _hash_user(data.get("user_id")),
            "window": window,
            "events": events,
            "incidents": [_incident_record(host_id, window, inc) for inc in incidents],
            "tickets": tickets.get(host_id, []),
            "last_event_ts": _latest_ts(events),
            "severity": _host_severity(incidents),
        }
        timelines[host_id] = timeline
        _append_shadow(store, run_id, "timeline", f"Evaluated {host_id}", incidents=len(incidents))
    return timelines


def _aggregate_clusters(timelines: Dict[str, dict]) -> List[dict]:
    clusters: Dict[str, dict] = {}
    for host_id, timeline in timelines.items():
        for inc in timeline.get("incidents", []):
            signature = inc.get("signature", {})
            sig_hash = signature.get("signature_hash")
            sig_key = signature.get("signature_key")
            if not sig_hash:
                continue
            if sig_hash not in clusters:
                clusters[sig_hash] = {
                    "signature_hash": sig_hash,
                    "signature_key": sig_key,
                    "type": inc.get("type"),
                    "affected_hosts": set(),
                    "severity": 0,
                    "first_seen": None,
                    "last_seen": None,
                }
            cluster = clusters[sig_hash]
            cluster["affected_hosts"].add(host_id)
            cluster["severity"] = max(cluster["severity"], inc.get("severity", 0))
            try:
                inc_start = _parse_ts(inc.get("window", {}).get("start", ""))
                inc_end = _parse_ts(inc.get("window", {}).get("end", ""))
            except Exception:
                continue
            if cluster["first_seen"] is None or inc_start < cluster["first_seen"]:
                cluster["first_seen"] = inc_start
            if cluster["last_seen"] is None or inc_end > cluster["last_seen"]:
                cluster["last_seen"] = inc_end

    results: List[dict] = []
    for cluster in clusters.values():
        results.append(
            {
                "signature_hash": cluster["signature_hash"],
                "signature_key": cluster.get("signature_key"),
                "type": cluster.get("type"),
                "affected_hosts": len(cluster["affected_hosts"]),
                "example_hosts": sorted(list(cluster["affected_hosts"]))[:20],
                "severity": min(100, cluster["severity"] + 5 * (len(cluster["affected_hosts"]) - 1)),
                "first_seen": cluster["first_seen"].isoformat() if cluster["first_seen"] else None,
                "last_seen": cluster["last_seen"].isoformat() if cluster["last_seen"] else None,
            }
        )
    results.sort(key=lambda c: (c["severity"], c["affected_hosts"]), reverse=True)
    return results


def _top_hosts(timelines: Dict[str, dict], limit: int = 10) -> List[dict]:
    hosts = []
    for host_id, timeline in timelines.items():
        incident_refs = [inc.get("incident_id") for inc in timeline.get("incidents", []) if inc.get("incident_id")]
        reasons = [f"{inc.get('type')} (sev {inc.get('severity')})" for inc in timeline.get("incidents", [])]
        hosts.append(
            {
                "host_id": host_id,
                "user_id": timeline.get("user_id"),
                "score": timeline.get("severity", 0),
                "reasons": reasons,
                "incident_refs": incident_refs,
            }
        )
    hosts.sort(key=lambda h: (h["score"], len(h.get("incident_refs", []))), reverse=True)
    return hosts[:limit]


def _action_for_host(score: int, prev_score: Optional[int], has_cluster_spike: bool, has_new_critical: bool) -> Tuple[str, Optional[int], str]:
    delta = score - prev_score if prev_score is not None else None
    if has_new_critical or has_cluster_spike or (score >= 70 and (prev_score is None or (delta is not None and delta >= 5))):
        return "contact", delta, "High severity or cluster spike"
    if score >= 50 or (delta is not None and delta >= 10):
        return "monitor", delta, "Moderate severity or trending up"
    return "ignore", delta, "Low severity or stable"


def _cluster_status(clusters: List[dict], prev_summary: Optional[dict]) -> None:
    if not prev_summary:
        for cluster in clusters:
            cluster["status"] = "new"
            cluster["delta_affected_hosts"] = None
        return
    prev_map = {c.get("signature_hash"): c for c in prev_summary.get("clusters", [])}
    for cluster in clusters:
        prev = prev_map.get(cluster.get("signature_hash"))
        if not prev:
            cluster["status"] = "new"
            cluster["delta_affected_hosts"] = None
        else:
            delta = cluster.get("affected_hosts", 0) - prev.get("affected_hosts", 0)
            cluster["delta_affected_hosts"] = delta
            if delta >= 2:
                cluster["status"] = "spiking"
            else:
                cluster["status"] = "ongoing"


def build_fleet_summary(run_id: str, timelines: Dict[str, dict], prev_summary: Optional[dict] = None) -> dict:
    clusters = _aggregate_clusters(timelines)
    _cluster_status(clusters, prev_summary)
    hosts = _top_hosts(timelines)
    cluster_by_hash = {c.get("signature_hash"): c for c in clusters}
    host_cluster_map: Dict[str, List[dict]] = {}
    for host_id, timeline in timelines.items():
        for inc in timeline.get("incidents", []):
            sig = inc.get("signature", {}).get("signature_hash")
            if sig and sig in cluster_by_hash:
                host_cluster_map.setdefault(host_id, []).append(cluster_by_hash[sig])
    prev_scores = {}
    if prev_summary:
        for host in prev_summary.get("top_hosts", []):
            prev_scores[host.get("host_id")] = host.get("score")
    for host in hosts:
        related_clusters = host_cluster_map.get(host["host_id"], [])
        has_spike = any(c.get("status") == "spiking" for c in related_clusters)
        has_new = any(c.get("status") == "new" for c in related_clusters)
        has_critical_incident = any("bsod" in r.lower() or "unexpected_shutdown" in r.lower() for r in host.get("reasons", []))
        action, delta, reason = _action_for_host(host["score"], prev_scores.get(host["host_id"]), has_spike, has_new or has_critical_incident)
        host["action"] = action
        host["delta_score"] = delta
        host["action_reason"] = reason
    overall = 0
    if hosts:
        overall = min(100, int(sum(h["score"] for h in hosts[:5]) / max(1, min(5, len(hosts))) + len(clusters) * 2))
    window = {"start": None, "end": None}
    for timeline in timelines.values():
        w = timeline.get("window") or {}
        if w.get("start") and (window["start"] is None or w["start"] < window["start"]):
            window["start"] = w["start"]
        if w.get("end") and (window["end"] is None or w["end"] > window["end"]):
            window["end"] = w["end"]
    if window["start"] is None:
        window["start"] = _utc_now_iso()
    if window["end"] is None:
        window["end"] = window["start"]
    incident_count = sum(len(timeline.get("incidents", [])) for timeline in timelines.values())
    return {
        "schema_version": "1.0",
        "run_id": run_id,
        "generated_at": _utc_now_iso(),
        "window": window,
        "host_count": len(timelines),
        "incident_count": incident_count,
        "overall_risk_score": overall,
        "top_hosts": hosts,
        "clusters": clusters,
    }


def _run_is_pinned(store: ArtifactStore, run_id: str) -> bool:
    return store.exists(f"{run_id}/pinned")


def _run_generated_at(store: ArtifactStore, run_id: str) -> Optional[datetime]:
    key = f"{run_id}/fleet_summary.json"
    if not store.exists(key):
        return None
    try:
        payload = json.loads(store.read_text(key))
        ts = payload.get("generated_at")
        if ts:
            return _parse_ts(ts)
    except Exception:
        return None
    return None


def _load_snapshots(
    snap_store: ArtifactStore,
    run_id: str,
    snapshot_prefix: Optional[str],
    window_hours: int,
    select_mode: str,
    max_hosts: Optional[int],
) -> List[dict]:
    prefix = snapshot_prefix or f"{run_id}/snapshots"
    keys = _list_snapshot_keys(snap_store, prefix)
    host_pattern = re.compile(r"^[A-Za-z0-9._:-]{3,64}$")
    file_pattern = re.compile(r"^snapshot-\d{8}T\d{6}Z\.json$")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    per_host: Dict[str, List[dict]] = {}
    for key in keys:
        parts = key.split("/")
        if len(parts) < 2:
            continue
        host_id = parts[-2]
        filename = parts[-1]
        if not host_pattern.match(host_id):
            continue
        if not file_pattern.match(filename):
            continue
        try:
            data = _load_json(snap_store, key)
        except Exception:
            continue
        receipt_ts = data.get("_receipt_time") or None
        window = data.get("window", {})
        end_ts = window.get("end") or data.get("window_end")
        try:
            end_dt = _parse_ts(end_ts) if end_ts else None
        except Exception:
            end_dt = None
        if end_dt and end_dt < cutoff:
            continue
        host_id = data.get("host_id") or host_id
        per_host.setdefault(host_id, []).append(
            {
                "key": key,
                "data": data,
                "end": end_dt or datetime.now(timezone.utc),
                "receipt": _parse_ts(receipt_ts) if receipt_ts else datetime.now(timezone.utc),
            }
        )
    selected: List[dict] = []
    for host_id, items in per_host.items():
        items.sort(key=lambda item: item["end"], reverse=True)
        if select_mode == "latest" and items:
            selected.append(items[0])
        else:
            selected.extend(items)
    selected.sort(key=lambda item: item["data"].get("host_id", ""))
    if max_hosts is not None:
        selected = selected[:max_hosts]
    return selected


def purge_old_runs(store: ArtifactStore, retention_hours: int, keep_run: str) -> List[str]:
    deleted: List[str] = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
    for run_id in store.list_runs():
        if run_id in {"history"}:
            continue
        if run_id == keep_run or _run_is_pinned(store, run_id):
            continue
        generated_at = _run_generated_at(store, run_id)
        if generated_at and generated_at < cutoff:
            store.delete_prefix(run_id)
            deleted.append(run_id)
    return deleted


def _write_run_status(store: ArtifactStore, run_id: str, status: str, message: str, started_at: Optional[str] = None) -> None:
    payload = {
        "run_id": run_id,
        "status": status,
        "message": message,
        "started_at": started_at,
        "finished_at": _utc_now_iso(),
    }
    store.write_json(f"{run_id}/run_status.json", payload, content_type="application/json")


def write_host_artifacts(store: ArtifactStore, run_id: str, timelines: Dict[str, dict]) -> None:
    for host_id, timeline in timelines.items():
        timeline_key = f"{run_id}/hosts/{host_id}/timeline.json"
        report_key = f"{run_id}/hosts/{host_id}/report.md"
        store.write_text(timeline_key, json.dumps(timeline, indent=2, ensure_ascii=True), content_type="application/json")
        report = _render_host_report(timeline)
        store.write_text(report_key, report, content_type="text/markdown")
        _append_shadow(store, run_id, "write_host", f"Wrote artifacts for {host_id}")


def _render_host_report(timeline: dict) -> str:
    lines = [
        f"# Host report: {timeline.get('host_id', 'unknown')}",
        "",
        f"Window: {timeline.get('window_start', '')} → {timeline.get('window_end', '')}",
        "",
    ]
    incidents = timeline.get("incidents", [])
    if not incidents:
        lines.append("No incidents detected.")
        return "\n".join(lines)
    lines.append("Incidents:")
    for inc in incidents:
        lines.append(f"- [{inc.get('severity')}] {inc.get('title')} (type={inc.get('type')}, confidence={inc.get('confidence')})")
        for action in inc.get("recommended_actions", []):
            lines.append(f"  - Action: {action}")
        if inc.get("evidence"):
            sample = inc["evidence"][0]
            lines.append(f"  - Evidence: {sample.get('ts')} {sample.get('provider')} {sample.get('event_id')} {sample.get('message')}")
    return "\n".join(lines)


def write_fleet_artifacts(store: ArtifactStore, run_id: str, fleet_summary: dict) -> None:
    key = f"{run_id}/fleet_summary.json"
    store.write_text(key, json.dumps(fleet_summary, indent=2, ensure_ascii=True), content_type="application/json")
    _append_shadow(store, run_id, "fleet", "Wrote fleet summary", clusters=len(fleet_summary.get("clusters", [])))


def run_incident_flow(
    run_id: str,
    artifacts_root: str,
    snapshot_root: Optional[str] = None,
    snapshot_prefix: Optional[str] = None,
    ticket_prefix: Optional[str] = None,
    retention_hours: int = DEFAULT_RETENTION_HOURS,
    window_hours: int = 24,
    select_mode: str = "latest",
    max_hosts: Optional[int] = None,
) -> dict:
    store = build_artifact_store(artifacts_root)
    snapshot_store = build_artifact_store(snapshot_root) if snapshot_root else store
    history = _load_history(store)
    prev_summary = _previous_summary(history)
    snapshots = _load_snapshots(snapshot_store, run_id, snapshot_prefix, window_hours, select_mode, max_hosts)
    timelines = build_host_timelines(store, run_id, snapshots=snapshots, ticket_prefix=ticket_prefix)
    fleet = build_fleet_summary(run_id, timelines, prev_summary=prev_summary)
    write_host_artifacts(store, run_id, timelines)
    write_fleet_artifacts(store, run_id, fleet)
    validate_or_raise(store, run_id)
    _append_history(store, fleet)
    write_latest(store, run_id)
    purged = purge_old_runs(store, retention_hours, keep_run=run_id)
    return {"fleet_summary": fleet, "timelines": timelines, "purged_runs": purged}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run incident detection flow against snapshots.")
    parser.add_argument("--run-id", required=False, help="Run identifier (used for artifacts/<run-id>/...). If omitted, a UTC timestamp-based id is generated.")
    parser.add_argument(
        "--artifacts-root",
        default=DEFAULT_ARTIFACTS_ROOT,
        help="Root for artifacts. Defaults to ARTIFACTS_ROOT env or ./artifacts.",
    )
    parser.add_argument(
        "--snapshot-root",
        default=None,
        help="Optional root for reading snapshots (local path or gs://). Defaults to artifacts root.",
    )
    parser.add_argument(
        "--snapshot-prefix",
        default=None,
        help="Optional override for snapshot key prefix (defaults to <run-id>/snapshots).",
    )
    parser.add_argument(
        "--ticket-prefix",
        default=None,
        help="Optional override for ticket key prefix (defaults to <run-id>/tickets).",
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=DEFAULT_RETENTION_HOURS,
        help="Purge runs older than this (skip if pinned). Defaults to RETENTION_HOURS env or 48h.",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=24,
        help="Consider snapshots within this many hours from now (for snapshot-root mode).",
    )
    parser.add_argument(
        "--select-mode",
        choices=["latest", "all"],
        default="latest",
        help="Pick only the latest snapshot per host or all snapshots within the window.",
    )
    parser.add_argument(
        "--max-hosts",
        type=int,
        default=None,
        help="Optional safety cap on number of hosts processed (for rollout).",
    )
    args = parser.parse_args()
    run_id = args.run_id or f"run-{datetime.utcnow().strftime('%Y%m%d-%H%M%SZ')}"
    started_at = _utc_now_iso()
    store = build_artifact_store(args.artifacts_root)
    lock_acquired = False
    break_glass = False
    try:
        lock_acquired, break_glass = _acquire_lock(store, run_id, LOCK_TTL_MINUTES)
        if not lock_acquired:
            _append_shadow(store, run_id, "lock", "Another run in progress; exiting")
            raise RuntimeError("Worker lock held; exiting")
        _append_shadow(store, run_id, "start", "incident_flow started", break_glass=break_glass)
        _write_run_status(store, run_id, status="running", message="started", started_at=started_at)
        result = run_incident_flow(
            run_id=run_id,
            artifacts_root=args.artifacts_root,
            snapshot_root=args.snapshot_root,
            snapshot_prefix=args.snapshot_prefix,
            ticket_prefix=args.ticket_prefix,
            retention_hours=args.retention_hours,
            window_hours=args.window_hours,
            select_mode=args.select_mode,
            max_hosts=args.max_hosts,
        )
        _write_run_status(store, run_id, status="success", message="completed", started_at=started_at)
        _append_shadow(store, run_id, "done", "incident_flow completed")
        if result.get("purged_runs"):
            _append_shadow(store, run_id, "retention", "Purged old runs", purged=result["purged_runs"])
    except Exception as exc:  # noqa: BLE001
        _write_run_status(store, run_id, status="failure", message=str(exc), started_at=started_at)
        _append_shadow(store, run_id, "error", "incident_flow failed", error=str(exc))
        raise
    finally:
        if lock_acquired:
            _release_lock(store)


if __name__ == "__main__":
    main()
