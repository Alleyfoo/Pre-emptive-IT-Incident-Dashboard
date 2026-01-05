# Pre-emptive IT Incident Dashboard — Demo Report

**Run:** `preemptive-worker-m9gbd`  
**Host:** `HOST-001`

## Executive summary
- Verdict: Contact user
- Window: 2026-01-05T14:11:58Z -> 2026-01-05T14:41:58Z
- Why: disk_full (sev 70); network_instability (sev 65)
- Next actions: Clear temp folders and large caches.; Reset adapter and DNS cache, verify driver version.
- Evidence: 2026-01-05T14:41:58Z Disk 2019 disk full: C: volume at 99%, write failures, temp/profile cannot expand

## Fleet summary
```json
{
  "schema_version": "1.0",
  "run_id": "preemptive-worker-m9gbd",
  "generated_at": "2026-01-05T14:42:24.359955+00:00",
  "window": {
    "start": "2026-01-05T14:11:58Z",
    "end": "2026-01-05T14:41:58Z"
  },
  "host_count": 1,
  "incident_count": 2,
  "overall_risk_score": 74,
  "top_hosts": [
    {
      "host_id": "HOST-001",
      "score": 70,
      "reasons": [
        "disk_full (sev 70)",
        "network_instability (sev 65)"
      ],
      "incident_refs": [
        "HOST-001-incident-1",
        "HOST-001-incident-2"
      ],
      "user_id": "user-87c6d7473a59",
      "action": "contact",
      "delta_score": null,
      "action_reason": "High severity or cluster spike"
    }
  ],
  "clusters": [
    {
      "signature_hash": "bdfdbe16a1d2",
      "signature_key": "Disk:2019|disk full: c: volume at <n>%, write failures, temp/profile cannot expand",
      "type": "disk_full",
      "affected_hosts": 1,
      "example_hosts": [
        "HOST-001"
      ],
      "severity": 70,
      "first_seen": "2026-01-05T14:11:58+00:00",
      "last_seen": "2026-01-05T14:41:58+00:00",
      "status": "new",
      "delta_affected_hosts": null
    },
    {
      "signature_hash": "b308ff1a2715",
      "signature_key": "DNS Client Events:1014|name resolution failure for critical service",
      "type": "network_instability",
      "affected_hosts": 1,
      "example_hosts": [
        "HOST-001"
      ],
      "severity": 65,
      "first_seen": "2026-01-05T14:11:58+00:00",
      "last_seen": "2026-01-05T14:41:58+00:00",
      "status": "new",
      "delta_affected_hosts": null
    }
  ]
}
```

## Incidents (from timeline.json)
```json
[
  {
    "schema_version": "1.0",
    "incident_id": "HOST-001-incident-1",
    "host_id": "HOST-001",
    "type": "disk_full",
    "window": {
      "start": "2026-01-05T14:11:58Z",
      "end": "2026-01-05T14:41:58Z"
    },
    "detected_at": "2026-01-05T14:42:24.228319+00:00",
    "severity": 70,
    "confidence": 0.75,
    "summary": "Disk usage approaching capacity",
    "signature": {
      "signature_key": "Disk:2019|disk full: c: volume at <n>%, write failures, temp/profile cannot expand",
      "signature_hash": "bdfdbe16a1d2"
    },
    "recommended_actions": [
      "Clear temp folders and large caches.",
      "Expand disk or reassign user data to secondary volume."
    ],
    "evidence": [
      {
        "ts": "2026-01-05T14:41:58Z",
        "provider": "Disk",
        "level": "Error",
        "message": "disk full: C: volume at 99%, write failures, temp/profile cannot expand",
        "event_id": 2019,
        "source": "disk"
      }
    ],
    "tags": []
  },
  {
    "schema_version": "1.0",
    "incident_id": "HOST-001-incident-2",
    "host_id": "HOST-001",
    "type": "network_instability",
    "window": {
      "start": "2026-01-05T14:11:58Z",
      "end": "2026-01-05T14:41:58Z"
    },
    "detected_at": "2026-01-05T14:42:24.228329+00:00",
    "severity": 65,
    "confidence": 0.7,
    "summary": "Network instability detected",
    "signature": {
      "signature_key": "DNS Client Events:1014|name resolution failure for critical service",
      "signature_hash": "b308ff1a2715"
    },
    "recommended_actions": [
      "Reset adapter and DNS cache, verify driver version.",
      "Check site switch/appliance for correlated resets."
    ],
    "evidence": [
      {
        "ts": "2026-01-01T01:40:35.508597+00:00",
        "provider": "DNS Client Events",
        "level": "Warning",
        "message": "Name resolution failure for critical service",
        "event_id": 1014,
        "source": "WindowsEventLog:System"
      },
      {
        "ts": "2026-01-01T10:24:26.987068+00:00",
        "provider": "e1cexpress",
        "level": "Warning",
        "message": "Network link disconnected unexpectedly",
        "event_id": 10400,
        "source": "WindowsEventLog:System"
      }
    ],
    "tags": []
  }
]
```

## Host report
```
# Host report: HOST-001

Window: 2026-01-05T14:11:58Z -> 2026-01-05T14:41:58Z

Incidents:
- [70] Disk near capacity (type=disk_full, confidence=0.75)
  - Action: Clear temp folders and large caches.
  - Action: Expand disk or reassign user data to secondary volume.
  - Evidence: 2026-01-05T14:41:58Z Disk 2019 disk full: C: volume at 99%, write failures, temp/profile cannot expand
- [65] Network instability detected (type=network_instability, confidence=0.7)
  - Action: Reset adapter and DNS cache, verify driver version.
  - Action: Check site switch/appliance for correlated resets.
  - Evidence: 2026-01-01T01:40:35.508597+00:00 DNS Client Events 1014 Name resolution failure for critical service
```
