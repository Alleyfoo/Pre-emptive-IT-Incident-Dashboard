# Copilot Instructions

NOTE: Canonical agent role definitions live in `agent-base/agents/`.
`.github/agents/` is an auto-generated mirror for Copilot compatibility.
Edit `agent-base/*` and run `python scripts/sync_github_agents.py`.

This repo describes an **agentic circuit architecture**:
- Speaker/Orchestrator routes work and enforces plan-first.
- Shadow agent logs telemetry and decisions.
- Compassionate layer adds supportive text without changing technical outputs.
- Dataframe pipeline handles tabular data (headers → schema → transform → validate → save).

## Guardrails
- Pass artifact keys, not full payloads.
- New facts = new artifact key (immutability).
- Require confidence + alternatives for any ambiguous step.
- Enforce Plan → Delegate → Review → Merge.
