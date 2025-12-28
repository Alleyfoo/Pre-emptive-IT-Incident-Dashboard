# START_HERE_AI

This repository is a **tool-agnostic Agent Base kit**.

Important: The word "agent" here means **a reusable role specification** (Markdown contracts + JSON output shapes),
NOT a VS Code/Copilot runtime agent framework.

## What this repo is
- A reusable base for future projects that use **role-based LLM agents**.
- It contains:
  - **Agent role definitions** (what each role does/does not do)
  - **Skills/playbooks** (how to apply roles to tasks)
  - **Contracts** (artifact keys, schemas, expected outputs)
  - **Templates** (plan-first task format, runbooks)

This repo intentionally avoids shipping a full "pipeline implementation".
It focuses on behavior, boundaries, and reproducible artifacts.

## Where the canonical content lives
✅ Canonical source of truth is under:

- `agent-base/agents/`   (role definitions)
- `agent-base/skills/`   (skills/playbooks)
- `agent-base/contracts/` (artifact/key contracts)
- `agent-base/templates/` (planning templates)

## Why `.github/` exists
`.github/` exists ONLY to support tools (especially VS Code + GitHub Copilot) that look for agent definitions inside `.github/`.

- `.github/agents/` is a **mirror** of `agent-base/agents/`
- `.github/skills/` is a **mirror** of `agent-base/skills/`
- `.github/copilot-instructions.md` contains tool-facing guidance for Copilot users

Do NOT manually edit `.github/agents` or `.github/skills`.
Edit the canonical files under `agent-base/` and then run the sync script.

## Syncing mirrors
To update `.github/` mirrors from canonical `agent-base/`:

```bash
python scripts/sync_github_agents.py


This copies agent and skill files into .github/ so Copilot can see them.

Artifact-first philosophy (keys not payloads)

Agents should NOT pass large raw data between themselves.
They pass:

run_id

artifact_key references (paths/hashes)

metadata + decisions + confidence

New facts => new artifact key (do not overwrite artifacts).
See agent-base/contracts/artifacts.md.

Typical workflow (Plan → Delegate → Review)

Write a plan using agent-base/templates/agent-plan.md

Ask the Orchestrator role to split into tasks and select specialist roles

Specialists produce artifacts (HeaderSpec, SchemaSpec, TransformPlan, ValidationReport, SaveManifest)

Human reviews diffs/artifacts

Execute deterministic runner in the target project (outside this base repo)

If you are an automated coding agent (Codex, etc.)

Treat agent-base/ as the canonical content.

If asked to modify agent roles/skills/contracts, change agent-base/* and run sync.

Do not assume VS Code Copilot is available.

Do not implement a full pipeline unless explicitly requested; this repo is a reusable kit.

End.


This file alone will reduce “agent wandering” by like 80%.
