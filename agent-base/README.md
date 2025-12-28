# Agent Base

A minimal starter kit of reusable agent roles, skills, and planning templates. This repo contains **no project-specific pipeline implementation code**—only plain Markdown contracts and playbooks.

## Canonical Content

`agent-base/` is the single source of truth. If you need VS Code/Copilot mirrors under `.github/`, run:
```
python scripts/sync_github_agents.py
```

## How to Use

Pick one of these patterns:

1) **Copy-in**: copy `agent-base/` into a new repo and adapt names.
2) **Git submodule**: add this repo as a submodule and reference the files.
3) **Reference-only**: link to these docs and keep them in a shared knowledge base.

## Workflow Loop

**Plan → Delegate → Review → Merge**

1. **Plan**: define inputs, constraints, acceptance criteria, and files-to-touch.
2. **Delegate**: assign focused roles (header → schema → transform → validate → save).
3. **Review**: check diffs, verify acceptance criteria, audit logs.
4. **Merge**: ship the minimal change and record decisions.

## Contents

- `docs/`: philosophy, roles, orchestration patterns, review and safety guardrails
- `agents/`: strict role contracts for each pipeline stage
- `skills/`: repeatable playbooks for safe operations
- `templates/`: reusable plan/runbook templates
- `contracts/`: artifact contracts and key conventions
- `examples/`: tiny, synthetic example packets
