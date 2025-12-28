# Agent Base Repo (Starter Kit)

This repository is a minimal, reusable “agent kit” for planning and running agentic workflows without copying any project-specific application code. It is designed to be understandable by a skeptical engineer, portable to any tooling stack, and easy to extend later.

## Canonical Source of Truth

All canonical content lives under `agent-base/`. The `.github/` directory mirrors agent and skill definitions for VS Code/Copilot users.

To sync mirrors:
```
python scripts/sync_github_agents.py
```

## What’s Inside

- **agent-base/**: reusable, tool-agnostic agent roles, skills, templates, contracts, examples
- **.github/**: optional VS Code/Copilot-friendly agent definitions and skills

## First Workflow

1. Review `agent-base/docs/philosophy.md` and `agent-base/docs/agent-roles.md`.
2. Start a plan using `agent-base/templates/agent-plan.md`.
3. Run the loop: **Plan → Delegate → Review → Merge**.
4. Capture decisions in the artifact contract and shadow log.

## Reusable VS Code Agents

If you use VS Code or Copilot, see `.github/agents/` and `.github/skills/` for short, strict agent definitions and playbooks. Intended workflow:

**Plan → Delegate → Review → Merge**

The repo also works without these tools—everything is available as plain Markdown under `agent-base/`.

## Codex Prompt Header

When you run GPT-5.2-Codex, start tasks with this header:

Repo orientation:
- This repo is tool-agnostic. VS Code/Copilot is optional.
- Canonical: agent-base/*
- Mirrors for Copilot: .github/agents and .github/skills
- Never edit mirrors directly; edit canonical and run python scripts/sync_github_agents.py
- Focus: role specs, playbooks, contracts, templates (not a full pipeline)
