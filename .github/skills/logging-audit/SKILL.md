# Skill: logging-audit

## Purpose
Maintain a minimal audit trail for decisions and confirmations.

## Required Fields
- timestamp
- actor/role
- decision
- evidence summary
- confirmations

## Rules
- Log user confirmations explicitly.
- Never modify plans or outputs—only record them.
- Keep entries append-only.
