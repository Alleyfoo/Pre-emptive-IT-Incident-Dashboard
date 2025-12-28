# Review and Safety

## Guardrails

- Work in branches or worktrees.
- Review diffs before merge.
- Define acceptance criteria up front.
- Never bypass allowlists.
- Log all decisions and confirmations.

## Artifact Contract

All runs must follow the artifact contract defined in `agent-base/contracts/artifacts.md`:
- Pass artifact keys, not payloads.
- New facts = new artifact key (immutability).

## Operational Safety

- Small, reversible changes.
- Keep tests passing.
- Never change public JSON shapes without updating tests.
