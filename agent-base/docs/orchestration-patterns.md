# Orchestration Patterns

## Diverge → Converge

1. **Parallel discovery**: collect evidence packets (headers, samples, constraints).
2. **Plan**: converge on a single plan with acceptance criteria.
3. **Parallel execution**: run header → schema → transform → validate → save in sequence or in parallel where safe.
4. **Review**: audit logs and diffs, verify acceptance criteria.

## Plan-First Contract

Every run should start with:
- Inputs
- Constraints
- Acceptance criteria
- Files-to-touch list
- Non-goals
