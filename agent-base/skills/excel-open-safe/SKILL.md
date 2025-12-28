# Skill: excel-open-safe

## Purpose
Safely read Excel files without corrupting IDs or coercing dates.

## Guardrails
- Preserve IDs as strings (explicit dtype for identifier columns when known).
- Avoid accidental date coercion; prefer parsing only when required.
- Handle `sheet_name=None` carefully (explicitly select the intended sheet).
- Respect allowlist roots and staging (`--stage-input`).

## Steps
1. Read Excel with pandas (`pd.read_excel`) using explicit dtypes where possible.
2. If `sheet_name=None`, enumerate sheets and choose explicitly.
3. Preview top N rows and collect:
   - column list
   - header candidates
   - sample values (first 10 non-null per column)
4. Emit an evidence packet for the header agent.

## Pitfalls
- Leading zeros in IDs being dropped.
- Dates parsed from numeric strings.
- Selecting the wrong sheet when multiple exist.
