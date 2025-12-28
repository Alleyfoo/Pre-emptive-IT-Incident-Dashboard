# Skill: excel-observe

## Purpose
Observe Excel files without transforming data. Inventory tables and candidate header rows.

## Steps
1. List sheet names and dimensions.
2. Sample top N rows per sheet.
3. Identify candidate header rows (by density and text patterns).
4. Emit an evidence packet with sheet names, candidate rows, and small previews.

## Constraints
- Observation-only. No schema/transform decisions.
- Do not guess; record ambiguities.
