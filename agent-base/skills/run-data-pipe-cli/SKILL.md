# Skill: run-data-pipe-cli

## Purpose
Run the canonical data pipeline CLI safely and consistently.

## Canonical Usage
```
python -m app.cli ...
```

## Examples
- Input/output inside allow-root:
  ```
  python -m app.cli --input /allowed/root/in.xlsx --output /allowed/root/out/
  ```
- Dev mode with explicit allow-root:
  ```
  python -m app.cli --allow-root /tmp/dev --input /tmp/dev/in.xlsx --output /tmp/dev/out/
  ```
- Stage input:
  ```
  python -m app.cli --stage-input --input /allowed/root/in.xlsx --output /allowed/root/out/
  ```

## Output Contract
CLI responses must be JSON and include:
```
{"ok": true/false, ...}
```
