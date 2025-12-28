#!/usr/bin/env python3
from __future__ import annotations

import argparse
import filecmp
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"

REQUIRED_PATHS = [
    ROOT / "agent-base",
    ROOT / "agent-base" / "agents",
    ROOT / "agent-base" / "docs",
    ROOT / "agent-base" / "skills",
    ROOT / "scripts" / "sync_github_agents.py",
    README,
]

PATH_PATTERN = re.compile(r"`([^`]+)`")
PATH_PREFIXES = ("agent-base/", ".github/", "scripts/")


def validate_required_paths() -> list[str]:
    missing = []
    for path in REQUIRED_PATHS:
        if not path.exists():
            missing.append(str(path.relative_to(ROOT)))
    return missing


def iter_readme_paths() -> list[Path]:
    text = README.read_text(encoding="utf-8")
    paths: list[Path] = []
    for match in PATH_PATTERN.finditer(text):
        raw = match.group(1).strip()
        if not raw or " " in raw:
            continue
        if raw.startswith(PATH_PREFIXES) or raw.endswith(".md"):
            paths.append(ROOT / raw)
    return paths


def validate_readme_paths() -> list[str]:
    missing = []
    for path in iter_readme_paths():
        if not path.exists():
            missing.append(str(path.relative_to(ROOT)))
    return missing


def compare_dirs(src: Path, dest: Path) -> list[str]:
    mismatches = []
    if not src.exists() or not dest.exists():
        return mismatches
    for src_path in src.rglob("*.md"):
        rel = src_path.relative_to(src)
        dest_path = dest / rel
        if not dest_path.exists():
            mismatches.append(str(rel))
            continue
        if not filecmp.cmp(src_path, dest_path, shallow=False):
            mismatches.append(str(rel))
    return mismatches


def check_mirrors() -> list[str]:
    agent_src = ROOT / "agent-base" / "agents"
    skill_src = ROOT / "agent-base" / "skills"
    agent_dest = ROOT / ".github" / "agents"
    skill_dest = ROOT / ".github" / "skills"
    mismatches = []
    mismatches.extend(compare_dirs(agent_src, agent_dest))
    mismatches.extend(compare_dirs(skill_src, skill_dest))
    return mismatches


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke check Agent Base repo.")
    parser.add_argument(
        "--check-mirrors",
        action="store_true",
        help="Fail if .github mirrors do not match agent-base.",
    )
    args = parser.parse_args()

    errors = []
    missing = validate_required_paths()
    if missing:
        errors.append(f"Missing required paths: {', '.join(missing)}")

    readme_missing = validate_readme_paths()
    if readme_missing:
        errors.append(f"README references missing paths: {', '.join(readme_missing)}")

    if args.check_mirrors:
        mismatches = check_mirrors()
        if mismatches:
            errors.append(
                "Mirror mismatch detected. Run 'python scripts/sync_github_agents.py'."
            )

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print("Smoke check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
