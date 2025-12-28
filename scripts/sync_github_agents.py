#!/usr/bin/env python3
from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
AGENT_SRC = ROOT / "agent-base" / "agents"
SKILL_SRC = ROOT / "agent-base" / "skills"
AGENT_DEST = ROOT / ".github" / "agents"
SKILL_DEST = ROOT / ".github" / "skills"


def sync_dir(src: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    for path in src.rglob("*.md"):
        rel = path.relative_to(src)
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def main() -> None:
    sync_dir(AGENT_SRC, AGENT_DEST)
    sync_dir(SKILL_SRC, SKILL_DEST)
    print(f"Synced agents to {AGENT_DEST}")
    print(f"Synced skills to {SKILL_DEST}")


if __name__ == "__main__":
    main()
