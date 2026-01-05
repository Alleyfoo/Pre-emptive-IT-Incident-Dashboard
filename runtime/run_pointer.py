import os
from typing import Optional

from runtime.artifact_store import ArtifactStore

LATEST_KEY = "latest_run.txt"


def write_latest(store: ArtifactStore, run_id: str) -> None:
    store.write_text(LATEST_KEY, run_id, content_type="text/plain")


def read_latest(store: ArtifactStore) -> Optional[str]:
    if store.exists(LATEST_KEY):
        try:
            return store.read_text(LATEST_KEY).strip()
        except Exception:
            return None
    return None


def get_latest_run_id(store: ArtifactStore) -> Optional[str]:
    latest = read_latest(store)
    if latest:
        return latest
    runs = store.list_runs()
    if not runs:
        return None
    # If run ids are date-prefixed, the last sorted is likely newest; otherwise, fallback to last.
    return runs[-1]
