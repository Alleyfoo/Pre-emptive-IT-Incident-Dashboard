import json
import os
from pathlib import Path
from typing import Dict, List

from jsonschema import Draft202012Validator, RefResolver

from runtime.artifact_store import ArtifactStore

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"
SCHEMA_STORE: Dict[str, dict] = {}


def _load_schema_store() -> None:
    if SCHEMA_STORE:
        return
    for path in SCHEMA_DIR.glob("*.schema.json"):
        schema = json.loads(path.read_text(encoding="utf-8"))
        schema_id = schema.get("$id")
        if schema_id:
            SCHEMA_STORE[schema_id] = schema
        SCHEMA_STORE[path.name] = schema


def _validator(schema_name: str, cache: Dict[str, Draft202012Validator]) -> Draft202012Validator:
    _load_schema_store()
    if schema_name in cache:
        return cache[schema_name]
    schema = SCHEMA_STORE.get(schema_name)
    if schema is None:
        raise FileNotFoundError(f"Schema {schema_name} not found in {SCHEMA_DIR}")
    base_uri = f"{SCHEMA_DIR.as_uri()}/"
    resolver = RefResolver(base_uri=base_uri, referrer=schema, store=SCHEMA_STORE)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, resolver=resolver)
    cache[schema_name] = validator
    return validator


def _load_json(store: ArtifactStore, key: str) -> dict:
    return json.loads(store.read_text(key))


def _validate_documents(store: ArtifactStore, keys: List[str], validator: Draft202012Validator, label: str) -> List[str]:
    errors: List[str] = []
    for key in keys:
        try:
            payload = _load_json(store, key)
            validator.validate(payload)
        except Exception as exc:  # noqa: BLE001 - bubble user-facing error text
            errors.append(f"{label} {key}: {exc}")
    return errors


def validate_run(store: ArtifactStore, run_id: str) -> List[str]:
    cache: Dict[str, Draft202012Validator] = {}
    errors: List[str] = []

    snapshot_keys = [key for key in store.list(f"{run_id}/snapshots") if key.endswith(".json")]
    ticket_keys = [key for key in store.list(f"{run_id}/tickets") if key.endswith(".json")]
    timeline_keys = [key for key in store.list(f"{run_id}/hosts") if key.endswith("timeline.json")]
    fleet_key = f"{run_id}/fleet_summary.json"
    manifest_key = f"{run_id}/run_manifest.json"

    if snapshot_keys:
        errors.extend(_validate_documents(store, snapshot_keys, _validator("snapshot.schema.json", cache), "snapshot"))
    if ticket_keys:
        errors.extend(_validate_documents(store, ticket_keys, _validator("ticket.schema.json", cache), "ticket"))
    if timeline_keys:
        incident_validator = _validator("incident.schema.json", cache)
        for key in timeline_keys:
            try:
                timeline = _load_json(store, key)
                for inc in timeline.get("incidents", []):
                    incident_validator.validate(inc)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"timeline {key}: {exc}")
    if store.exists(fleet_key):
        errors.extend(_validate_documents(store, [fleet_key], _validator("fleet_summary.schema.json", cache), "fleet_summary"))
    if store.exists(manifest_key):
        errors.extend(_validate_documents(store, [manifest_key], _validator("run_manifest.schema.json", cache), "run_manifest"))
    return errors


def validate_or_raise(store: ArtifactStore, run_id: str) -> None:
    errors = validate_run(store, run_id)
    if errors:
        raise ValueError("Schema validation failed: " + "; ".join(errors))


def schema_dir() -> str:
    return os.fspath(SCHEMA_DIR)


def main() -> None:
    import argparse
    from runtime.artifact_store import build_artifact_store

    parser = argparse.ArgumentParser(description="Validate artifacts against schemas.")
    parser.add_argument("--run-id", required=True, help="Run identifier to validate.")
    parser.add_argument(
        "--artifacts-root",
        default=os.environ.get("ARTIFACTS_ROOT") or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "artifacts"),
        help="Artifacts root (local path or gs://).",
    )
    args = parser.parse_args()
    store = build_artifact_store(args.artifacts_root)
    validate_or_raise(store, args.run_id)
    print(f"Validation passed for run {args.run_id}")


if __name__ == "__main__":
    main()
