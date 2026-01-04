import csv
import hashlib
import io
import json
import os
import re
import tempfile
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from runtime.artifact_store import ArtifactStore, build_artifact_store, is_gcs_uri, parse_gcs_uri
from runtime.data_janitor import clean_series, clean_value

ARTIFACT_PREFIX = "artifacts"


@dataclass
class RunStore:
    store: ArtifactStore
    run_id: str

    def artifact_key(self, filename: str) -> str:
        return f"{ARTIFACT_PREFIX}/{self.run_id}/{filename}"

    def store_key(self, filename: str) -> str:
        return f"{self.run_id}/{filename}"

    def write_json(self, filename: str, payload: dict) -> None:
        self.store.write_text(
            self.store_key(filename),
            json.dumps(payload, indent=2, ensure_ascii=True),
            content_type="application/json",
        )

    def read_json(self, filename: str) -> dict:
        return json.loads(self.store.read_text(self.store_key(filename)))

    def exists(self, filename: str) -> bool:
        return self.store.exists(self.store_key(filename))

    def uri_for(self, filename: str) -> str:
        return self.store.uri_for_key(self.store_key(filename))

# Unpivot defaults (used by future batch runner). Adjust here if needed.
DEFAULT_UNPIVOT_ID_COLUMNS = ["product_code"]
DEFAULT_UNPIVOT_VAR_NAME = "period"
DEFAULT_UNPIVOT_VALUE_NAME = "value"


@dataclass
class PuhemiesResponse:
    run_id: str
    status: str
    message: str
    question: Optional[str] = None
    choices: Optional[List[dict]] = None
    next_step: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


def _normalize_header(value: object, idx: int) -> str:
    if value is None:
        return f"unnamed_{idx}"
    text = str(value).strip().lower()
    if not text:
        return f"unnamed_{idx}"
    return text.replace(" ", "_")


def _normalize_label(value: object) -> str:
    return " ".join(str(value).strip().split()).lower()


def _numeric_like(value: object) -> bool:
    text = str(value).strip()
    if not text:
        return False
    if text.replace(".", "", 1).isdigit():
        return True
    return False


def _header_looks_like_data(headers: List[str]) -> bool:
    if not headers:
        return True
    numeric_count = sum(1 for h in headers if _numeric_like(h))
    return numeric_count >= max(1, len(headers) // 2)


def _build_run_store(run_id: str, artifacts_root: str) -> RunStore:
    store = build_artifact_store(artifacts_root)
    return RunStore(store=store, run_id=run_id)


def _store_key_from_artifact_key(artifact_key: str) -> str:
    if artifact_key.startswith(f"{ARTIFACT_PREFIX}/"):
        return artifact_key[len(f"{ARTIFACT_PREFIX}/") :]
    return artifact_key.lstrip("/")


def _hash_bytes(data: bytes) -> str:
    hasher = hashlib.sha256()
    hasher.update(data)
    return hasher.hexdigest()


def _hash_file(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _compute_structural_hash(preview_rows: List[List[object]], file_label: Optional[str]) -> str:
    preview_limit = preview_rows[:5]
    flattened = ["|".join(_normalize_label(value) for value in row) for row in preview_limit]
    digest = hashlib.sha256("\n".join(flattened).encode("utf-8"))
    return digest.hexdigest()


def _append_shadow(run_store: RunStore, event: str, details: dict) -> None:
    key = run_store.store_key("shadow.jsonl")
    entry = {
        "run_id": run_store.run_id,
        "event": event,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    entry.update(details)
    line = json.dumps(entry, ensure_ascii=True)
    if run_store.store.exists(key):
        existing = run_store.store.read_text(key)
        separator = ""
        if existing and not existing.endswith("\n"):
            separator = "\n"
        content = f"{existing}{separator}{line}\n"
    else:
        content = f"{line}\n"
    run_store.store.write_text(key, content, content_type="application/json")


RECIPE_INDEX_KEY = "recipe_store/recipe_index.json"


def _recipe_store_key(structural_hash: str) -> str:
    return f"recipe_store/{structural_hash}/manual_recipe.json"


def _recipe_artifact_key(structural_hash: str) -> str:
    return f"{ARTIFACT_PREFIX}/{_recipe_store_key(structural_hash)}"


def _load_recipe_index(store: ArtifactStore) -> Dict[str, dict]:
    if not store.exists(RECIPE_INDEX_KEY):
        return {}
    return json.loads(store.read_text(RECIPE_INDEX_KEY))


def _save_recipe_index(store: ArtifactStore, payload: Dict[str, dict]) -> None:
    store.write_text(
        RECIPE_INDEX_KEY,
        json.dumps(payload, indent=2, ensure_ascii=True),
        content_type="application/json",
    )


def _input_temp_dir(run_id: str) -> str:
    path = os.path.join(tempfile.gettempdir(), "data-agents", run_id, "input")
    os.makedirs(path, exist_ok=True)
    return path


def _download_gcs_uri(uri: str, destination: str) -> str:
    bucket, object_key = parse_gcs_uri(uri)
    try:
        from google.cloud import storage
    except Exception as exc:  # pragma: no cover - import guard
        raise ImportError("google-cloud-storage is required for gs:// inputs") from exc

    client = storage.Client()
    blob = client.bucket(bucket).blob(object_key)
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    blob.download_to_filename(destination)
    return destination


def _prepare_local_input(
    source_uri: Optional[str],
    input_artifact_key: Optional[str],
    run_store: RunStore,
) -> Optional[str]:
    if source_uri:
        if is_gcs_uri(source_uri):
            filename = os.path.basename(source_uri.rstrip("/"))
            destination = os.path.join(_input_temp_dir(run_store.run_id), filename or "input")
            return _download_gcs_uri(source_uri, destination)
        if source_uri.startswith("file://"):
            path = source_uri[len("file://") :]
            if os.path.exists(path):
                return path
        if os.path.isabs(source_uri) and os.path.exists(source_uri):
            return source_uri

    if input_artifact_key:
        store_key = _store_key_from_artifact_key(input_artifact_key)
        if run_store.store.exists(store_key):
            data = run_store.store.read_bytes(store_key)
            filename = os.path.basename(store_key)
            local_path = os.path.join(_input_temp_dir(run_store.run_id), filename or "input")
            with open(local_path, "wb") as handle:
                handle.write(data)
            return local_path

    return None


def _persist_input_copy(run_store: RunStore, local_input_path: str, fallback_name: str) -> str:
    filename = os.path.basename(local_input_path) or fallback_name
    store_key = run_store.store_key(f"input/{filename}")
    with open(local_input_path, "rb") as handle:
        data = handle.read()
    run_store.store.write_bytes(store_key, data)
    return run_store.artifact_key(f"input/{filename}")


def _lookup_recipe_for_hash(store: ArtifactStore, structural_hash: str) -> Optional[dict]:
    index = _load_recipe_index(store)
    entry = index.get(structural_hash)
    if not entry:
        return None
    recipe_key = entry.get("recipe_key")
    if not recipe_key:
        return None
    store_key = _store_key_from_artifact_key(recipe_key)
    if not store.exists(store_key):
        return None
    return json.loads(store.read_text(store_key))


def _store_recipe_for_hash(
    store: ArtifactStore,
    structural_hash: str,
    recipe: dict,
    run_id: str,
) -> str:
    recipe_store_key = _recipe_store_key(structural_hash)
    store.write_text(
        recipe_store_key,
        json.dumps(recipe, indent=2, ensure_ascii=True),
        content_type="application/json",
    )
    index = _load_recipe_index(store)
    index[structural_hash] = {
        "recipe_key": _recipe_artifact_key(structural_hash),
        "stored_at": datetime.utcnow().isoformat() + "Z",
        "source_run_id": run_id,
    }
    _save_recipe_index(store, index)
    return _recipe_artifact_key(structural_hash)


def write_human_confirmation(artifacts_root: str, run_id: str, choice_id: str, confirmed_by: str) -> None:
    run_store = _build_run_store(run_id, artifacts_root)
    payload = {
        "confirmed_header_candidate": choice_id,
        "confirmed_by": confirmed_by,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    run_store.write_json("human_confirmation.json", payload)


def _build_header_candidates(preview_rows: List[List[object]], evidence_key: str) -> List[dict]:
    candidates = []
    if not preview_rows:
        return candidates
    col_count = max(len(row) for row in preview_rows)
    for ridx, row in enumerate(preview_rows):
        normalized = [_normalize_header(value, idx) for idx, value in enumerate(row)]
        non_empty = sum(1 for value in row if str(value).strip() != "")
        fill_ratio = (non_empty / col_count) if col_count else 0.0
        data_like_penalty = 0.2 if _header_looks_like_data(normalized) else 0.0
        confidence = max(0.0, fill_ratio - data_like_penalty)
        confidence = min(confidence, 0.95)
        candidates.append(
            {
                "candidate_id": f"row_{ridx}",
                "header_rows": [ridx],
                "merge_strategy": "single_row",
                "normalized_headers": normalized,
                "confidence": round(confidence, 3),
                "evidence_keys": [evidence_key],
            }
        )
    return candidates


def _select_candidate(candidates: List[dict]) -> Optional[dict]:
    if not candidates:
        return None
    return max(candidates, key=lambda c: c.get("confidence", 0.0))


def _infer_dtype(values: List[object]) -> str:
    cleaned = [v for v in values if str(v).strip() != ""]
    if not cleaned:
        return "string"
    if all(_numeric_like(v) for v in cleaned):
        return "number"
    return "string"


def _clean_number_value(value: object) -> str:
    text = str(value).strip()
    if not text:
        return ""
    normalized = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", normalized)
    return match.group(0) if match else ""


def _clean_date_value(value: object) -> str:
    text = str(value).strip()
    if not text:
        return ""
    try:
        parsed = pd.to_datetime(value)
        return parsed.date().isoformat()
    except Exception:
        return text


def _apply_type_enforcement(rows: List[List[object]], types: List[str]) -> List[List[object]]:
    cleaned_rows = []
    for row in rows:
        cleaned = []
        for idx, value in enumerate(row):
            dtype = types[idx] if idx < len(types) else "string"
            if dtype == "number":
                cleaned.append(_clean_number_value(value))
            elif dtype == "date":
                cleaned.append(_clean_date_value(value))
            else:
                cleaned.append(value)
        cleaned_rows.append(cleaned)
    return cleaned_rows


def _write_schema_and_output(
    run_store: RunStore,
    data_rows: List[List[object]],
    headers: List[str],
    adapter_spec: Optional[Dict[str, object]] = None,
) -> None:
    rows = data_rows or []

    if adapter_spec:
        canonical_fields = adapter_spec.get("canonical_fields") or []
        field_map = adapter_spec.get("field_map") or {}
        types = adapter_spec.get("types") or {}
        required_fields = set(adapter_spec.get("required_fields") or [])
        output_headers = [field for field in canonical_fields if field in field_map]
        if not output_headers:
            output_headers = list(field_map.keys())
        header_index = {name: idx for idx, name in enumerate(headers)}
        mapped_rows: List[List[object]] = []
        for row in rows:
            mapped_row = []
            for canonical in output_headers:
                source = field_map.get(canonical)
                value = ""
                if source in header_index and header_index[source] < len(row):
                    value = row[header_index[source]]
                mapped_row.append(value)
            mapped_rows.append(mapped_row)
        types_by_header = [types.get(header, "string") for header in output_headers]
        rows = _apply_type_enforcement(mapped_rows, types_by_header)
        headers = output_headers
        schema_fields = []
        for canonical in headers:
            source = field_map.get(canonical, "")
            dtype = types.get(canonical, "string")
            schema_fields.append(
                {
                    "source": source,
                    "canonical": canonical,
                    "dtype": dtype,
                    "required": canonical in required_fields,
                }
            )
        schema_layer = "adapter"
        evidence_keys = adapter_spec.get("evidence_keys") or [run_store.artifact_key("header_spec.json")]
    else:
        schema_fields = []
        columns = list(zip(*rows)) if rows else [[] for _ in headers]
        for idx, header in enumerate(headers):
            values = list(columns[idx]) if idx < len(columns) else []
            dtype = _infer_dtype(values)
            required = all(str(v).strip() != "" for v in values) if values else False
            schema_fields.append(
                {
                    "source": header,
                    "canonical": header,
                    "dtype": dtype,
                    "required": required,
                }
            )
        schema_layer = "core"
        evidence_keys = [run_store.artifact_key("header_spec.json")]

    schema_spec = {
        "run_id": run_store.run_id,
        "artifact_key": run_store.artifact_key("schema_spec.json"),
        "schema_layer": schema_layer,
        "schema_spec": {"fields": schema_fields, "unmapped_columns": []},
        "confidence": 0.7,
        "alternatives": [],
        "evidence_keys": evidence_keys,
        "refusal_reason": None,
    }
    run_store.write_json("schema_spec.json", schema_spec)

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(rows)
    run_store.store.write_bytes(
        run_store.store_key("output/clean.csv"),
        csv_buffer.getvalue().encode("utf-8"),
        content_type="text/csv",
    )

    saved_artifacts = [run_store.artifact_key("output/clean.csv")]
    save_manifest = {
        "run_id": run_store.run_id,
        "artifact_key": run_store.artifact_key("save_manifest.json"),
        "saved_files": saved_artifacts,
        "saved_uris": [run_store.uri_for("output/clean.csv")],
        "report_paths": [],
        "confidence": 0.7,
        "alternatives": [],
        "evidence_keys": [schema_spec["artifact_key"]],
        "refusal_reason": None,
    }
    run_store.write_json("save_manifest.json", save_manifest)


def _read_preview_rows(input_path: str, max_rows: int = 5) -> Tuple[List[List[object]], Optional[str]]:
    extension = os.path.splitext(input_path)[1].lower()
    if extension in [".xlsx", ".xls"]:
        excel = pd.ExcelFile(input_path)
        sheet = excel.sheet_names[0]
        df = pd.read_excel(input_path, sheet_name=sheet, header=None, dtype=object)
        preview_rows = df.head(max_rows).fillna("").values.tolist()
        return preview_rows, sheet
    if extension in [".csv"]:
        preview_rows = []
        with open(input_path, "r", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            for idx, row in enumerate(reader):
                preview_rows.append(row)
                if idx + 1 >= max_rows:
                    break
        return preview_rows, None
    raise ValueError(f"Unsupported input type: {input_path}")


def _read_sheet_dataframe(input_path: str, sheet_name: Optional[str]) -> pd.DataFrame:
    extension = os.path.splitext(input_path)[1].lower()
    if extension in [".xlsx", ".xls"]:
        df = pd.read_excel(input_path, sheet_name=sheet_name or 0, header=None, dtype=object)
        return df.fillna("")
    if extension in [".csv"]:
        df = pd.read_csv(input_path, header=None, dtype=object, keep_default_na=False)
        return df.fillna("")
    raise ValueError(f"Unsupported input type: {input_path}")


def _read_data_rows(input_path: str, header_row: int, sheet_name: Optional[str]) -> List[List[object]]:
    extension = os.path.splitext(input_path)[1].lower()
    if extension in [".xlsx", ".xls"]:
        df = pd.read_excel(input_path, sheet_name=sheet_name or 0, header=None, dtype=object)
        return df.iloc[header_row + 1 :].fillna("").values.tolist()
    if extension in [".csv"]:
        rows = []
        with open(input_path, "r", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            for idx, row in enumerate(reader):
                if idx > header_row:
                    rows.append(row)
        return rows
    raise ValueError(f"Unsupported input type: {input_path}")


def _read_header_row(input_path: str, header_row: int, sheet_name: Optional[str]) -> List[object]:
    extension = os.path.splitext(input_path)[1].lower()
    if extension in [".xlsx", ".xls"]:
        df = pd.read_excel(input_path, sheet_name=sheet_name or 0, header=None, dtype=object)
        if header_row < 0 or header_row >= len(df):
            return []
        return df.iloc[header_row].fillna("").tolist()
    if extension in [".csv"]:
        with open(input_path, "r", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            for idx, row in enumerate(reader):
                if idx == header_row:
                    return row
        return []
    raise ValueError(f"Unsupported input type: {input_path}")


def _apply_table_region(
    headers: List[str],
    data_rows: List[List[object]],
    header_row: int,
    table_region: Optional[Dict[str, object]],
) -> Tuple[List[str], List[List[object]]]:
    if not table_region:
        return headers, data_rows

    start_row = table_region.get("start_row")
    end_row = table_region.get("end_row")
    include_columns = table_region.get("include_columns") or []
    exclude_columns = table_region.get("exclude_columns") or []

    data_start_index = header_row + 1
    start_offset = 0
    if start_row is not None:
        start_offset = max(0, int(start_row) - data_start_index)
    end_offset = None
    if end_row is not None:
        end_offset = max(0, int(end_row) - data_start_index)
    if end_offset is not None:
        data_rows = data_rows[start_offset : end_offset + 1]
    else:
        data_rows = data_rows[start_offset:]

    if include_columns:
        keep = [idx for idx, name in enumerate(headers) if name in include_columns]
    elif exclude_columns:
        keep = [idx for idx, name in enumerate(headers) if name not in exclude_columns]
    else:
        keep = list(range(len(headers)))

    headers = [headers[idx] for idx in keep]
    filtered_rows = []
    for row in data_rows:
        filtered_rows.append([row[idx] if idx < len(row) else "" for idx in keep])

    return headers, filtered_rows


def _apply_header_override(
    run_store: RunStore,
    override: Dict[str, object],
    evidence: Dict[str, object],
    input_path: Optional[str],
) -> Tuple[List[str], int]:
    sheet_name = override.get("sheet_name") or evidence.get("sheet_name")
    header_row_index = int(override.get("header_row_index", 0))
    raw_headers: List[object]
    if input_path:
        raw_headers = _read_header_row(input_path, header_row_index, sheet_name)
    else:
        preview_rows = evidence.get("preview_rows", [])
        raw_headers = preview_rows[header_row_index] if header_row_index < len(preview_rows) else []

    normalized_headers = [_normalize_header(value, idx) for idx, value in enumerate(raw_headers)]
    edited_headers = override.get("edited_headers") or {}
    final_headers = [edited_headers.get(header, header) for header in normalized_headers]

    header_spec = {
        "run_id": run_store.run_id,
        "artifact_key": run_store.artifact_key("header_spec.json"),
        "selected_candidate_id": "manual",
        "candidates": [
            {
                "candidate_id": "manual",
                "header_rows": override.get("header_rows", [header_row_index]),
                "merge_strategy": override.get("merge_strategy", "single"),
                "normalized_headers": final_headers,
                "confidence": 0.9,
                "evidence_keys": [evidence.get("artifact_key")],
            }
        ],
        "needs_human_confirmation": False,
        "alternatives": [],
        "refusal_reason": None,
    }
    run_store.write_json("header_spec.json", header_spec)
    _append_shadow(run_store, "header_override_applied", {"header_row_index": header_row_index, "sheet_name": sheet_name})
    return final_headers, header_row_index


def _merge_metadata_into_rows(
    rows: List[List[object]],
    column_fields: List[dict],
    metadata: Dict[str, object],
    merge_fields: List[str],
    metadata_fields: List[dict],
) -> Tuple[List[List[object]], List[dict]]:
    if not merge_fields:
        return rows, column_fields
    metadata_types = {
        field["target"]: field.get("data_type") or "string" for field in metadata_fields if field.get("target")
    }
    merged_rows = []
    for row in rows:
        merged_values = []
        for field in merge_fields:
            value = metadata.get(field, "")
            dtype = metadata_types.get(field, "string")
            merged_values.append(clean_value(value, dtype))
        merged_rows.append(row + merged_values)
    merged_fields = column_fields + [
        {
            "target": field,
            "data_type": metadata_types.get(field, "string"),
            "column_name": field,
            "column_index": None,
        }
        for field in merge_fields
    ]
    return merged_rows, merged_fields


def _parse_metadata_pointer(pointer: object) -> Optional[Tuple[int, int]]:
    if not isinstance(pointer, dict):
        return None
    if "row" not in pointer or "col" not in pointer:
        return None
    try:
        return int(pointer["row"]), int(pointer["col"])
    except (TypeError, ValueError):
        return None


def _parse_column_pointer(pointer: object) -> Optional[Dict[str, object]]:
    if isinstance(pointer, str):
        return {"column_name": pointer}
    if isinstance(pointer, int):
        return {"column_index": pointer}
    if isinstance(pointer, dict):
        if "column" in pointer:
            return {"column_name": pointer["column"]}
        if "header" in pointer:
            return {"column_name": pointer["header"]}
        if "column_name" in pointer:
            return {"column_name": pointer["column_name"]}
        if "col" in pointer and "row" not in pointer:
            try:
                return {"column_index": int(pointer["col"])}
            except (TypeError, ValueError):
                return None
    return None


def _collect_manual_recipe_fields(fields: List[dict]) -> Tuple[List[dict], List[dict], List[str]]:
    metadata_fields: List[dict] = []
    column_fields: List[dict] = []
    warnings: List[str] = []

    for field in fields:
        target = field.get("target") or field.get("target_name")
        if not target:
            warnings.append("missing_target")
            continue

        source_pointer = field.get("source_pointer")
        source_type = field.get("source_type")
        data_type = field.get("data_type")
        if source_pointer is None:
            warnings.append(f"missing_source_pointer:{target}")
            continue

        if source_type == "metadata":
            coords = _parse_metadata_pointer(source_pointer)
            if not coords:
                warnings.append(f"invalid_metadata_pointer:{target}")
                continue
            row, col = coords
            metadata_fields.append({"target": target, "row": row, "col": col, "data_type": data_type})
            continue

        if source_type == "column":
            column_pointer = _parse_column_pointer(source_pointer)
            if not column_pointer:
                warnings.append(f"invalid_column_pointer:{target}")
                continue
            column_pointer.update({"target": target, "data_type": data_type})
            column_fields.append(column_pointer)
            continue

        coords = _parse_metadata_pointer(source_pointer)
        if coords:
            row, col = coords
            metadata_fields.append({"target": target, "row": row, "col": col, "data_type": data_type})
            continue

        column_pointer = _parse_column_pointer(source_pointer)
        if column_pointer:
            column_pointer.update({"target": target, "data_type": data_type})
            column_fields.append(column_pointer)
            continue

        warnings.append(f"unsupported_source_pointer:{target}")

    return metadata_fields, column_fields, warnings


def _resolve_header_row(recipe: dict, df: pd.DataFrame, column_fields: List[dict]) -> int:
    explicit_row = recipe.get("header_row_index")
    if explicit_row is None:
        explicit_row = recipe.get("header_row")
    if explicit_row is None:
        explicit_row = recipe.get("header_row_idx")
    if explicit_row is not None:
        try:
            return int(explicit_row)
        except (TypeError, ValueError):
            return 0

    header_names = [
        field.get("column_name")
        for field in column_fields
        if field.get("column_name") is not None
    ]
    if not header_names:
        return 0

    normalized_headers = {_normalize_label(name) for name in header_names if str(name).strip()}
    if not normalized_headers:
        return 0

    max_rows = min(len(df), 50)
    best_row = 0
    best_match = -1
    for idx in range(max_rows):
        row_values = df.iloc[idx].tolist()
        normalized_row = {_normalize_label(value) for value in row_values if str(value).strip()}
        match_count = len(normalized_headers.intersection(normalized_row))
        if match_count > best_match:
            best_match = match_count
            best_row = idx
    return best_row


def _write_manual_recipe_outputs(
    run_store: RunStore,
    column_fields: List[dict],
    data_rows: List[List[object]],
    metadata: Dict[str, object],
) -> None:
    column_targets = [field["target"] for field in column_fields]
    clean_data_buffer = io.StringIO()
    writer = csv.writer(clean_data_buffer)
    writer.writerow(column_targets)
    writer.writerows(data_rows)
    run_store.store.write_bytes(
        run_store.store_key("output/clean_data.csv"),
        clean_data_buffer.getvalue().encode("utf-8"),
        content_type="text/csv",
    )

    run_store.write_json("output/extracted_metadata.json", metadata)

    schema_fields = []
    if column_fields:
        columns = list(zip(*data_rows)) if data_rows else [[] for _ in column_fields]
        for idx, field in enumerate(column_fields):
            values = list(columns[idx]) if idx < len(columns) else []
            dtype = field.get("data_type") or _infer_dtype(values)
            required = all(str(v).strip() != "" for v in values) if values else False
            schema_fields.append(
                {
                    "source": field.get("column_name") or f"col_{field.get('column_index', idx)}",
                    "canonical": field["target"],
                    "dtype": dtype,
                    "required": required,
                }
            )

    schema_spec = {
        "run_id": run_store.run_id,
        "artifact_key": run_store.artifact_key("schema_spec.json"),
        "schema_layer": "manual_recipe",
        "schema_spec": {"fields": schema_fields, "unmapped_columns": []},
        "confidence": 0.9,
        "alternatives": [],
        "evidence_keys": [run_store.artifact_key("manual_recipe.json")],
        "refusal_reason": None,
    }
    run_store.write_json("schema_spec.json", schema_spec)

    save_manifest = {
        "run_id": run_store.run_id,
        "artifact_key": run_store.artifact_key("save_manifest.json"),
        "saved_files": [
            run_store.artifact_key("output/clean_data.csv"),
            run_store.artifact_key("output/extracted_metadata.json"),
        ],
        "saved_uris": [
            run_store.uri_for("output/clean_data.csv"),
            run_store.uri_for("output/extracted_metadata.json"),
        ],
        "report_paths": [],
        "confidence": 0.9,
        "alternatives": [],
        "evidence_keys": [schema_spec["artifact_key"]],
        "refusal_reason": None,
    }
    run_store.write_json("save_manifest.json", save_manifest)


def _apply_manual_recipe(
    run_store: RunStore,
    recipe: dict,
    evidence: dict,
    input_path: Optional[str],
) -> None:
    if not input_path:
        raise ValueError("Manual recipe requires a readable input file.")

    fields = recipe.get("fields") or []
    metadata_fields, column_fields, warnings = _collect_manual_recipe_fields(fields)
    if not metadata_fields and not column_fields:
        raise ValueError("Manual recipe has no usable fields.")
    if not column_fields:
        raise ValueError("Manual recipe must include at least one column field to build a table.")

    df = _read_sheet_dataframe(input_path, evidence.get("sheet_name"))
    header_row = _resolve_header_row(recipe, df, column_fields)

    header_values = []
    if 0 <= header_row < len(df):
        header_values = df.iloc[header_row].tolist()
    header_index: Dict[str, int] = {}
    for idx, value in enumerate(header_values):
        key = _normalize_label(value)
        if key and key not in header_index:
            header_index[key] = idx

    resolved_columns = []
    for field in column_fields:
        column_index = field.get("column_index")
        if column_index is None:
            column_name = field.get("column_name", "")
            column_index = header_index.get(_normalize_label(column_name))
        resolved_columns.append(
            {
                "target": field["target"],
                "data_type": field.get("data_type"),
                "column_name": field.get("column_name"),
                "column_index": column_index,
            }
        )

    data_rows = []
    if header_row + 1 < len(df):
        data_rows = df.iloc[header_row + 1 :].values.tolist()

    output_rows = []
    for row in data_rows:
        output_row = []
        for field in resolved_columns:
            idx = field.get("column_index")
            if idx is None or idx >= len(row):
                output_row.append("")
            else:
                output_row.append(row[idx])
        output_rows.append(output_row)

    if resolved_columns:
        targets = [field["target"] for field in resolved_columns]
        df_out = pd.DataFrame(output_rows, columns=targets)
        for field in resolved_columns:
            dtype = field.get("data_type") or "string"
            df_out[field["target"]] = clean_series(df_out[field["target"]], dtype)
        output_rows = df_out.values.tolist()

    extracted_metadata: Dict[str, object] = {}
    for field in metadata_fields:
        row = field["row"]
        col = field["col"]
        if 0 <= row < len(df) and 0 <= col < len(df.columns):
            value = df.iat[row, col]
        else:
            value = ""
        extracted_metadata[field["target"]] = value

    merge_fields = recipe.get("merge_metadata_fields") or []
    if recipe.get("merge_metadata") and not merge_fields:
        merge_fields = [field["target"] for field in metadata_fields]
    output_rows, merged_columns = _merge_metadata_into_rows(
        output_rows,
        resolved_columns,
        extracted_metadata,
        merge_fields,
        metadata_fields,
    )

    _write_manual_recipe_outputs(run_store, merged_columns, output_rows, extracted_metadata)
    _append_shadow(
        run_store,
        "manual_recipe_applied",
        {
            "header_row": header_row,
            "metadata_fields": len(metadata_fields),
            "column_fields": len(merged_columns),
            "warnings": warnings,
        },
    )

    structural_hash = evidence.get("structural_hash")
    if structural_hash:
        _store_recipe_for_hash(run_store.store, structural_hash, recipe, run_store.run_id)


def puhemies_orchestrate(
    run_id: str,
    preview_rows: List[List[object]],
    artifacts_root: str,
    file_path: Optional[str] = None,
    sheet_name: Optional[str] = None,
    source_uri: Optional[str] = None,
    input_artifact_key: Optional[str] = None,
    structural_hash: Optional[str] = None,
    file_hash: Optional[str] = None,
) -> PuhemiesResponse:
    run_store = _build_run_store(run_id, artifacts_root)
    evidence = {
        "run_id": run_id,
        "artifact_key": run_store.artifact_key("evidence_packet.json"),
        "preview_rows": preview_rows,
        "notes": "synthetic preview rows",
    }
    source_label = file_path or source_uri
    if source_uri or file_path:
        evidence["source_uri"] = source_uri or f"file://{os.path.abspath(file_path)}"
    if input_artifact_key:
        evidence["input_artifact_key"] = input_artifact_key
    if file_hash:
        evidence["file_hash"] = file_hash
    if structural_hash:
        evidence["structural_hash"] = structural_hash
    if sheet_name:
        evidence["sheet_name"] = sheet_name
    filename = os.path.basename(source_label or "")
    if filename:
        evidence["input_filename"] = filename
    if "structural_hash" not in evidence:
        evidence["structural_hash"] = _compute_structural_hash(preview_rows, source_label)
    run_store.write_json("evidence_packet.json", evidence)

    candidates = _build_header_candidates(preview_rows, evidence["artifact_key"])
    selected = _select_candidate(candidates)
    selected_id = selected["candidate_id"] if selected else ""
    header_spec = {
        "run_id": run_id,
        "artifact_key": run_store.artifact_key("header_spec.json"),
        "selected_candidate_id": selected_id,
        "candidates": candidates,
        "needs_human_confirmation": False,
        "alternatives": [c["candidate_id"] for c in candidates if c.get("candidate_id") != selected_id],
        "refusal_reason": None,
    }

    if selected and _header_looks_like_data(selected["normalized_headers"]):
        header_spec["needs_human_confirmation"] = True
        run_store.write_json("header_spec.json", header_spec)
        _append_shadow(run_store, "stop_due_to_ambiguous_headers", {"selected_candidate_id": selected_id})
        return PuhemiesResponse(
            run_id=run_id,
            status="needs_human_confirmation",
            message="Header selection is ambiguous and looks like data.",
            question="Which header candidate should be used?",
            choices=[
                {
                    "id": c["candidate_id"],
                    "normalized_headers": c["normalized_headers"],
                    "confidence": c["confidence"],
                }
                for c in candidates
            ],
            next_step="provide_confirmed_header_candidate",
        )

    run_store.write_json("header_spec.json", header_spec)
    _append_shadow(run_store, "header_selection_ok", {"selected_candidate_id": selected_id})

    return PuhemiesResponse(
        run_id=run_id,
        status="ok",
        message="Header selection accepted.",
        next_step="continue_to_schema",
    )


def puhemies_continue(run_id: str, artifacts_root: str) -> PuhemiesResponse:
    run_store = _build_run_store(run_id, artifacts_root)
    evidence = run_store.read_json("evidence_packet.json")

    input_path = _prepare_local_input(evidence.get("source_uri"), evidence.get("input_artifact_key"), run_store)
    expected_hash = evidence.get("file_hash")
    if expected_hash and input_path:
        current_hash = _hash_file(input_path)
        if current_hash != expected_hash:
            _append_shadow(
                run_store,
                "resume_guard_file_changed",
                {"expected_hash": expected_hash, "current_hash": current_hash},
            )
            return PuhemiesResponse(
                run_id=run_id,
                status="needs_human_confirmation",
                message="Input file has changed since the run started.",
                question="Please re-run with the updated file.",
                next_step="rerun_required",
            )
    elif expected_hash and not input_path:
        _append_shadow(
            run_store,
            "resume_guard_source_missing",
            {"expected_hash": expected_hash, "source_uri": evidence.get("source_uri")},
        )

    if run_store.exists("manual_recipe.json"):
        manual_recipe = run_store.read_json("manual_recipe.json")
        try:
            _apply_manual_recipe(run_store, manual_recipe, evidence, input_path)
        except ValueError as exc:
            return PuhemiesResponse(
                run_id=run_id,
                status="needs_human_confirmation",
                message=str(exc),
                question="Please fix manual_recipe.json and retry.",
                next_step="fix_manual_recipe",
            )
        return PuhemiesResponse(
            run_id=run_id,
            status="ok",
            message="Manual recipe applied and outputs saved.",
            next_step="review_artifacts",
        )

    if run_store.exists("header_override.json"):
        override = run_store.read_json("header_override.json")
        headers, header_row = _apply_header_override(run_store, override, evidence, input_path)
    else:
        if not run_store.exists("human_confirmation.json"):
            return PuhemiesResponse(
                run_id=run_id,
                status="needs_human_confirmation",
                message="Missing human confirmation.",
                question="Provide confirmed header candidate id.",
                next_step="write_human_confirmation",
            )

        header_spec = run_store.read_json("header_spec.json")
        confirmation = run_store.read_json("human_confirmation.json")

        confirmed_id = confirmation.get("confirmed_header_candidate")
        selected = next(
            (c for c in header_spec.get("candidates", []) if c.get("candidate_id") == confirmed_id), None
        )
        if not selected:
            return PuhemiesResponse(
                run_id=run_id,
                status="needs_human_confirmation",
                message="Confirmed header candidate not found.",
                question="Provide a valid header candidate id.",
                next_step="write_human_confirmation",
            )

        _append_shadow(run_store, "human_confirmation_received", {"confirmed_header_candidate": confirmed_id})

        headers = selected["normalized_headers"]
        header_row = selected["header_rows"][0]

    if input_path:
        data_rows = _read_data_rows(
            input_path,
            header_row,
            evidence.get("sheet_name"),
        )
    else:
        data_rows = evidence.get("preview_rows", [])[header_row + 1 :]
    adapter_spec = None
    if run_store.exists("adapter_schema_spec.json"):
        adapter_spec = run_store.read_json("adapter_schema_spec.json")

    table_region = None
    if run_store.exists("table_region.json"):
        table_region = run_store.read_json("table_region.json")

    headers, data_rows = _apply_table_region(headers, data_rows, header_row, table_region)
    _write_schema_and_output(run_store, data_rows, headers, adapter_spec=adapter_spec)

    return PuhemiesResponse(
        run_id=run_id,
        status="ok",
        message="Schema created and output saved.",
        next_step="review_artifacts",
    )


def puhemies_run_from_file(run_id: str, input_path: str, artifacts_root: str) -> PuhemiesResponse:
    run_store = _build_run_store(run_id, artifacts_root)
    if is_gcs_uri(input_path):
        filename = os.path.basename(input_path.rstrip("/")) or "input"
        local_input = _download_gcs_uri(input_path, os.path.join(_input_temp_dir(run_id), filename))
        source_uri = input_path
    else:
        local_input = os.path.abspath(input_path)
        source_uri = f"file://{local_input}"

    preview_rows, sheet_name = _read_preview_rows(local_input)
    file_hash = _hash_file(local_input)
    structural_hash = _compute_structural_hash(preview_rows, os.path.basename(local_input))
    input_artifact_key = _persist_input_copy(run_store, local_input, os.path.basename(local_input) or "input")

    response = puhemies_orchestrate(
        run_id,
        preview_rows,
        artifacts_root,
        file_path=local_input,
        sheet_name=sheet_name,
        source_uri=source_uri,
        input_artifact_key=input_artifact_key,
        structural_hash=structural_hash,
        file_hash=file_hash,
    )
    recalled = _lookup_recipe_for_hash(run_store.store, structural_hash)
    if recalled:
        run_store.write_json("manual_recipe.json", recalled)
        _append_shadow(run_store, "manual_recipe_recalled", {"structural_hash": structural_hash})
        return puhemies_continue(run_id, artifacts_root)
    return response
