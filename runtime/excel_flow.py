import csv
import hashlib
import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from runtime.data_janitor import clean_series, clean_value

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


def _artifact_dir(artifacts_root: str, run_id: str) -> str:
    return os.path.join(artifacts_root, run_id)


def _write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)


def _append_shadow(artifacts_root: str, run_id: str, event: str, details: dict) -> None:
    path = os.path.join(_artifact_dir(artifacts_root, run_id), "shadow.jsonl")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    entry = {
        "run_id": run_id,
        "event": event,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    entry.update(details)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=True) + "\n")


def _hash_file(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _compute_structural_hash(preview_rows: List[List[object]], file_path: Optional[str]) -> str:
    preview_limit = preview_rows[:5]
    flattened = ["|".join(_normalize_label(value) for value in row) for row in preview_limit]
    if file_path:
        flattened.append(os.path.basename(file_path).lower())
    digest = hashlib.sha256("\n".join(flattened).encode("utf-8"))
    return digest.hexdigest()


def _recipe_index_path(artifacts_root: str) -> str:
    return os.path.join(artifacts_root, "recipe_store", "recipe_index.json")


def _load_recipe_index(artifacts_root: str) -> Dict[str, dict]:
    path = _recipe_index_path(artifacts_root)
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _save_recipe_index(artifacts_root: str, payload: Dict[str, dict]) -> None:
    path = _recipe_index_path(artifacts_root)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)


def _lookup_recipe_for_hash(artifacts_root: str, structural_hash: str) -> Optional[dict]:
    index = _load_recipe_index(artifacts_root)
    entry = index.get(structural_hash)
    if not entry:
        return None
    recipe_path = entry.get("recipe_path")
    if not recipe_path or not os.path.exists(recipe_path):
        return None
    with open(recipe_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _store_recipe_for_hash(
    artifacts_root: str,
    structural_hash: str,
    recipe: dict,
    run_id: str,
) -> str:
    store_dir = os.path.join(artifacts_root, "recipe_store", structural_hash)
    os.makedirs(store_dir, exist_ok=True)
    recipe_path = os.path.join(store_dir, "manual_recipe.json")
    _write_json(recipe_path, recipe)
    index = _load_recipe_index(artifacts_root)
    index[structural_hash] = {
        "recipe_path": recipe_path,
        "stored_at": datetime.utcnow().isoformat() + "Z",
        "source_run_id": run_id,
    }
    _save_recipe_index(artifacts_root, index)
    return recipe_path


def write_human_confirmation(artifacts_root: str, run_id: str, choice_id: str, confirmed_by: str) -> None:
    run_dir = _artifact_dir(artifacts_root, run_id)
    os.makedirs(run_dir, exist_ok=True)
    payload = {
        "confirmed_header_candidate": choice_id,
        "confirmed_by": confirmed_by,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    _write_json(os.path.join(run_dir, "human_confirmation.json"), payload)


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
    run_id: str,
    artifacts_root: str,
    data_rows: List[List[object]],
    headers: List[str],
    adapter_spec: Optional[Dict[str, object]] = None,
) -> None:
    if not data_rows:
        rows = []
    else:
        rows = data_rows

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
        evidence_keys = adapter_spec.get("evidence_keys") or [f"artifacts/{run_id}/header_spec.json"]
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
        evidence_keys = [f"artifacts/{run_id}/header_spec.json"]

    schema_spec = {
        "run_id": run_id,
        "artifact_key": f"artifacts/{run_id}/schema_spec.json",
        "schema_layer": schema_layer,
        "schema_spec": {"fields": schema_fields, "unmapped_columns": []},
        "confidence": 0.7,
        "alternatives": [],
        "evidence_keys": evidence_keys,
        "refusal_reason": None,
    }
    _write_json(os.path.join(_artifact_dir(artifacts_root, run_id), "schema_spec.json"), schema_spec)

    output_dir = os.path.join(_artifact_dir(artifacts_root, run_id), "output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "clean.csv")
    with open(output_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)

    save_manifest = {
        "run_id": run_id,
        "artifact_key": f"artifacts/{run_id}/save_manifest.json",
        "saved_files": [output_path],
        "report_paths": [],
        "confidence": 0.7,
        "alternatives": [],
        "evidence_keys": [schema_spec["artifact_key"]],
        "refusal_reason": None,
    }
    _write_json(os.path.join(_artifact_dir(artifacts_root, run_id), "save_manifest.json"), save_manifest)


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
    run_id: str,
    artifacts_root: str,
    override: Dict[str, object],
    evidence: Dict[str, object],
) -> Tuple[List[str], int]:
    sheet_name = override.get("sheet_name") or evidence.get("sheet_name")
    header_row_index = int(override.get("header_row_index", 0))
    raw_headers: List[object]
    if evidence.get("file_path"):
        raw_headers = _read_header_row(evidence["file_path"], header_row_index, sheet_name)
    else:
        preview_rows = evidence.get("preview_rows", [])
        raw_headers = preview_rows[header_row_index] if header_row_index < len(preview_rows) else []

    normalized_headers = [_normalize_header(value, idx) for idx, value in enumerate(raw_headers)]
    edited_headers = override.get("edited_headers") or {}
    final_headers = [edited_headers.get(header, header) for header in normalized_headers]

    header_spec = {
        "run_id": run_id,
        "artifact_key": f"artifacts/{run_id}/header_spec.json",
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
    _write_json(os.path.join(_artifact_dir(artifacts_root, run_id), "header_spec.json"), header_spec)
    _append_shadow(
        artifacts_root,
        run_id,
        "header_override_applied",
        {"header_row_index": header_row_index, "sheet_name": sheet_name},
    )
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
    run_id: str,
    artifacts_root: str,
    column_fields: List[dict],
    data_rows: List[List[object]],
    metadata: Dict[str, object],
) -> None:
    run_dir = _artifact_dir(artifacts_root, run_id)
    output_dir = os.path.join(run_dir, "output")
    os.makedirs(output_dir, exist_ok=True)

    column_targets = [field["target"] for field in column_fields]
    clean_data_path = os.path.join(output_dir, "clean_data.csv")
    with open(clean_data_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(column_targets)
        writer.writerows(data_rows)

    metadata_path = os.path.join(output_dir, "extracted_metadata.json")
    _write_json(metadata_path, metadata)

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
        "run_id": run_id,
        "artifact_key": f"artifacts/{run_id}/schema_spec.json",
        "schema_layer": "manual_recipe",
        "schema_spec": {"fields": schema_fields, "unmapped_columns": []},
        "confidence": 0.9,
        "alternatives": [],
        "evidence_keys": [f"artifacts/{run_id}/manual_recipe.json"],
        "refusal_reason": None,
    }
    _write_json(os.path.join(run_dir, "schema_spec.json"), schema_spec)

    save_manifest = {
        "run_id": run_id,
        "artifact_key": f"artifacts/{run_id}/save_manifest.json",
        "saved_files": [clean_data_path, metadata_path],
        "report_paths": [],
        "confidence": 0.9,
        "alternatives": [],
        "evidence_keys": [schema_spec["artifact_key"]],
        "refusal_reason": None,
    }
    _write_json(os.path.join(run_dir, "save_manifest.json"), save_manifest)


def _apply_manual_recipe(
    run_id: str,
    artifacts_root: str,
    recipe: dict,
    evidence: dict,
) -> None:
    if not evidence.get("file_path"):
        raise ValueError("Manual recipe requires a file path in evidence.")

    fields = recipe.get("fields") or []
    metadata_fields, column_fields, warnings = _collect_manual_recipe_fields(fields)
    if not metadata_fields and not column_fields:
        raise ValueError("Manual recipe has no usable fields.")
    if not column_fields:
        raise ValueError("Manual recipe must include at least one column field to build a table.")

    df = _read_sheet_dataframe(evidence["file_path"], evidence.get("sheet_name"))
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

    _write_manual_recipe_outputs(
        run_id,
        artifacts_root,
        merged_columns,
        output_rows,
        extracted_metadata,
    )
    _append_shadow(
        artifacts_root,
        run_id,
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
        _store_recipe_for_hash(artifacts_root, structural_hash, recipe, run_id)


def puhemies_orchestrate(
    run_id: str,
    preview_rows: List[List[object]],
    artifacts_root: str,
    file_path: Optional[str] = None,
    sheet_name: Optional[str] = None,
) -> PuhemiesResponse:
    evidence = {
        "run_id": run_id,
        "artifact_key": f"artifacts/{run_id}/evidence_packet.json",
        "preview_rows": preview_rows,
        "notes": "synthetic preview rows",
    }
    if file_path:
        evidence["file_path"] = file_path
        evidence["file_hash"] = _hash_file(file_path)
        evidence["structural_hash"] = _compute_structural_hash(preview_rows, file_path)
    if sheet_name:
        evidence["sheet_name"] = sheet_name
    _write_json(os.path.join(_artifact_dir(artifacts_root, run_id), "evidence_packet.json"), evidence)

    candidates = _build_header_candidates(preview_rows, evidence["artifact_key"])
    selected = _select_candidate(candidates)
    selected_id = selected["candidate_id"] if selected else ""
    header_spec = {
        "run_id": run_id,
        "artifact_key": f"artifacts/{run_id}/header_spec.json",
        "selected_candidate_id": selected_id,
        "candidates": candidates,
        "needs_human_confirmation": False,
        "alternatives": [c["candidate_id"] for c in candidates if c.get("candidate_id") != selected_id],
        "refusal_reason": None,
    }

    if selected and _header_looks_like_data(selected["normalized_headers"]):
        header_spec["needs_human_confirmation"] = True
        _write_json(os.path.join(_artifact_dir(artifacts_root, run_id), "header_spec.json"), header_spec)
        _append_shadow(
            artifacts_root,
            run_id,
            "stop_due_to_ambiguous_headers",
            {"selected_candidate_id": selected_id},
        )
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

    _write_json(os.path.join(_artifact_dir(artifacts_root, run_id), "header_spec.json"), header_spec)
    _append_shadow(artifacts_root, run_id, "header_selection_ok", {"selected_candidate_id": selected_id})

    return PuhemiesResponse(
        run_id=run_id,
        status="ok",
        message="Header selection accepted.",
        next_step="continue_to_schema",
    )


def puhemies_continue(run_id: str, artifacts_root: str) -> PuhemiesResponse:
    run_dir = _artifact_dir(artifacts_root, run_id)
    confirmation_path = os.path.join(run_dir, "human_confirmation.json")
    header_path = os.path.join(run_dir, "header_spec.json")
    evidence_path = os.path.join(run_dir, "evidence_packet.json")
    adapter_path = os.path.join(run_dir, "adapter_schema_spec.json")
    table_region_path = os.path.join(run_dir, "table_region.json")
    header_override_path = os.path.join(run_dir, "header_override.json")
    manual_recipe_path = os.path.join(run_dir, "manual_recipe.json")

    with open(evidence_path, "r", encoding="utf-8") as handle:
        evidence = json.load(handle)

    if evidence.get("file_path") and evidence.get("file_hash"):
        current_hash = _hash_file(evidence["file_path"])
        if current_hash != evidence["file_hash"]:
            _append_shadow(
                artifacts_root,
                run_id,
                "resume_guard_file_changed",
                {"expected_hash": evidence["file_hash"], "current_hash": current_hash},
            )
            return PuhemiesResponse(
                run_id=run_id,
                status="needs_human_confirmation",
                message="Input file has changed since the run started.",
                question="Please re-run with the updated file.",
                next_step="rerun_required",
            )

    if os.path.exists(manual_recipe_path):
        with open(manual_recipe_path, "r", encoding="utf-8") as handle:
            manual_recipe = json.load(handle)
        try:
            _apply_manual_recipe(run_id, artifacts_root, manual_recipe, evidence)
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

    if os.path.exists(header_override_path):
        with open(header_override_path, "r", encoding="utf-8") as handle:
            override = json.load(handle)
        headers, header_row = _apply_header_override(run_id, artifacts_root, override, evidence)
    else:
        if not os.path.exists(confirmation_path):
            return PuhemiesResponse(
                run_id=run_id,
                status="needs_human_confirmation",
                message="Missing human confirmation.",
                question="Provide confirmed header candidate id.",
                next_step="write_human_confirmation",
            )

        with open(header_path, "r", encoding="utf-8") as handle:
            header_spec = json.load(handle)

        with open(confirmation_path, "r", encoding="utf-8") as handle:
            confirmation = json.load(handle)

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

        _append_shadow(
            artifacts_root,
            run_id,
            "human_confirmation_received",
            {"confirmed_header_candidate": confirmed_id},
        )

        headers = selected["normalized_headers"]
        header_row = selected["header_rows"][0]

    if evidence.get("file_path"):
        data_rows = _read_data_rows(
            evidence["file_path"],
            header_row,
            evidence.get("sheet_name"),
        )
    else:
        data_rows = evidence.get("preview_rows", [])[header_row + 1 :]
    adapter_spec = None
    if os.path.exists(adapter_path):
        with open(adapter_path, "r", encoding="utf-8") as handle:
            adapter_spec = json.load(handle)

    table_region = None
    if os.path.exists(table_region_path):
        with open(table_region_path, "r", encoding="utf-8") as handle:
            table_region = json.load(handle)

    headers, data_rows = _apply_table_region(headers, data_rows, header_row, table_region)
    _write_schema_and_output(run_id, artifacts_root, data_rows, headers, adapter_spec=adapter_spec)

    return PuhemiesResponse(
        run_id=run_id,
        status="ok",
        message="Schema created and output saved.",
        next_step="review_artifacts",
    )


def puhemies_run_from_file(run_id: str, input_path: str, artifacts_root: str) -> PuhemiesResponse:
    preview_rows, sheet_name = _read_preview_rows(input_path)
    response = puhemies_orchestrate(
        run_id,
        preview_rows,
        artifacts_root,
        file_path=input_path,
        sheet_name=sheet_name,
    )
    structural_hash = _compute_structural_hash(preview_rows, input_path)
    recalled = _lookup_recipe_for_hash(artifacts_root, structural_hash)
    if recalled:
        run_dir = _artifact_dir(artifacts_root, run_id)
        os.makedirs(run_dir, exist_ok=True)
        _write_json(os.path.join(run_dir, "manual_recipe.json"), recalled)
        _append_shadow(
            artifacts_root,
            run_id,
            "manual_recipe_recalled",
            {"structural_hash": structural_hash},
        )
        return puhemies_continue(run_id, artifacts_root)
    return response
