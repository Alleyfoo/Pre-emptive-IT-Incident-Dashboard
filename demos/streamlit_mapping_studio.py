import json
import os
import sys
from datetime import datetime
from typing import Dict, List

import streamlit as st
import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from runtime.excel_flow import puhemies_continue, puhemies_run_from_file, write_human_confirmation


def artifacts_root() -> str:
    return os.path.join(REPO_ROOT, "artifacts")


def uploads_dir() -> str:
    path = os.path.join(REPO_ROOT, "demos", ".uploads")
    os.makedirs(path, exist_ok=True)
    return path


def load_json(path: str) -> Dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def list_runs() -> List[str]:
    root = artifacts_root()
    if not os.path.exists(root):
        return []
    return sorted([name for name in os.listdir(root) if os.path.isdir(os.path.join(root, name))], reverse=True)


def load_shadow_status(run_id: str) -> str:
    shadow_path = os.path.join(artifacts_root(), run_id, "shadow.jsonl")
    if not os.path.exists(shadow_path):
        return "new"
    last_event = None
    with open(shadow_path, "r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                last_event = json.loads(line).get("event")
    if last_event in ["stop_due_to_ambiguous_headers", "resume_guard_file_changed"]:
        return "needs_confirmation"
    if os.path.exists(os.path.join(artifacts_root(), run_id, "save_manifest.json")):
        return "ok"
    return last_event or "unknown"


def get_header_candidates(run_id: str) -> List[Dict]:
    header_spec = load_json(os.path.join(artifacts_root(), run_id, "header_spec.json"))
    return header_spec.get("candidates", [])


def get_selected_headers(run_id: str) -> List[str]:
    header_spec = load_json(os.path.join(artifacts_root(), run_id, "header_spec.json"))
    selected_id = header_spec.get("selected_candidate_id")
    for cand in header_spec.get("candidates", []):
        if cand.get("candidate_id") == selected_id:
            return cand.get("normalized_headers", [])
    return []


def load_preview_rows(run_id: str) -> List[List[object]]:
    evidence = load_json(os.path.join(artifacts_root(), run_id, "evidence_packet.json"))
    return evidence.get("preview_rows", [])


def write_table_region(run_id: str, payload: Dict) -> None:
    path = os.path.join(artifacts_root(), run_id, "table_region.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)


def write_adapter_schema(run_id: str, payload: Dict) -> None:
    path = os.path.join(artifacts_root(), run_id, "adapter_schema_spec.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)


def count_rows(csv_path: str) -> int:
    if not os.path.exists(csv_path):
        return 0
    with open(csv_path, "r", encoding="utf-8") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def find_output_files() -> List[str]:
    outputs = []
    root = artifacts_root()
    if not os.path.exists(root):
        return outputs
    for run_id in os.listdir(root):
        output_path = os.path.join(root, run_id, "output", "clean.csv")
        if os.path.exists(output_path):
            outputs.append(output_path)
    return outputs


def validation_preview(rows: List[List[object]], field_map: Dict[str, str], required_fields: List[str]) -> Dict[str, float]:
    stats = {"missing_required_pct": 0.0, "quantity_numeric_pct": 0.0, "date_parse_pct": 0.0}
    if not rows or not field_map:
        return stats
    header_index = {name: idx for idx, name in enumerate(field_map.values())}
    missing_required = 0
    total = len(rows)
    for row in rows:
        for field in required_fields:
            source = field_map.get(field)
            idx = header_index.get(source, None)
            value = row[idx] if idx is not None and idx < len(row) else ""
            if str(value).strip() == "":
                missing_required += 1
    if required_fields and total > 0:
        stats["missing_required_pct"] = round(100 * missing_required / (total * len(required_fields)), 1)
    return stats


def source_columns_from_preview(headers: List[str], preview_rows: List[List[object]]) -> List[str]:
    if headers:
        return headers
    col_count = max((len(row) for row in preview_rows), default=0)
    return [f"col_{idx}" for idx in range(col_count)]


def preview_column_samples(preview_rows: List[List[object]], header_row: int, source_columns: List[str]) -> Dict[str, str]:
    samples = {}
    data_rows = preview_rows[header_row + 1 :] if preview_rows else []
    for idx, name in enumerate(source_columns):
        values = []
        for row in data_rows[:3]:
            if idx < len(row):
                values.append(str(row[idx]))
        samples[name] = ", ".join(values)
    return samples


def normalize_header(value: object, idx: int) -> str:
    if value is None:
        return f"unnamed_{idx}"
    text = str(value).strip().lower()
    if not text:
        return f"unnamed_{idx}"
    return text.replace(" ", "_")


def infer_column_type(values: List[object]) -> str:
    cleaned = [v for v in values if str(v).strip() != ""]
    if not cleaned:
        return "string"
    numeric = True
    date_like = True
    for value in cleaned:
        text = str(value).strip()
        if not text.replace(".", "", 1).isdigit():
            numeric = False
        try:
            pd.to_datetime(value)
        except Exception:
            date_like = False
    if numeric:
        return "number"
    if date_like:
        return "date"
    return "string"


def load_canonical_schema(path: str, header_row_index: int) -> Dict[str, object]:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path, header=None, dtype=object)
    else:
        df = pd.read_csv(path, header=None, dtype=object)
    if header_row_index < 0 or header_row_index >= len(df):
        return {}
    raw_headers = df.iloc[header_row_index].fillna("").tolist()
    normalized_headers = [normalize_header(value, idx) for idx, value in enumerate(raw_headers)]
    data = df.iloc[header_row_index + 1 :].copy()
    data.columns = normalized_headers

    fields = []
    for col in data.columns:
        values = data[col].tolist()
        fields.append(
            {
                "canonical": col,
                "dtype": infer_column_type(values),
                "required": not data[col].isna().any(),
            }
        )
    return {"fields": fields}


def sheet_names_for_file(path: str) -> List[str]:
    if not path or not os.path.exists(path):
        return []
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        return pd.ExcelFile(path).sheet_names
    if ext == ".csv":
        return ["csv"]
    return []


def header_preview(path: str, sheet_name: str | None, header_row: int, rows_after: int = 10) -> Dict[str, List]:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path, sheet_name=sheet_name or 0, header=None, dtype=object)
    else:
        df = pd.read_csv(path, header=None, dtype=object)
    if header_row < 0 or header_row >= len(df):
        return {"headers": [], "rows": []}
    raw_headers = df.iloc[header_row].fillna("").tolist()
    normalized_headers = [normalize_header(value, idx) for idx, value in enumerate(raw_headers)]
    data_rows = df.iloc[header_row + 1 : header_row + 1 + rows_after].fillna("").values.tolist()
    return {"headers": normalized_headers, "rows": data_rows}


st.set_page_config(page_title="Mapping Studio", page_icon="🧭", layout="wide")
st.title("Mapping Studio (V2)")
st.caption("Puhemies-only workflow with replayable artifacts.")

if "selected_run" not in st.session_state:
    st.session_state.selected_run = None
if "response" not in st.session_state:
    st.session_state.response = None

tabs = st.tabs(["Runs Dashboard", "Run Details"])

with tabs[0]:
    st.subheader("Runs")
    runs = list_runs()
    if runs:
        rows = []
        for run_id in runs:
            rows.append({"run_id": run_id, "status": load_shadow_status(run_id)})
        st.table(rows)
        st.session_state.selected_run = st.selectbox("Open run", runs, index=0)
    else:
        st.write("No runs yet.")

    st.subheader("Start new run")
    uploaded = st.file_uploader("Upload Excel or CSV", type=["xlsx", "xls", "csv"])
    run_id = st.text_input("Run id", value=datetime.utcnow().strftime("%Y%m%d-%H%M%S"))
    if uploaded and st.button("Run"):
        file_path = os.path.join(uploads_dir(), f"{run_id}_{uploaded.name}")
        with open(file_path, "wb") as handle:
            handle.write(uploaded.getbuffer())
        response = puhemies_run_from_file(run_id, file_path, artifacts_root())
        st.session_state.selected_run = run_id
        st.session_state.response = response.to_dict()
        st.success("Run started.")

with tabs[1]:
    run_id = st.session_state.selected_run
    if not run_id:
        st.write("Select a run from the dashboard.")
    else:
        st.subheader(f"Run: {run_id}")
        response = st.session_state.response
        if response:
            st.info(response.get("message", ""))
        evidence = load_json(os.path.join(artifacts_root(), run_id, "evidence_packet.json"))
        header_spec = load_json(os.path.join(artifacts_root(), run_id, "header_spec.json"))
        adapter_spec = load_json(os.path.join(artifacts_root(), run_id, "adapter_schema_spec.json"))
        table_region = load_json(os.path.join(artifacts_root(), run_id, "table_region.json"))
        header_override_path = os.path.join(artifacts_root(), run_id, "header_override.json")
        if os.path.exists(header_override_path):
            st.warning("Manual header override is active for this run.")
            if st.button("Clear Manual Header Override"):
                os.remove(header_override_path)
                st.success("Manual header override cleared.")

        detail_tabs = st.tabs(
            [
                "Evidence Preview",
                "Header Picker",
                "Manual Header",
                "Header List + Mapping",
                "Table Region",
                "Column Mapping",
                "Validation Preview",
                "Output",
                "Combine Outputs",
            ]
        )

        with detail_tabs[0]:
            st.write("Preview rows (from evidence packet):")
            preview_rows = evidence.get("preview_rows", [])
            if preview_rows:
                st.dataframe(preview_rows, use_container_width=True)
            st.write(f"Source file: {evidence.get('file_path', 'n/a')}")

        with detail_tabs[1]:
            st.write("Header candidates:")
            candidates = header_spec.get("candidates", [])
            if not candidates:
                st.warning("No header candidates found yet. Run the flow first.")
            else:
                options = {c["candidate_id"]: c for c in candidates}
                selected = st.radio(
                    "Choose header candidate",
                    options=list(options.keys()),
                    format_func=lambda key: f"{key} | confidence={options[key]['confidence']} | "
                    f"{', '.join(options[key]['normalized_headers'])}",
                )
                if st.button("Apply Header and Resume"):
                    write_human_confirmation(artifacts_root(), run_id, selected, confirmed_by="mapping_studio")
                    response_after = puhemies_continue(run_id, artifacts_root())
                    st.session_state.response = response_after.to_dict()
                    st.success("Header confirmation saved. Resumed.")
                st.caption("If no candidate fits, use the Manual Header tab to override the header row.")

        with detail_tabs[2]:
            st.write("Manual header selection (override).")
            file_path = evidence.get("file_path", "")
            if not file_path:
                st.warning("No file path found in evidence. Run the flow from a file to use manual override.")
            else:
                preview_rows = load_preview_rows(run_id)
                if "manual_header_row" not in st.session_state:
                    st.session_state.manual_header_row = 0
                available_sheets = sheet_names_for_file(file_path)
                header_row_index = 0
                if not available_sheets:
                    st.warning("No sheets found for this file.")
                    preview = {"headers": [], "rows": []}
                    sheet = None
                else:
                    sheet = st.selectbox("Sheet", options=available_sheets, index=0)
                    if preview_rows:
                        row_options = []
                        for idx, row in enumerate(preview_rows):
                            row_preview = ", ".join(str(value) for value in row[:4])
                            row_options.append(f"{idx}: {row_preview}")
                        selected_row = st.selectbox(
                            "Use preview row as header",
                            options=row_options,
                            index=min(st.session_state.manual_header_row, len(row_options) - 1),
                        )
                        st.session_state.manual_header_row = int(selected_row.split(":")[0])
                    header_row_index = st.number_input(
                        "Header row index (0-based)",
                        min_value=0,
                        value=int(st.session_state.manual_header_row),
                    )
                    preview = header_preview(file_path, sheet, int(header_row_index))
                if preview["headers"]:
                    st.write("Header preview:")
                    st.dataframe([preview["headers"]], use_container_width=True)
                    st.write("Rows preview:")
                    st.dataframe(preview["rows"], use_container_width=True)

                    rename_rows = [
                        {"original_header": header, "edited_header": header} for header in preview["headers"]
                    ]
                    rename_df = st.data_editor(rename_rows, use_container_width=True, hide_index=True)
                    edited_headers = {}
                    for row in rename_df:
                        original = row.get("original_header", "")
                        edited = row.get("edited_header", "")
                        if original and edited and edited != original:
                            edited_headers[original] = edited
                else:
                    edited_headers = {}
                    st.write("No headers found for that row.")

                if st.button("Apply Manual Header and Resume"):
                    override_payload = {
                        "run_id": run_id,
                        "mode": "manual",
                        "sheet_name": sheet,
                        "header_row_index": int(header_row_index),
                        "header_rows": [int(header_row_index)],
                        "merge_strategy": "single",
                        "edited_headers": edited_headers,
                        "confirmed_by": "streamlit",
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "notes": "manual override from mapping studio",
                    }
                    path = os.path.join(artifacts_root(), run_id, "header_override.json")
                    with open(path, "w", encoding="utf-8") as handle:
                        json.dump(override_payload, handle, indent=2, ensure_ascii=True)
                    response_after = puhemies_continue(run_id, artifacts_root())
                    st.session_state.response = response_after.to_dict()
                    st.success("Header override saved. Resumed.")

        with detail_tabs[3]:
            st.write("Define an ordered header list and map columns explicitly.")
            preview_rows = load_preview_rows(run_id)
            selected_headers = get_selected_headers(run_id)
            source_columns = source_columns_from_preview(selected_headers, preview_rows)
            header_row_index = 0
            selected_id = header_spec.get("selected_candidate_id")
            selected = next(
                (c for c in header_spec.get("candidates", []) if c.get("candidate_id") == selected_id), None
            )
            if selected:
                header_row_index = selected.get("header_rows", [0])[0]

            initial_headers = adapter_spec.get("canonical_fields") or selected_headers or ["field_1", "field_2"]
            rows = [{"order": idx + 1, "canonical_header": name} for idx, name in enumerate(initial_headers)]
            edited_rows = st.data_editor(rows, use_container_width=True, num_rows="dynamic")
            ordered = sorted(
                [row for row in edited_rows if row.get("canonical_header")],
                key=lambda row: int(row.get("order", 0)),
            )
            canonical_fields = [row["canonical_header"] for row in ordered]

            samples = preview_column_samples(preview_rows, header_row_index, source_columns)
            field_map = {}
            required_fields = []
            types = {}
            for idx, field in enumerate(canonical_fields):
                col = st.selectbox(
                    f"Map {field} to source column",
                    options=[""] + source_columns,
                    index=0,
                    key=f"header_list_map_{idx}",
                )
                if col:
                    field_map[field] = col
                    st.caption(f"Sample: {samples.get(col, '')}")
                types[field] = st.selectbox(
                    f"{field} type",
                    options=["string", "number", "date"],
                    index=0,
                    key=f"header_list_type_{idx}",
                )
                if st.checkbox(f"{field} required", value=False, key=f"header_list_req_{idx}"):
                    required_fields.append(field)

            if st.button("Save Header List Mapping"):
                payload = {
                    "run_id": run_id,
                    "schema_layer": "adapter",
                    "canonical_fields": canonical_fields,
                    "field_map": field_map,
                    "types": types,
                    "required_fields": required_fields,
                    "evidence_keys": [
                        f"artifacts/{run_id}/header_spec.json",
                        f"artifacts/{run_id}/evidence_packet.json",
                    ],
                }
                write_adapter_schema(run_id, payload)
                st.success("Adapter schema saved.")

            if st.button("Resume with Header List Mapping"):
                response_after = puhemies_continue(run_id, artifacts_root())
                st.session_state.response = response_after.to_dict()
                st.success("Resumed with header list mapping.")

        with detail_tabs[4]:
            st.write("Define table region (optional).")
            sheet_name = st.text_input("Sheet name", value=table_region.get("sheet_name", ""))
            start_row = st.number_input("Start row (0-based)", min_value=0, value=table_region.get("start_row", 0))
            end_row = st.number_input("End row (0-based)", min_value=0, value=table_region.get("end_row", 0))
            include_columns = st.text_input(
                "Include columns (comma-separated)", value=", ".join(table_region.get("include_columns", []))
            )
            exclude_columns = st.text_input(
                "Exclude columns (comma-separated)", value=", ".join(table_region.get("exclude_columns", []))
            )
            if st.button("Save Table Region"):
                payload = {
                    "sheet_name": sheet_name or None,
                    "start_row": int(start_row),
                    "end_row": int(end_row),
                    "include_columns": [c.strip() for c in include_columns.split(",") if c.strip()],
                    "exclude_columns": [c.strip() for c in exclude_columns.split(",") if c.strip()],
                }
                write_table_region(run_id, payload)
                st.success("Table region saved.")

        with detail_tabs[5]:
            st.write("Map detected columns to canonical fields.")
            headers = get_selected_headers(run_id)
            if not headers:
                st.warning("Pick a header candidate first.")
            else:
                schema_path = st.text_input("Canonical schema path (optional)")
                schema_header_row = st.number_input("Schema header row (0-based)", min_value=0, value=0)
                if st.button("Load Canonical Schema"):
                    if not schema_path or not os.path.exists(schema_path):
                        st.warning("Provide a valid schema path.")
                    else:
                        schema_payload = load_canonical_schema(schema_path, int(schema_header_row))
                        if schema_payload.get("fields"):
                            canonical_schema = {
                                "run_id": run_id,
                                "schema_layer": "core",
                                "fields": schema_payload["fields"],
                                "source_path": os.path.relpath(schema_path, REPO_ROOT),
                            }
                            path = os.path.join(artifacts_root(), run_id, "canonical_schema.json")
                            with open(path, "w", encoding="utf-8") as handle:
                                json.dump(canonical_schema, handle, indent=2, ensure_ascii=True)
                            st.success("Canonical schema loaded.")
                        else:
                            st.warning("No headers found at that row.")

                canonical_schema = load_json(os.path.join(artifacts_root(), run_id, "canonical_schema.json"))
                canonical_fields_from_schema = [
                    field.get("canonical")
                    for field in canonical_schema.get("fields", [])
                    if field.get("canonical")
                ]
                types_from_schema = {
                    field.get("canonical"): field.get("dtype")
                    for field in canonical_schema.get("fields", [])
                    if field.get("canonical")
                }
                required_from_schema = [
                    field.get("canonical")
                    for field in canonical_schema.get("fields", [])
                    if field.get("required")
                ]

                canonical_fields = adapter_spec.get("canonical_fields", ["product_code", "quantity", "order_date"])
                if canonical_fields_from_schema:
                    canonical_fields = canonical_fields_from_schema
                types = adapter_spec.get("types", {})
                if types_from_schema:
                    types.update(types_from_schema)
                required_fields = adapter_spec.get("required_fields", [])
                if required_from_schema:
                    required_fields = required_from_schema
                field_map = adapter_spec.get("field_map", {})
                evidence_keys = [
                    f"artifacts/{run_id}/header_spec.json",
                    f"artifacts/{run_id}/evidence_packet.json",
                ]

                updated_fields = []
                updated_map = {}
                updated_types = {}
                updated_required = []
                for field in canonical_fields:
                    col = st.selectbox(f"{field}", options=[""] + headers, index=0)
                    updated_fields.append(field)
                    if col:
                        updated_map[field] = col
                    dtype = st.selectbox(f"{field} type", options=["string", "number", "date"], index=0)
                    updated_types[field] = dtype
                    if st.checkbox(f"{field} required", value=field in required_fields):
                        updated_required.append(field)

                if st.button("Save Adapter Schema"):
                    payload = {
                        "run_id": run_id,
                        "schema_layer": "adapter",
                        "canonical_fields": updated_fields,
                        "field_map": updated_map,
                        "types": updated_types,
                        "required_fields": updated_required,
                        "evidence_keys": evidence_keys,
                    }
                    write_adapter_schema(run_id, payload)
                    st.success("Adapter schema saved.")

                if st.button("Resume with Adapter"):
                    response_after = puhemies_continue(run_id, artifacts_root())
                    st.session_state.response = response_after.to_dict()
                    st.success("Resumed with adapter schema.")

        with detail_tabs[6]:
            st.write("Validation preview (lightweight).")
            preview_rows = load_preview_rows(run_id)
            header_spec = load_json(os.path.join(artifacts_root(), run_id, "header_spec.json"))
            selected_id = header_spec.get("selected_candidate_id")
            selected = next(
                (c for c in header_spec.get("candidates", []) if c.get("candidate_id") == selected_id), None
            )
            if selected:
                header_row = selected.get("header_rows", [0])[0]
                data_rows = preview_rows[header_row + 1 :]
                field_map = adapter_spec.get("field_map", {})
                required_fields = adapter_spec.get("required_fields", [])
                stats = validation_preview(data_rows, field_map, required_fields)
                st.metric("Missing required (%)", stats["missing_required_pct"])
            else:
                st.write("No header selected.")

        with detail_tabs[7]:
            output_path = os.path.join(artifacts_root(), run_id, "output", "clean.csv")
            if os.path.exists(output_path):
                relative_output = os.path.relpath(output_path, REPO_ROOT)
                st.success("Output ready.")
                st.write(f"Output: {relative_output}")
                st.write(f"Rows written: {count_rows(output_path)}")
                with open(output_path, "rb") as handle:
                    st.download_button("Download CSV", handle.read(), file_name=os.path.basename(output_path))
                st.write("Preview:")
                with open(output_path, "r", encoding="utf-8") as handle:
                    preview = [line.rstrip("\n").split(",") for _, line in zip(range(200), handle)]
                st.dataframe(preview, use_container_width=True)
            else:
                st.write("No output yet. Resume the run to generate output.")

        with detail_tabs[8]:
            st.write("Combine outputs from all runs under artifacts/.")
            output_files = find_output_files()
            if not output_files:
                st.write("No output files found.")
            else:
                st.write(f"Found {len(output_files)} output files.")
                if st.button("Combine All Outputs", key="combine_outputs"):
                    frames = []
                    for path in output_files:
                        try:
                            df = pd.read_csv(path)
                            df["source_run_id"] = os.path.basename(os.path.dirname(os.path.dirname(path)))
                            frames.append(df)
                        except Exception:
                            continue
                    if frames:
                        combined = pd.concat(frames, ignore_index=True)
                        combined_dir = os.path.join(artifacts_root(), "combined")
                        os.makedirs(combined_dir, exist_ok=True)
                        combined_path = os.path.join(combined_dir, "combined.csv")
                        combined.to_csv(combined_path, index=False)
                        st.success("Combined output saved.")
                        st.write(f"Output: {os.path.relpath(combined_path, REPO_ROOT)}")
                        st.write(f"Rows written: {len(combined)}")
                        st.dataframe(combined.head(200), use_container_width=True)
                        with open(combined_path, "rb") as handle:
                            st.download_button(
                                "Download Combined CSV",
                                handle.read(),
                                file_name=os.path.basename(combined_path),
                            )
                    else:
                        st.warning("No readable outputs to combine.")
