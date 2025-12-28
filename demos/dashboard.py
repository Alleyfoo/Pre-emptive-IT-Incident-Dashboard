import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEMOS_ROOT = os.path.join(REPO_ROOT, "demos")
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if DEMOS_ROOT not in sys.path:
    sys.path.insert(0, DEMOS_ROOT)

from simple_schema_builder import render_schema_builder
from runtime.excel_flow import puhemies_continue, puhemies_run_from_file


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
    run_dir = os.path.join(artifacts_root(), run_id)
    shadow_path = os.path.join(run_dir, "shadow.jsonl")
    if not os.path.exists(shadow_path):
        if os.path.exists(os.path.join(run_dir, "save_manifest.json")):
            return "ok"
        return "new"
    last_event = None
    with open(shadow_path, "r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                last_event = json.loads(line).get("event")
    if last_event in ["stop_due_to_ambiguous_headers", "resume_guard_file_changed"]:
        return "needs_confirmation"
    if os.path.exists(os.path.join(run_dir, "save_manifest.json")):
        return "ok"
    return last_event or "unknown"


def load_dataframe_for_run(evidence: Dict[str, object]) -> Optional[pd.DataFrame]:
    file_path = evidence.get("file_path")
    if not file_path or not os.path.exists(file_path):
        return None
    ext = os.path.splitext(file_path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(file_path, sheet_name=evidence.get("sheet_name") or 0, header=None, dtype=str).fillna("")
    if ext == ".csv":
        return pd.read_csv(file_path, header=None, dtype=str, keep_default_na=False).fillna("")
    return None


def count_rows(csv_path: str) -> int:
    if not os.path.exists(csv_path):
        return 0
    with open(csv_path, "r", encoding="utf-8") as handle:
        return max(0, sum(1 for _ in handle) - 1)


st.set_page_config(page_title="Puhemies Dashboard", page_icon="D", layout="wide")
st.title("Puhemies Dashboard")
st.caption("Unified runs view with embedded Schema Builder.")

if "selected_run" not in st.session_state:
    st.session_state.selected_run = None
if "response" not in st.session_state:
    st.session_state.response = None

tabs = st.tabs(["Runs", "Run Details"])

with tabs[0]:
    st.subheader("Runs")
    runs = list_runs()
    if runs:
        rows = [{"run_id": run_id, "status": load_shadow_status(run_id)} for run_id in runs]
        st.table(rows)
        st.session_state.selected_run = st.selectbox("Open run", runs, index=0)
        if st.button("Clear Selected Run Outputs"):
            run_dir = os.path.join(artifacts_root(), st.session_state.selected_run)
            output_dir = os.path.join(run_dir, "output")
            removed = False
            for filename in ["clean.csv", "clean_data.csv", "extracted_metadata.json"]:
                path = os.path.join(output_dir, filename)
                if os.path.exists(path):
                    os.remove(path)
                    removed = True
            if removed:
                st.success("Outputs cleared for selected run.")
            else:
                st.info("No outputs found to clear.")
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
        st.write("Select a run from the Runs tab.")
    else:
        st.subheader(f"Run: {run_id}")
        response = st.session_state.response
        if response:
            st.info(response.get("message", ""))

        evidence = load_json(os.path.join(artifacts_root(), run_id, "evidence_packet.json"))
        status = load_shadow_status(run_id)
        if response and response.get("status"):
            status = response["status"]
        st.write(f"Status: {status}")
        st.write(f"Source file: {evidence.get('file_path', 'n/a')}")
        if evidence.get("structural_hash"):
            st.write(f"Structural hash: {evidence.get('structural_hash')}")

        manual_recipe_path = os.path.join(artifacts_root(), run_id, "manual_recipe.json")
        proposed_recipe_path = os.path.join(artifacts_root(), run_id, "proposed_recipe.json")
        initial_recipe = None
        if os.path.exists(manual_recipe_path):
            initial_recipe = load_json(manual_recipe_path)
        elif os.path.exists(proposed_recipe_path):
            initial_recipe = load_json(proposed_recipe_path)

        df_raw = load_dataframe_for_run(evidence)
        normalized_status = status
        if status == "needs_human_confirmation":
            normalized_status = "needs_confirmation"

        if normalized_status == "needs_confirmation" and df_raw is not None:
            st.subheader("Schema Builder (V6)")
            render_schema_builder(
                df_raw=df_raw,
                initial_recipe=initial_recipe,
                run_id=run_id,
                show_uploader=False,
                allow_download=False,
                return_payload_on_submit=False,
                show_submit_button=False,
            )
            recipe_summary = st.session_state.get("schema_builder_recipe") or {}
            fields = recipe_summary.get("fields", [])
            column_count = sum(1 for field in fields if field.get("source_type") == "column")
            metadata_count = sum(1 for field in fields if field.get("source_type") == "metadata")
            if fields:
                st.caption(f"Recipe fields: {column_count} columns, {metadata_count} metadata.")
            if st.session_state.get("schema_builder_has_fields"):
                if st.button("Save Manual Recipe and Resume"):
                    payload = st.session_state.get("schema_builder_recipe")
                    if payload:
                        has_columns = any(field.get("source_type") == "column" for field in fields)
                        if not has_columns:
                            st.warning("Add at least one Table Column field before saving the recipe.")
                        else:
                            with open(manual_recipe_path, "w", encoding="utf-8") as handle:
                                json.dump(payload, handle, indent=2, ensure_ascii=True)
                            response_after = puhemies_continue(run_id, artifacts_root())
                            st.session_state.response = response_after.to_dict()
                            st.success("Manual recipe saved. Run resumed.")
                    else:
                        st.warning("No recipe available yet.")
        elif normalized_status == "needs_confirmation":
            st.warning("Run needs confirmation, but no readable source file is available.")

        output_dir = os.path.join(artifacts_root(), run_id, "output")
        clean_data_path = os.path.join(output_dir, "clean_data.csv")
        clean_path = os.path.join(output_dir, "clean.csv")
        metadata_path = os.path.join(output_dir, "extracted_metadata.json")
        if os.path.exists(clean_data_path) or os.path.exists(clean_path):
            st.subheader("Outputs")
        if os.path.exists(clean_data_path):
            st.write(f"Table: {os.path.relpath(clean_data_path, REPO_ROOT)}")
            st.write(f"Rows written: {count_rows(clean_data_path)}")
            st.caption("Note: outputs persist across reruns; clear artifacts to regenerate.")
        if os.path.exists(clean_path):
            st.write(f"Table: {os.path.relpath(clean_path, REPO_ROOT)}")
            st.write(f"Rows written: {count_rows(clean_path)}")
        if os.path.exists(metadata_path):
            st.write(f"Metadata: {os.path.relpath(metadata_path, REPO_ROOT)}")
