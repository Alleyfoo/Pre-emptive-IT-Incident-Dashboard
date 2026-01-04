import json
import os
import sys
import tempfile
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from runtime.artifact_store import build_artifact_store
from runtime.excel_flow import puhemies_continue, puhemies_run_from_file
from simple_schema_builder import render_schema_builder

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEMOS_ROOT = os.path.join(REPO_ROOT, "demos")
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if DEMOS_ROOT not in sys.path:
    sys.path.insert(0, DEMOS_ROOT)


def artifacts_root() -> str:
    return os.environ.get("ARTIFACTS_ROOT") or os.path.join(REPO_ROOT, "artifacts")


def artifact_store():
    return build_artifact_store(artifacts_root())


def uploads_dir() -> str:
    path = os.path.join(REPO_ROOT, "demos", ".uploads")
    os.makedirs(path, exist_ok=True)
    return path


def load_json_from_store(store, key: str) -> Dict:
    if not store.exists(key):
        return {}
    return json.loads(store.read_text(key))


def list_runs(store) -> List[str]:
    run_ids = set()
    for key in store.list(""):
        parts = key.split("/")
        if parts and parts[0]:
            run_ids.add(parts[0])
    return sorted(run_ids, reverse=True)


def load_shadow_status(store, run_id: str) -> str:
    shadow_key = f"{run_id}/shadow.jsonl"
    manifest_key = f"{run_id}/save_manifest.json"
    if not store.exists(shadow_key):
        if store.exists(manifest_key):
            return "ok"
        return "new"
    last_event = None
    content = store.read_text(shadow_key).splitlines()
    for line in content:
        if line.strip():
            last_event = json.loads(line).get("event")
    if last_event in ["stop_due_to_ambiguous_headers", "resume_guard_file_changed"]:
        return "needs_confirmation"
    if store.exists(manifest_key):
        return "ok"
    return last_event or "unknown"


def _materialize_input(store, run_id: str, evidence: Dict[str, object]) -> Optional[str]:
    artifact_key = evidence.get("input_artifact_key")
    source_uri = evidence.get("source_uri")
    cache_dir = os.path.join(tempfile.gettempdir(), "data-agents-dashboard", run_id)
    if artifact_key:
        store_key = artifact_key.split("artifacts/", 1)[1] if artifact_key.startswith("artifacts/") else artifact_key
        if store.exists(store_key):
            os.makedirs(cache_dir, exist_ok=True)
            filename = os.path.basename(store_key) or "input"
            local_path = os.path.join(cache_dir, filename)
            with open(local_path, "wb") as handle:
                handle.write(store.read_bytes(store_key))
            return local_path
    if source_uri and source_uri.startswith("file://"):
        path = source_uri[len("file://") :]
        if os.path.exists(path):
            return path
    if source_uri and os.path.isabs(source_uri) and os.path.exists(source_uri):
        return source_uri
    return None


def load_dataframe_for_run(store, run_id: str, evidence: Dict[str, object]) -> Optional[pd.DataFrame]:
    file_path = _materialize_input(store, run_id, evidence)
    if not file_path:
        return None
    ext = os.path.splitext(file_path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(file_path, sheet_name=evidence.get("sheet_name") or 0, header=None, dtype=str).fillna("")
    if ext == ".csv":
        return pd.read_csv(file_path, header=None, dtype=str, keep_default_na=False).fillna("")
    return None


def count_rows(store, key: str) -> int:
    if not store.exists(key):
        return 0
    text = store.read_text(key)
    lines = text.splitlines()
    return max(0, len(lines) - 1)


st.set_page_config(page_title="Puhemies Dashboard", page_icon="D", layout="wide")
st.title("Puhemies Dashboard")
st.caption("Unified runs view with embedded Schema Builder.")

if "selected_run" not in st.session_state:
    st.session_state.selected_run = None
if "response" not in st.session_state:
    st.session_state.response = None

store = artifact_store()
tabs = st.tabs(["Runs", "Run Details"])

with tabs[0]:
    st.subheader("Runs")
    runs = list_runs(store)
    if runs:
        rows = [{"run_id": run_id, "status": load_shadow_status(store, run_id)} for run_id in runs]
        st.table(rows)
        st.session_state.selected_run = st.selectbox("Open run", runs, index=0)
        if st.button("Clear Selected Run Outputs"):
            removed = False
            for filename in ["clean.csv", "clean_data.csv", "extracted_metadata.json"]:
                key = f"{st.session_state.selected_run}/output/{filename}"
                if store.exists(key):
                    store.write_bytes(key, b"", content_type="text/plain")
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

        evidence = load_json_from_store(store, f"{run_id}/evidence_packet.json")
        status = load_shadow_status(store, run_id)
        if response and response.get("status"):
            status = response["status"]
        st.write(f"Status: {status}")
        st.write(f"Source: {evidence.get('source_uri') or 'n/a'}")
        if evidence.get("structural_hash"):
            st.write(f"Structural hash: {evidence.get('structural_hash')}")

        initial_recipe = None
        if store.exists(f"{run_id}/manual_recipe.json"):
            initial_recipe = load_json_from_store(store, f"{run_id}/manual_recipe.json")
        elif store.exists(f"{run_id}/proposed_recipe.json"):
            initial_recipe = load_json_from_store(store, f"{run_id}/proposed_recipe.json")

        df_raw = load_dataframe_for_run(store, run_id, evidence)
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
                            store.write_text(
                                f"{run_id}/manual_recipe.json",
                                json.dumps(payload, indent=2, ensure_ascii=True),
                                content_type="application/json",
                            )
                            response_after = puhemies_continue(run_id, artifacts_root())
                            st.session_state.response = response_after.to_dict()
                            st.success("Manual recipe saved. Run resumed.")
                    else:
                        st.warning("No recipe available yet.")
        elif normalized_status == "needs_confirmation":
            st.warning("Run needs confirmation, but no readable source file is available.")

        clean_data_key = f"{run_id}/output/clean_data.csv"
        clean_key = f"{run_id}/output/clean.csv"
        metadata_key = f"{run_id}/output/extracted_metadata.json"
        if store.exists(clean_data_key) or store.exists(clean_key):
            st.subheader("Outputs")
        if store.exists(clean_data_key):
            st.write(f"Table: {store.uri_for_key(clean_data_key)}")
            st.write(f"Rows written: {count_rows(store, clean_data_key)}")
            st.caption("Note: outputs persist across reruns; clear artifacts to regenerate.")
        if store.exists(clean_key):
            st.write(f"Table: {store.uri_for_key(clean_key)}")
            st.write(f"Rows written: {count_rows(store, clean_key)}")
        if store.exists(metadata_key):
            st.write(f"Metadata: {store.uri_for_key(metadata_key)}")
