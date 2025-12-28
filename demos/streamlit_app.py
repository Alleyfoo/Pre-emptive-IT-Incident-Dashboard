import os
import sys
from datetime import datetime

import streamlit as st

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from runtime.excel_flow import puhemies_continue, puhemies_run_from_file, write_human_confirmation


def repo_root() -> str:
    return REPO_ROOT


def artifacts_root() -> str:
    return os.path.join(repo_root(), "artifacts")


def uploads_dir() -> str:
    path = os.path.join(repo_root(), "demos", ".uploads")
    os.makedirs(path, exist_ok=True)
    return path


def count_rows(csv_path: str) -> int:
    if not os.path.exists(csv_path):
        return 0
    with open(csv_path, "r", encoding="utf-8") as handle:
        return max(0, sum(1 for _ in handle) - 1)


st.set_page_config(page_title="Data Agents Demo", page_icon="📊")
st.title("Data Agents Demo")
st.caption("Puhemies-only workflow with artifacts under artifacts/<run_id>/")

uploaded = st.file_uploader("Upload Excel or CSV", type=["xlsx", "xls", "csv"])

if "run_id" not in st.session_state:
    st.session_state.run_id = None
if "response" not in st.session_state:
    st.session_state.response = None

if uploaded:
    run_id = st.text_input("Run id", value=st.session_state.run_id or datetime.utcnow().strftime("%Y%m%d-%H%M%S"))
    if st.button("Run"):
        file_path = os.path.join(uploads_dir(), f"{run_id}_{uploaded.name}")
        with open(file_path, "wb") as handle:
            handle.write(uploaded.getbuffer())
        response = puhemies_run_from_file(run_id, file_path, artifacts_root())
        st.session_state.run_id = run_id
        st.session_state.response = response.to_dict()

if st.session_state.response:
    response = st.session_state.response
    st.write(response["message"])

    if response["status"] == "needs_human_confirmation":
        st.write(response.get("question", ""))
        choices = response.get("choices", [])
        options = {choice["id"]: choice for choice in choices}
        selected = st.radio(
            "Header candidates",
            options=list(options.keys()),
            format_func=lambda key: f"{key} | confidence={options[key]['confidence']} | "
            f"{', '.join(options[key]['normalized_headers'])}",
        )
        if st.button("Confirm and Resume"):
            write_human_confirmation(artifacts_root(), st.session_state.run_id, selected, confirmed_by="streamlit")
            response_after = puhemies_continue(st.session_state.run_id, artifacts_root())
            st.session_state.response = response_after.to_dict()
            if hasattr(st, "rerun"):
                st.rerun()
            else:
                st.experimental_rerun()

    if response["status"] == "ok":
        output_path = os.path.join(artifacts_root(), st.session_state.run_id, "output", "clean.csv")
        relative_output = os.path.relpath(output_path, repo_root())
        st.success("Run completed.")
        st.write(f"Output: {relative_output}")
        st.write(f"Rows written: {count_rows(output_path)}")
