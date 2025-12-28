import ast
import json
import os
import sys
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from runtime.data_investigator import scan_dataframe_structure, get_column_inventory_from_df

DATA_TYPES = ["string", "number", "date"]


def get_excel_col_name(n: int) -> str:
    n = int(n) if n is not None else 0
    name = ""
    while n >= 0:
        name = chr(n % 26 + 65) + name
        n = n // 26 - 1
    return name


def _sanitize_source_pointer(pointer: object) -> object:
    if isinstance(pointer, str) and pointer.strip().startswith("{") and "row" in pointer:
        try:
            return ast.literal_eval(pointer)
        except Exception:
            return pointer
    return pointer


def _schema_list_from_recipe(recipe: Dict[str, object]) -> List[dict]:
    fields = recipe.get("fields", [])
    schema_list = []
    for field in fields:
        target = field.get("target") or field.get("target_name")
        if not target:
            continue
        source_pointer = field.get("source_pointer")
        source_type = field.get("source_type")
        if not source_type:
            if isinstance(source_pointer, dict) and "row" in source_pointer and "col" in source_pointer:
                source_type = "metadata"
            else:
                source_type = "column"
        schema_list.append(
            {
                "target_name": target,
                "source_type": source_type,
                "source_pointer": source_pointer,
                "data_type": field.get("data_type", "string"),
            }
        )
    return schema_list


def _recipe_from_schema_list(
    schema_list: List[dict],
    header_row_index: Optional[int],
    merge_metadata_fields: List[str],
) -> Dict[str, object]:
    final_fields = []
    for item in schema_list:
        clean_item = item.copy()
        clean_item["source_pointer"] = _sanitize_source_pointer(clean_item.get("source_pointer"))
        source_type = clean_item.get("source_type")
        source_pointer = clean_item.get("source_pointer")
        if source_type == "column" and isinstance(source_pointer, dict):
            if "row" in source_pointer and "col" in source_pointer:
                clean_item["source_pointer"] = clean_item.get("target_name")
        final_fields.append(
            {
                "target": clean_item.get("target_name"),
                "source_type": source_type,
                "source_pointer": clean_item.get("source_pointer"),
                "data_type": clean_item.get("data_type"),
            }
        )
    payload = {"fields": final_fields}
    if header_row_index is not None:
        payload["header_row_index"] = int(header_row_index)
    if merge_metadata_fields:
        payload["merge_metadata_fields"] = merge_metadata_fields
    return payload


def _load_dataframe_from_upload(uploaded_file: object) -> Optional[pd.DataFrame]:
    if not uploaded_file:
        return None
    name = getattr(uploaded_file, "name", "")
    if name.lower().endswith(".csv"):
        df = pd.read_csv(uploaded_file, header=None, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(uploaded_file, header=None, dtype=str)
    return df.fillna("")


def render_schema_builder(
    df_raw: Optional[pd.DataFrame] = None,
    initial_recipe: Optional[Dict[str, object]] = None,
    run_id: Optional[str] = None,
    show_uploader: bool = True,
    allow_download: bool = True,
    use_page_config: bool = False,
    return_payload_on_submit: bool = True,
    show_submit_button: bool = True,
) -> Optional[Dict[str, object]]:
    if use_page_config:
        st.set_page_config(layout="wide", page_title="Schema Builder V6.2")
    st.title("Schema Builder V6.2")
    st.caption(
        "Use Metadata for single cells (titles/dates) and Table Columns for data headers."
    )

    if "schema_list" not in st.session_state:
        st.session_state.schema_list = []
    if "target_field_name" not in st.session_state:
        st.session_state.target_field_name = ""
    if "last_click_hash" not in st.session_state:
        st.session_state.last_click_hash = ""
    if "header_row_index" not in st.session_state:
        st.session_state.header_row_index = 0
    if "merge_metadata_fields" not in st.session_state:
        st.session_state.merge_metadata_fields = []
    if "schema_builder_run_id" not in st.session_state:
        st.session_state.schema_builder_run_id = None
    if "selected_row_idx" not in st.session_state:
        st.session_state.selected_row_idx = None
    if "selected_col_idx" not in st.session_state:
        st.session_state.selected_col_idx = 0
    if "schema_builder_recipe" not in st.session_state:
        st.session_state.schema_builder_recipe = None
    if "schema_builder_has_fields" not in st.session_state:
        st.session_state.schema_builder_has_fields = False

    if run_id and st.session_state.schema_builder_run_id != run_id:
        st.session_state.schema_builder_run_id = run_id
        st.session_state.schema_list = []
        st.session_state.target_field_name = ""
        st.session_state.last_click_hash = ""
        st.session_state.header_row_index = 0
        st.session_state.merge_metadata_fields = []
        st.session_state.selected_row_idx = None
        st.session_state.selected_col_idx = 0
        st.session_state.schema_builder_recipe = None
        st.session_state.schema_builder_has_fields = False

    if initial_recipe and not st.session_state.schema_list:
        st.session_state.schema_list = _schema_list_from_recipe(initial_recipe)
        st.session_state.header_row_index = int(initial_recipe.get("header_row_index", 0))
        st.session_state.merge_metadata_fields = initial_recipe.get("merge_metadata_fields", [])

    if df_raw is None and show_uploader:
        uploaded_file = st.file_uploader("Upload Excel or CSV", type=["xlsx", "xls", "csv"])
        df_raw = _load_dataframe_from_upload(uploaded_file)

    if df_raw is None:
        st.info("Upload a file to begin.")
        return None

    def add_field(name: str, source_type: str, source_pointer: object, target_type: str = "string") -> None:
        for item in st.session_state.schema_list:
            if item["target_name"] == name:
                st.warning(f"Field '{name}' already exists.")
                return

        st.session_state.schema_list.append(
            {
                "target_name": name,
                "source_type": source_type,
                "source_pointer": source_pointer,
                "data_type": target_type,
            }
        )

    col_preview, col_builder = st.columns([2, 1])

    with col_preview:
        st.subheader("1. Select Row/Cell")
        event = st.dataframe(
            df_raw,
            use_container_width=True,
            on_select="rerun",
            selection_mode=["single-row", "single-column"],
            height=400,
        )

        if len(event.selection.rows) > 0:
            st.session_state.selected_row_idx = int(event.selection.rows[0])

        if len(event.selection.columns) > 0:
            try:
                st.session_state.selected_col_idx = int(event.selection.columns[0])
            except (ValueError, TypeError):
                st.session_state.selected_col_idx = 0

        selected_row_idx = st.session_state.selected_row_idx
        selected_col_idx = st.session_state.selected_col_idx

        if selected_row_idx is not None:
            st.divider()
            st.markdown(f"**Refine Selection for Row {selected_row_idx}:**")

            col_options = []
            try:
                row_values = df_raw.iloc[selected_row_idx].tolist()
            except IndexError:
                row_values = []

            for idx, val in enumerate(row_values):
                col_name = get_excel_col_name(idx)
                s_val = str(val)
                preview = s_val[:30] + "..." if len(s_val) > 30 else s_val
                label = f"Col {col_name}: {preview}"
                col_options.append(label)

            safe_index = selected_col_idx if 0 <= selected_col_idx < len(col_options) else 0

            selected_col_label = st.selectbox(
                "Which column contains the metadata value?",
                options=col_options,
                index=safe_index,
            )

            final_col_idx = col_options.index(selected_col_label) if selected_col_label in col_options else 0

            try:
                final_value = str(df_raw.iloc[selected_row_idx, final_col_idx])
            except Exception:
                final_value = ""

            current_click_hash = f"{selected_row_idx}-{final_col_idx}"
            if st.session_state.last_click_hash != current_click_hash:
                st.session_state.target_field_name = final_value
                st.session_state.last_click_hash = current_click_hash

    with col_builder:
        st.subheader("2. Add to Recipe")
        tab_meta, tab_cols = st.tabs(["Metadata (Titles/Dates)", "Table Columns (Data)"])

        with tab_meta:
            st.info("Extract one value from this exact cell.")
            if selected_row_idx is not None:
                coord_name = f"{get_excel_col_name(final_col_idx)}{selected_row_idx + 1}"
                st.write(f"Cell: **{coord_name}**")
                st.write(f"Value: `{final_value}`")

                if final_col_idx > 0 and selected_row_idx > 0:
                    st.caption("Tip: If this is a column header (like 'Qty'), use Table Columns.")
            else:
                st.warning("Select a row on the left.")

            name_input = st.text_input("Target Field Name", key="target_field_name")
            if st.button("Add Metadata Field", disabled=(selected_row_idx is None)):
                if name_input:
                    pointer = {"row": int(selected_row_idx), "col": int(final_col_idx)}
                    add_field(name_input, "metadata", pointer)
                    st.success(f"Added '{name_input}'")

        with tab_cols:
            st.info("Extract all data below this row.")
            if selected_row_idx is not None:
                st.write(f"Header Row: **{selected_row_idx}**")
                if st.button("Import Columns from Row"):
                    st.session_state.header_row_index = int(selected_row_idx)
                    potential_headers = df_raw.iloc[selected_row_idx].tolist()
                    count = 0
                    for h_name in potential_headers:
                        h_name = str(h_name).strip()
                        if h_name and "Unnamed" not in h_name:
                            add_field(h_name, "column", h_name)
                            count += 1
                    st.success(f"Imported {count} columns")
            else:
                st.warning("Select a row on the left.")

        if st.button("Scan for Header Row"):
            suggested = scan_dataframe_structure(df_raw)
            st.session_state.header_row_index = int(suggested)
            st.success(f"Suggested header row: {suggested}")
        st.number_input(
            "Header row index (0-based)",
            min_value=0,
            value=int(st.session_state.header_row_index),
            key="header_row_index",
        )

    if df_raw is not None and df_raw.shape[1] > 20:
        header_row = st.session_state.header_row_index
        if 0 <= header_row < len(df_raw):
            header_values = df_raw.iloc[header_row].tolist()
            sample_row = df_raw.iloc[header_row + 1] if header_row + 1 < len(df_raw) else []
            temp_df = pd.DataFrame([sample_row], columns=header_values)
            inventory = get_column_inventory_from_df(temp_df)
            st.subheader("Column Inventory")
            st.dataframe(inventory, use_container_width=True, height=300)

    st.divider()
    st.subheader("3. Current Recipe")
    if st.session_state.schema_list:
        edited = st.data_editor(
            st.session_state.schema_list,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "source_type": st.column_config.SelectboxColumn(
                    "source_type",
                    options=["metadata", "column"],
                    required=True,
                ),
                "data_type": st.column_config.SelectboxColumn(
                    "data_type",
                    options=DATA_TYPES,
                    required=True,
                )
            },
        )
        st.session_state.schema_list = edited

        metadata_targets = [
            row.get("target_name")
            for row in edited
            if row.get("source_type") == "metadata" and row.get("target_name")
        ]
        merge_selected = st.multiselect(
            "Merge metadata fields into table rows",
            options=metadata_targets,
            default=[field for field in st.session_state.merge_metadata_fields if field in metadata_targets],
        )
        st.session_state.merge_metadata_fields = merge_selected

        recipe_payload = _recipe_from_schema_list(
            edited,
            st.session_state.header_row_index,
            st.session_state.merge_metadata_fields,
        )
        st.session_state.schema_builder_recipe = recipe_payload
        st.session_state.schema_builder_has_fields = True

        submitted_payload = None
        if show_submit_button:
            if st.button("Save Manual Recipe"):
                submitted_payload = recipe_payload
                st.success("Manual recipe prepared.")

        if allow_download:
            st.download_button(
                "Download Manual Recipe JSON",
                json.dumps(recipe_payload, indent=2),
                "manual_recipe.json",
                "application/json",
            )

        return submitted_payload if return_payload_on_submit else None
    st.info("No fields added yet.")
    st.session_state.schema_builder_recipe = None
    st.session_state.schema_builder_has_fields = False
    return None


if __name__ == "__main__":
    render_schema_builder(use_page_config=True)
