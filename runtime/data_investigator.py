import os
from typing import List, Optional

import pandas as pd


def scan_file_structure(file_path: str, sample_rows: int = 100, sheet_name: Optional[str] = None) -> int:
    """
    Scans the file to suggest the header row index based on data density.
    """
    ext = os.path.splitext(file_path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path, sheet_name=sheet_name or 0, header=None, nrows=sample_rows, dtype=object)
    elif ext == ".csv":
        df = pd.read_csv(file_path, header=None, nrows=sample_rows, dtype=object, keep_default_na=False)
    else:
        raise ValueError(f"Unsupported input type: {file_path}")
    df = df.fillna("")
    return scan_dataframe_structure(df)


def scan_dataframe_structure(df: pd.DataFrame, sample_rows: int = 100) -> int:
    """
    Scans a dataframe to suggest the header row index based on data density.
    """
    if df.empty:
        return 0
    sample = df.head(sample_rows)
    row_density = sample.apply(lambda row: row.astype(str).str.strip().ne("").sum(), axis=1)
    col_count = sample.shape[1]

    suggested_header = 0
    for i in range(len(row_density) - 1):
        if (row_density.iloc[i] > col_count * 0.5) and (row_density.iloc[i + 1] > col_count * 0.5):
            suggested_header = int(i)
            break
    return suggested_header


def get_column_inventory(file_path: str, header_row: int, sheet_name: Optional[str] = None) -> List[dict]:
    """
    Returns a list of column index, name, and sample value.
    """
    ext = os.path.splitext(file_path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path, sheet_name=sheet_name or 0, header=header_row, nrows=1, dtype=object)
    elif ext == ".csv":
        df = pd.read_csv(file_path, header=header_row, nrows=1, dtype=object, keep_default_na=False)
    else:
        raise ValueError(f"Unsupported input type: {file_path}")
    return get_column_inventory_from_df(df)


def get_column_inventory_from_df(df: pd.DataFrame) -> List[dict]:
    inventory = []
    if df.empty:
        return inventory
    for idx, col_name in enumerate(df.columns):
        clean_name = str(col_name).strip()
        if "Unnamed" in clean_name:
            clean_name = "(Empty Header)"
        sample = ""
        if len(df) > 0 and idx < len(df.columns):
            sample = str(df.iloc[0, idx])
        inventory.append({"index": idx, "original_name": clean_name, "sample_value": sample})
    return inventory
