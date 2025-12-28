import re
from typing import Optional

import pandas as pd


def read_with_flattened_headers(file_path: str, header_row_start: int, header_row_end: int) -> list:
    """
    Handles multi-row headers by forward filling and joining parts.
    """
    df_headers = pd.read_excel(
        file_path,
        header=None,
        nrows=(header_row_end - header_row_start + 1),
        skiprows=header_row_start,
        dtype=object,
    )
    df_headers = df_headers.ffill(axis=1)
    new_headers = []
    for col_idx in range(df_headers.shape[1]):
        parts = []
        for row_idx in range(df_headers.shape[0]):
            val = str(df_headers.iloc[row_idx, col_idx]).strip()
            if val and val.lower() != "nan":
                parts.append(val)
        new_headers.append("_".join(parts))
    return new_headers


def clean_series(series: pd.Series, target_type: str) -> pd.Series:
    """
    Forces a column into the desired type.
    """
    if target_type == "number":
        def clean_num(x: object) -> Optional[float]:
            if pd.isna(x):
                return None
            s = str(x)
            s = re.sub(r"[^\d.\-]", "", s)
            if not s:
                return None
            try:
                return float(s)
            except Exception:
                return None

        return series.apply(clean_num)

    if target_type == "date":
        return pd.to_datetime(series, errors="coerce")

    if target_type == "string":
        return series.astype(str).str.strip().replace("nan", "")

    return series


def clean_value(value: object, target_type: str) -> object:
    series = pd.Series([value])
    cleaned = clean_series(series, target_type)
    return cleaned.iloc[0]
