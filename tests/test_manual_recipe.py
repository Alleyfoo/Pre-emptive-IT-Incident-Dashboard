import csv
import json
import os

from runtime.excel_flow import puhemies_continue, puhemies_run_from_file


def write_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)


def read_csv(path):
    with open(path, "r", encoding="utf-8") as handle:
        return list(csv.reader(handle))


def test_manual_recipe_applied(tmp_path):
    artifacts_root = tmp_path / "artifacts"
    run_id = "run_manual_recipe"

    input_path = tmp_path / "manual.csv"
    rows = [
        ["Report Date", "2025-01-01", "", ""],
        ["", "Product Code", "Qty", "Amount"],
        ["row1", "X100", "USD 3", "19.95"],
        ["row2", "Y200", "1", "5.00"],
    ]
    write_csv(input_path, rows)

    puhemies_run_from_file(run_id, str(input_path), str(artifacts_root))

    manual_recipe = {
        "header_row_index": 1,
        "merge_metadata_fields": ["report_date"],
        "fields": [
            {"target": "report_date", "source_pointer": {"row": 0, "col": 1}, "source_type": "metadata"},
            {"target": "product_code", "source_pointer": {"column": "Product Code"}, "source_type": "column"},
            {"target": "qty", "source_pointer": {"column": "Qty"}, "source_type": "column", "data_type": "number"},
        ],
    }
    run_dir = artifacts_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    with open(run_dir / "manual_recipe.json", "w", encoding="utf-8") as handle:
        json.dump(manual_recipe, handle, indent=2, ensure_ascii=True)

    response = puhemies_continue(run_id, str(artifacts_root))
    response_dict = response.to_dict()
    assert response_dict["status"] == "ok"

    output_dir = run_dir / "output"
    assert (output_dir / "clean_data.csv").exists()
    assert (output_dir / "extracted_metadata.json").exists()

    with open(output_dir / "extracted_metadata.json", "r", encoding="utf-8") as handle:
        metadata = json.load(handle)
    assert metadata["report_date"] == "2025-01-01"

    output_rows = read_csv(output_dir / "clean_data.csv")
    assert output_rows[0] == ["product_code", "qty", "report_date"]
    assert output_rows[1] == ["X100", "3.0", "2025-01-01"]
    assert output_rows[2] == ["Y200", "1.0", "2025-01-01"]


def test_manual_recipe_requires_columns(tmp_path):
    artifacts_root = tmp_path / "artifacts"
    run_id = "run_manual_recipe_missing_columns"

    input_path = tmp_path / "manual.csv"
    rows = [
        ["Report Date", "2025-01-01", "", ""],
        ["", "Product Code", "Qty", "Amount"],
        ["row1", "X100", "3", "19.95"],
    ]
    write_csv(input_path, rows)

    puhemies_run_from_file(run_id, str(input_path), str(artifacts_root))

    manual_recipe = {
        "fields": [
            {"target": "report_date", "source_pointer": {"row": 0, "col": 1}, "source_type": "metadata"},
        ],
    }
    run_dir = artifacts_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    with open(run_dir / "manual_recipe.json", "w", encoding="utf-8") as handle:
        json.dump(manual_recipe, handle, indent=2, ensure_ascii=True)

    response = puhemies_continue(run_id, str(artifacts_root))
    response_dict = response.to_dict()
    assert response_dict["status"] == "needs_human_confirmation"
    assert "column field" in response_dict["message"].lower()

    output_dir = run_dir / "output"
    assert not (output_dir / "clean_data.csv").exists()
