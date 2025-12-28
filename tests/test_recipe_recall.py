import csv
import json

from runtime.excel_flow import puhemies_continue, puhemies_run_from_file


def write_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)


def test_recipe_recall_applies_manual_recipe(tmp_path):
    artifacts_root = tmp_path / "artifacts"

    rows = [
        ["Report Date", "2025-01-01", "", ""],
        ["", "Product Code", "Qty", "Amount"],
        ["row1", "X100", "3", "19.95"],
        ["row2", "Y200", "1", "5.00"],
    ]

    input_path = tmp_path / "first.csv"
    write_csv(input_path, rows)

    run_id = "run_first"
    puhemies_run_from_file(run_id, str(input_path), str(artifacts_root))

    manual_recipe = {
        "header_row_index": 1,
        "fields": [
            {"target": "report_date", "source_pointer": {"row": 0, "col": 1}, "source_type": "metadata"},
            {"target": "product_code", "source_pointer": {"column": "Product Code"}, "source_type": "column"},
        ],
    }
    run_dir = artifacts_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    with open(run_dir / "manual_recipe.json", "w", encoding="utf-8") as handle:
        json.dump(manual_recipe, handle, indent=2, ensure_ascii=True)

    response = puhemies_continue(run_id, str(artifacts_root))
    assert response.to_dict()["status"] == "ok"

    recalled_path = tmp_path / "second.csv"
    write_csv(recalled_path, rows)
    response_recall = puhemies_run_from_file("run_second", str(recalled_path), str(artifacts_root))
    assert response_recall.to_dict()["status"] == "ok"

    output_dir = artifacts_root / "run_second" / "output"
    assert (output_dir / "clean_data.csv").exists()
    assert (output_dir / "extracted_metadata.json").exists()
