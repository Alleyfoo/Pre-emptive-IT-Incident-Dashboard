import json
import os

from runtime.excel_flow import puhemies_continue, puhemies_run_from_file


def write_csv(path, rows):
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(",".join(row) + "\n")


def read_shadow_events(artifacts_root, run_id):
    path = os.path.join(artifacts_root, run_id, "shadow.jsonl")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def test_header_override_resume_flow(tmp_path):
    artifacts_root = tmp_path / "artifacts"
    run_id = "run_override_test"

    input_path = tmp_path / "messy.csv"
    rows = [
        ["Sales Report Q1", "", "", ""],
        ["", "Product Code", "Qty", "Amount"],
        ["row1", "X100", "3", "19.95"],
        ["row2", "Y200", "1", "5.00"],
    ]
    write_csv(input_path, rows)

    response = puhemies_run_from_file(run_id, str(input_path), str(artifacts_root))
    response_dict = response.to_dict()
    assert response_dict["status"] == "needs_human_confirmation"

    override_payload = {
        "run_id": run_id,
        "mode": "manual",
        "sheet_name": "csv",
        "header_row_index": 1,
        "header_rows": [1],
        "merge_strategy": "single",
        "edited_headers": {"qty": "quantity"},
        "confirmed_by": "test",
        "timestamp": "2025-01-01T00:00:00Z",
        "notes": "test override",
    }
    run_dir = artifacts_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    with open(run_dir / "header_override.json", "w", encoding="utf-8") as handle:
        json.dump(override_payload, handle, indent=2, ensure_ascii=True)

    response_after = puhemies_continue(run_id, str(artifacts_root))
    response_after_dict = response_after.to_dict()
    assert response_after_dict["status"] == "ok"

    assert (run_dir / "schema_spec.json").exists()
    assert (run_dir / "save_manifest.json").exists()
    assert (run_dir / "output" / "clean.csv").exists()

    events = read_shadow_events(str(artifacts_root), run_id)
    event_names = {event["event"] for event in events}
    assert "stop_due_to_ambiguous_headers" in event_names
    assert "header_override_applied" in event_names
