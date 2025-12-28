import json
import os

from runtime.excel_flow import puhemies_orchestrate, puhemies_continue


def write_human_confirmation(artifacts_root, run_id, candidate_id):
    run_dir = os.path.join(artifacts_root, run_id)
    os.makedirs(run_dir, exist_ok=True)
    payload = {"confirmed_header_candidate": candidate_id}
    with open(os.path.join(run_dir, "human_confirmation.json"), "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)


def read_shadow_events(artifacts_root, run_id):
    path = os.path.join(artifacts_root, run_id, "shadow.jsonl")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def test_excel_flow_requires_confirmation_and_completes(tmp_path):
    artifacts_root = tmp_path / "artifacts"
    run_id = "run_test_messy"

    preview_rows = [
        ["Sales Report Q1", "", "", ""],
        ["", "Product Code", "Qty", "Amount"],
        ["row1", "X100", 3, 19.95],
        ["row2", "Y200", 1, 5.00],
    ]

    response = puhemies_orchestrate(run_id, preview_rows, str(artifacts_root))
    response_dict = response.to_dict()

    assert response_dict["status"] == "needs_human_confirmation"
    assert response_dict["question"]
    assert response_dict["choices"]
    assert any(choice["id"] == "row_1" for choice in response_dict["choices"])

    # Only Puhemies returns user-facing output (message + question).
    assert response_dict["message"]

    write_human_confirmation(str(artifacts_root), run_id, "row_1")
    response_after = puhemies_continue(run_id, str(artifacts_root))
    response_after_dict = response_after.to_dict()

    assert response_after_dict["status"] == "ok"

    run_dir = artifacts_root / run_id
    assert (run_dir / "schema_spec.json").exists()
    assert (run_dir / "save_manifest.json").exists()
    assert (run_dir / "output" / "clean.csv").exists()

    events = read_shadow_events(str(artifacts_root), run_id)
    event_names = {event["event"] for event in events}
    assert "stop_due_to_ambiguous_headers" in event_names
    assert "human_confirmation_received" in event_names
