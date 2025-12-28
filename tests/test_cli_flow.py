import os
import subprocess
import sys


def write_csv(path, rows):
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(",".join(row) + "\n")


def test_cli_run_confirm_resume(tmp_path):
    repo_root = os.path.dirname(os.path.dirname(__file__))
    cli_path = os.path.join(repo_root, "data_agents_cli.py")
    artifacts_root = os.path.join(repo_root, "artifacts")

    input_path = tmp_path / "messy.csv"
    rows = [
        ["Sales Report Q1", "", "", ""],
        ["", "Product Code", "Qty", "Amount"],
        ["row1", "X100", "3", "19.95"],
        ["row2", "Y200", "1", "5.00"],
    ]
    write_csv(input_path, rows)

    run_id = "run_cli_test"
    run_result = subprocess.run(
        [sys.executable, cli_path, "run", "--input", str(input_path), "--run-id", run_id],
        capture_output=True,
        text=True,
    )
    assert run_result.returncode == 2
    assert "needs" not in run_result.stdout.lower()
    assert "data-agents confirm --run-id" in run_result.stdout
    assert "row_1" in run_result.stdout

    confirm_result = subprocess.run(
        [sys.executable, cli_path, "confirm", "--run-id", run_id, "--choice", "row_1"],
        capture_output=True,
        text=True,
    )
    assert confirm_result.returncode == 0
    assert "resume" in confirm_result.stdout.lower()

    resume_result = subprocess.run(
        [sys.executable, cli_path, "resume", "--run-id", run_id],
        capture_output=True,
        text=True,
    )
    assert resume_result.returncode == 0

    run_dir = os.path.join(artifacts_root, run_id)
    assert os.path.exists(os.path.join(run_dir, "schema_spec.json"))
    assert os.path.exists(os.path.join(run_dir, "save_manifest.json"))
    assert os.path.exists(os.path.join(run_dir, "output", "clean.csv"))
