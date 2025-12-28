import argparse
import os
import sys
from datetime import datetime

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from runtime.excel_flow import puhemies_continue, puhemies_run_from_file, write_human_confirmation


def _repo_root() -> str:
    return REPO_ROOT


def _artifacts_root() -> str:
    return os.path.join(_repo_root(), "artifacts")


def _print_choices(choices):
    for idx, choice in enumerate(choices, start=1):
        headers_preview = ", ".join(choice.get("normalized_headers", []))
        print(f"{idx}) {choice['id']} | confidence={choice['confidence']} | {headers_preview}")


def _count_rows(csv_path: str) -> int:
    if not os.path.exists(csv_path):
        return 0
    with open(csv_path, "r", encoding="utf-8") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def run_tui(input_path: str, run_id: str, interactive: bool) -> int:
    response = puhemies_run_from_file(run_id, input_path, _artifacts_root())
    response_dict = response.to_dict()
    print(response_dict["message"])

    if response_dict["status"] == "needs_human_confirmation":
        print(response_dict["question"])
        choices = response_dict.get("choices", [])
        _print_choices(choices)
        print(f"Next: data-agents confirm --run-id {run_id} --choice <candidate_id>")

        if not interactive:
            return 2

        choice_input = input("Choose candidate (id or number): ").strip()
        selected_id = choice_input
        if choice_input.isdigit():
            index = int(choice_input) - 1
            if 0 <= index < len(choices):
                selected_id = choices[index]["id"]
        write_human_confirmation(_artifacts_root(), run_id, selected_id, confirmed_by="tui")
        response_after = puhemies_continue(run_id, _artifacts_root())
        response_dict = response_after.to_dict()
        print(response_dict["message"])

    if response_dict["status"] == "ok":
        output_path = os.path.join(_artifacts_root(), run_id, "output", "clean.csv")
        relative_output = os.path.relpath(output_path, _repo_root())
        rows_written = _count_rows(output_path)
        print(f"Output: {relative_output}")
        print(f"Rows written: {rows_written}")

    return 0


def main():
    parser = argparse.ArgumentParser(description="TUI demo for Puhemies workflow.")
    parser.add_argument("--input", help="Path to input Excel/CSV file.")
    parser.add_argument("--run-id", help="Optional run id.")
    parser.add_argument("--interactive", action="store_true", help="Prompt and auto-resume.")
    args = parser.parse_args()

    input_path = args.input or input("Input file path: ").strip()
    run_id = args.run_id or datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    return run_tui(os.path.abspath(input_path), run_id, args.interactive)


if __name__ == "__main__":
    raise SystemExit(main())
