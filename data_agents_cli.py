import argparse
import json
import os
import sys
from datetime import datetime

from runtime.excel_flow import puhemies_continue, puhemies_orchestrate, puhemies_run_from_file, write_human_confirmation


def _repo_root() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _artifacts_root() -> str:
    return os.path.join(_repo_root(), "artifacts")


def _print_choices(choices):
    for idx, choice in enumerate(choices, start=1):
        headers_preview = ", ".join(choice.get("normalized_headers", []))
        print(f"{idx}) {choice['id']} | confidence={choice['confidence']} | {headers_preview}")


def _load_header_candidates(artifacts_root, run_id):
    header_path = os.path.join(artifacts_root, run_id, "header_spec.json")
    if not os.path.exists(header_path):
        raise FileNotFoundError("header_spec.json not found for run.")
    with open(header_path, "r", encoding="utf-8") as handle:
        header_spec = json.load(handle)
    return header_spec.get("candidates", [])


def run_command(args):
    artifacts_root = _artifacts_root()
    run_id = args.run_id or datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    input_path = os.path.abspath(args.input)

    response = puhemies_run_from_file(run_id, input_path, artifacts_root)
    response_dict = response.to_dict()

    print(response_dict["message"])
    if response_dict["status"] == "needs_human_confirmation":
        print(response_dict["question"])
        _print_choices(response_dict.get("choices", []))
        print(f"Next: data-agents confirm --run-id {run_id} --choice <candidate_id>")

        if args.interactive:
            choice_input = input("Choose candidate (id or number): ").strip()
            choices = response_dict.get("choices", [])
            selected_id = choice_input
            if choice_input.isdigit():
                index = int(choice_input) - 1
                if 0 <= index < len(choices):
                    selected_id = choices[index]["id"]
            write_human_confirmation(artifacts_root, run_id, selected_id, confirmed_by="interactive")
            print(f"Confirmation saved for {selected_id}. Resuming...")
            response_after = puhemies_continue(run_id, artifacts_root)
            print(response_after.to_dict()["message"])
            return 0

        return 2

    return 0


def confirm_command(args):
    artifacts_root = _artifacts_root()
    run_id = args.run_id
    candidates = _load_header_candidates(artifacts_root, run_id)
    if not any(candidate.get("candidate_id") == args.choice for candidate in candidates):
        raise SystemExit(f"Invalid candidate id: {args.choice}")
    write_human_confirmation(artifacts_root, run_id, args.choice, confirmed_by="cli")
    print("Confirmation saved.")
    print(f"Next: data-agents resume --run-id {run_id}")
    return 0


def resume_command(args):
    artifacts_root = _artifacts_root()
    response = puhemies_continue(args.run_id, artifacts_root)
    response_dict = response.to_dict()
    print(response_dict["message"])
    if response_dict["status"] == "needs_human_confirmation":
        if response_dict.get("question"):
            print(response_dict["question"])
        return 2
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(prog="data-agents")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Start a Puhemies run.")
    run_parser.add_argument("--input", required=True, help="Path to input file (.xlsx or .csv).")
    run_parser.add_argument("--run-id", help="Optional run id.")
    run_parser.add_argument("--interactive", action="store_true", help="Prompt for confirmation and resume.")
    run_parser.set_defaults(func=run_command)

    confirm_parser = subparsers.add_parser("confirm", help="Confirm header choice for a run.")
    confirm_parser.add_argument("--run-id", required=True, help="Run id to confirm.")
    confirm_parser.add_argument("--choice", required=True, help="Header candidate id to confirm.")
    confirm_parser.set_defaults(func=confirm_command)

    resume_parser = subparsers.add_parser("resume", help="Resume a run after confirmation.")
    resume_parser.add_argument("--run-id", required=True, help="Run id to resume.")
    resume_parser.set_defaults(func=resume_command)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
