from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .benchmark_mode import end_problem, start_problem
from .config import ControlError
from .task_cli import find_root


def parser(prog: str = "labflow problem") -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog=prog, description="Operate a Benchmark problem channel.")
    value.add_argument("--root", type=Path)
    commands = value.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start", help="copy one prepared problem into the channel")
    start.add_argument("problem")
    end = commands.add_parser("end", help="archive and clear the active problem channel")
    end.add_argument("outcome", choices=("ok", "error", "cancel"))
    return value


def main(argv: list[str] | None = None, *, prog: str = "labflow problem") -> int:
    args = parser(prog).parse_args(argv)
    try:
        root = args.root.resolve() if args.root else find_root(Path.cwd())
        result = (start_problem(root, args.problem) if args.command == "start"
                  else end_problem(root, args.outcome))
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except ControlError as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return exc.code


if __name__ == "__main__":
    raise SystemExit(main())
