from __future__ import annotations

import argparse
import sys

from . import cli_host, cli_lab, problem_cli, task_cli


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="labflow",
        description="Run reproducible agent laboratories and artifact workflows.",
    )
    commands = root.add_subparsers(dest="group", required=True)
    commands.add_parser("lab", add_help=False, help="run and inspect laboratory servers")
    commands.add_parser("host", add_help=False, help="control and observe experiment sessions")
    commands.add_parser("agent", add_help=False, help="pull and submit artifact DAG work")
    commands.add_parser("problem", add_help=False, help="operate Benchmark problem channels")
    return root


def main(argv: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    if not values or values[0].startswith("-"):
        parser().parse_args(values)
        return 0
    group = values.pop(0)
    if group == "lab":
        return cli_lab.main(values, prog="labflow lab")
    if group == "host":
        return cli_host.main(values, prog="labflow host")
    if group == "agent":
        return task_cli.main(values, prog="labflow agent")
    if group == "problem":
        return problem_cli.main(values, prog="labflow problem")
    parser().parse_args([group])
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
