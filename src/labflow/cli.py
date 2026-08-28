from __future__ import annotations

import argparse
import sys

from . import cli_host, cli_lab, supervisor, task_cli


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="labflow",
        description="Run reproducible agent laboratories and artifact workflows.",
    )
    commands = root.add_subparsers(dest="group", required=True)
    commands.add_parser("lab", add_help=False, help="manage laboratory service data")
    commands.add_parser("attach", add_help=False, help="attach the OpenCode TUI")
    commands.add_parser("host", add_help=False, help="read Host-facing file projections")
    commands.add_parser("agent", add_help=False, help="inspect the project Artifact graph")
    commands.add_parser("supervisor", add_help=False, help="maintain Sessions and execution state")
    return root


def main(argv: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    if not values or values[0].startswith("-"):
        parser().parse_args(values)
        return 0
    group = values.pop(0)
    if group == "lab":
        return cli_lab.main(values, prog="labflow lab")
    if group == "attach":
        return cli_lab.attach_main(values, prog="labflow attach")
    if group == "host":
        return cli_host.main(values, prog="labflow host")
    if group == "agent":
        return task_cli.main(values, prog="labflow agent")
    if group == "supervisor":
        return supervisor.main(values, prog="labflow supervisor")
    parser().parse_args([group])
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
