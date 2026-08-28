from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from .config import ControlError
from .project import load_execution


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def parser(prog: str = "labflow host") -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog=prog, description="Read Host-facing file projections.")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="show the current project's Supervisor status")
    pull = commands.add_parser("pull", help="wait for Host-owned Artifacts")
    pull.add_argument("--timeout", type=float, default=60.0)
    return root


def status() -> dict[str, Any]:
    home, _, _ = load_execution()
    path = home / "supervisor-status.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ControlError("Supervisor status is not available", 75) from None
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlError(f"invalid Supervisor status: {exc}") from None
    if not isinstance(value, dict) or value.get("schema") != "labflow.supervisor-status/v1":
        raise ControlError("invalid Supervisor status")
    return value


def pull(timeout: float) -> dict[str, Any]:
    if timeout < 0 or timeout > 60:
        raise ControlError("timeout must be from 0 through 60 seconds", 64)
    home, _, _ = load_execution()
    path = home / "host-tasks.json"
    deadline = time.monotonic() + timeout
    while True:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            value = None
        except (OSError, json.JSONDecodeError) as exc:
            raise ControlError(f"invalid Host tasks: {exc}") from None
        if isinstance(value, dict) and (value.get("tasks") or value.get("optional_tasks")):
            return value
        if time.monotonic() >= deadline:
            return value if isinstance(value, dict) else {"tasks": [], "optional_tasks": []}
        time.sleep(min(.1, max(0, deadline - time.monotonic())))


def main(argv: list[str] | None = None, *, prog: str = "labflow host") -> int:
    args = parser(prog).parse_args(argv)
    try:
        if args.command == "status":
            emit(status())
        elif args.command == "pull":
            emit(pull(args.timeout))
        return 0
    except ControlError as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return exc.code
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
