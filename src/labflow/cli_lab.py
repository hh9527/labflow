from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from .config import ControlError
from .external import resolve_cli
from .project import LAB_SCHEMA, load_execution
from .runtime_opencode import ENVIRONMENT


def parser(prog: str = "labflow lab") -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog=prog, description="Manage laboratory service data.")
    commands = root.add_subparsers(dest="command", required=True)
    remove = commands.add_parser("remove", help="remove a stopped Lab service directory")
    remove.add_argument("lab_root", type=Path)
    return root


def attach_parser(prog: str = "labflow attach") -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog=prog, description="Attach to the current project's OpenCode.")


def remove(lab_root: Path) -> dict[str, Any]:
    root = lab_root.expanduser().resolve(strict=True)
    expected_parent = Path(tempfile.gettempdir()).resolve()
    if root.parent != expected_parent or not root.name.startswith("labflow-"):
        raise ControlError(f"refusing to remove unexpected Lab root: {root}", 64)
    try:
        value = json.loads((root / "config.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlError(f"invalid Lab configuration: {exc}") from None
    if (not isinstance(value, dict) or set(value) != {"schema", "port", "root"}
            or value.get("schema") != LAB_SCHEMA or value.get("root") != str(root)
            or isinstance(value.get("port"), bool) or not isinstance(value.get("port"), int)):
        raise ControlError("invalid Lab configuration")
    try:
        with socket.create_connection(("127.0.0.1", value["port"]), timeout=.2):
            raise ControlError("OpenCode is still running on the Lab port", 75)
    except OSError:
        pass
    shutil.rmtree(root)
    return {"root": str(root), "removed": True}


def attach() -> int:
    _, _, config = load_execution()
    command = [*resolve_cli("opencode"), "attach", f"http://127.0.0.1:{config['port']}"]
    environment = os.environ.copy()
    environment.update(ENVIRONMENT)
    return subprocess.run(command, cwd=config["project_home"],
                          env=environment).returncode


def main(argv: list[str] | None = None, *, prog: str = "labflow lab") -> int:
    args = parser(prog).parse_args(argv)
    try:
        result = remove(args.lab_root)
        print(f"Removed Lab; root={result['root']}")
        return 0
    except ControlError as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return exc.code
    except OSError as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return 69
    except KeyboardInterrupt:
        return 130


def attach_main(argv: list[str] | None = None, *, prog: str = "labflow attach") -> int:
    attach_parser(prog).parse_args(argv)
    try:
        return attach()
    except ControlError as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return exc.code
    except OSError as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return 69
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
