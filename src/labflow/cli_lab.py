from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

from .config import ControlError, repository_root, validate_identifier
from .external import resolve_cli
from .lifecycle import opencode_environment
from .state import create_lab_config, lab_link_path, load_lab_config, remove_lab_config


def parser(prog: str = "labflow lab") -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog=prog, description="Run an agent laboratory.")
    commands = root.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run a foreground headless laboratory")
    run.add_argument("lab_name")
    run.add_argument("--port", type=int)
    remove = commands.add_parser("remove", help="remove a stopped laboratory")
    remove.add_argument("lab_name")
    return root


def attach_parser(prog: str = "labflow attach") -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog=prog, description="Attach a TUI to a laboratory.")
    value.add_argument("lab_name")
    return value


def _free_port() -> int:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return int(reservation.getsockname()[1])


def _run(repo: Path, lab_name: str, requested_port: int | None) -> int:
    opencode = resolve_cli("opencode")
    validate_identifier(lab_name, "lab-name")
    port = requested_port if requested_port is not None else _free_port()
    if not 1 <= port <= 65535:
        raise ControlError("port must be from 1 through 65535", 64)
    link = lab_link_path(repo, lab_name)
    if link.exists() or link.is_symlink():
        raise ControlError(f"lab {lab_name} is already configured", 75)
    with socket.socket() as reservation:
        try:
            reservation.bind(("127.0.0.1", port))
        except OSError as exc:
            raise ControlError(f"cannot reserve lab port {port}: {exc}", 69) from None
    lab_root = Path(tempfile.mkdtemp(prefix=f"labflow-{lab_name}-", dir="/tmp")).resolve()
    log_path = lab_root / "logs" / "opencode.log"
    log_path.parent.mkdir()
    config = create_lab_config(repo, lab_name, port, lab_root)
    print(f"Lab {lab_name} is starting on port {port}; root={lab_root}", flush=True)
    command = [*opencode, "serve", "--hostname", "127.0.0.1", "--port", str(port), "--pure"]
    try:
        os.chdir(lab_root)
        with log_path.open("ab", buffering=0) as log:
            os.dup2(log.fileno(), 1)
            os.dup2(log.fileno(), 2)
        os.execvpe(command[0], command, opencode_environment({}))
        return 0
    except OSError:
        remove_lab_config(repo, lab_name, config)
        shutil.rmtree(lab_root, ignore_errors=True)
        raise


def _remove(repo: Path, lab_name: str) -> dict[str, str]:
    lab_name = validate_identifier(lab_name, "lab-name")
    config = load_lab_config(repo, lab_name)
    lab_root = Path(config["root"])
    expected_parent = Path(tempfile.gettempdir()).resolve()
    if lab_root.parent != expected_parent or not lab_root.name.startswith(f"labflow-{lab_name}-"):
        raise ControlError(f"refusing to remove unexpected lab root: {lab_root}", 64)
    try:
        with socket.create_connection(("127.0.0.1", config["port"]), timeout=.2):
            raise ControlError(f"lab {lab_name} is still running", 75)
    except OSError:
        pass
    remove_lab_config(repo, lab_name, config)
    shutil.rmtree(lab_root)
    return {"name": lab_name, "root": str(lab_root), "removed": "true"}


def _attach(repo: Path, lab_name: str) -> int:
    config = load_lab_config(repo, validate_identifier(lab_name, "lab-name"))
    command = [*resolve_cli("opencode"), "attach", f"http://127.0.0.1:{config['port']}"]
    return subprocess.run(command, cwd=config["root"], env=opencode_environment({})).returncode


def main(argv: list[str] | None = None, *, prog: str = "labflow lab") -> int:
    args = parser(prog).parse_args(argv)
    try:
        repo = repository_root(Path.cwd())
        if args.command == "run":
            return _run(repo, args.lab_name, args.port)
        result = _remove(repo, args.lab_name)
        print(f"Removed lab {result['name']}; root={result['root']}")
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
    args = attach_parser(prog).parse_args(argv)
    try:
        return _attach(repository_root(Path.cwd()), args.lab_name)
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
