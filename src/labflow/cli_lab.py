from __future__ import annotations

import argparse
import json
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from .client import Client
from .config import ControlError, repository_root, validate_identifier
from .external import resolve_cli
from .lifecycle import lab_sessions, opencode_environment
from .state import create_lab_config, lab_config_path, load_lab_config, remove_lab_config


def parser(prog: str = "labflow lab") -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog=prog, description="Run and inspect an agent laboratory.")
    commands = root.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run a foreground headless laboratory")
    run.add_argument("lab_name")
    run.add_argument("--port", type=int)
    ls = commands.add_parser("ls", help="list sessions whose workspace belongs to the laboratory")
    ls.add_argument("lab_name")
    attach = commands.add_parser("attach", help="attach a TUI to a session title")
    attach.add_argument("lab_name")
    attach.add_argument("session_name")
    return root


def _free_port() -> int:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return int(reservation.getsockname()[1])


def _client(config: dict[str, Any], timeout: float = 5) -> Client:
    return Client(f"http://127.0.0.1:{config['port']}", config["root"], timeout=timeout)


def _lab_sessions(config: dict[str, Any]) -> list[dict[str, Any]]:
    lab_root = Path(config["root"]).resolve()
    records = []
    for session in lab_sessions(_client(config), lab_root):
        directory = session.get("directory")
        if not isinstance(directory, str):
            continue
        try:
            Path(directory).resolve().relative_to(lab_root)
        except ValueError:
            continue
        records.append(session)
    return sorted(records, key=lambda item: (str(item.get("title", "")), str(item.get("id", ""))))


def _run(repo: Path, lab_name: str, requested_port: int | None) -> int:
    opencode = resolve_cli("opencode")
    validate_identifier(lab_name, "lab-name")
    port = requested_port if requested_port is not None else _free_port()
    if not 1 <= port <= 65535:
        raise ControlError("port must be from 1 through 65535", 64)
    if lab_config_path(repo, lab_name).exists():
        raise ControlError(f"lab {lab_name} is already configured", 75)
    with socket.socket() as reservation:
        try:
            reservation.bind(("127.0.0.1", port))
        except OSError as exc:
            raise ControlError(f"cannot reserve lab port {port}: {exc}", 69) from None
    lab_root = Path(tempfile.mkdtemp(prefix=f"labflow-{lab_name}-", dir="/tmp")).resolve()
    log_path = lab_root / "opencode.log"
    log = log_path.open("ab")
    server = subprocess.Popen(
        [*opencode, "serve", "--hostname", "127.0.0.1", "--port", str(port), "--pure"],
        cwd=lab_root,
        env=opencode_environment({}),
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    config: dict[str, Any] | None = None
    try:
        client = Client(f"http://127.0.0.1:{port}", str(lab_root), timeout=.1)
        deadline = time.monotonic() + 10
        while True:
            if server.poll() is not None:
                raise ControlError(f"opencode daemon exited; see {log_path}", 70)
            try:
                client.health()
                break
            except ControlError:
                if time.monotonic() >= deadline:
                    raise ControlError(f"timed out connecting to lab; see {log_path}", 69) from None
                time.sleep(.1)
        config = create_lab_config(repo, lab_name, port, lab_root)
        print(f"Lab {lab_name} is ready on port {port}; root={lab_root}", flush=True)
        returncode = server.wait()
        if returncode:
            raise ControlError(f"opencode daemon exited with status {returncode}; see {log_path}", 70)
        return 0
    finally:
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
        log.close()
        if config is not None:
            remove_lab_config(repo, lab_name, config)
        shutil.rmtree(lab_root, ignore_errors=True)


def _list(repo: Path, lab_name: str) -> int:
    config = load_lab_config(repo, validate_identifier(lab_name, "lab-name"))
    statuses: dict[str, Any] = {}
    status_directories: set[str] = set()
    result = []
    for session in _lab_sessions(config):
        session_id = session.get("id")
        directory = session.get("directory")
        if isinstance(directory, str) and directory not in status_directories:
            statuses.update(Client(
                f"http://127.0.0.1:{config['port']}", directory
            ).statuses())
            status_directories.add(directory)
        result.append({
            "title": session.get("title"),
            "id": session_id,
            "workspace": session.get("directory"),
            "state": statuses.get(session_id, {"type": "idle"}).get("type", "idle"),
        })
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _attach(repo: Path, lab_name: str, session_name: str) -> int:
    config = load_lab_config(repo, validate_identifier(lab_name, "lab-name"))
    matches = [item for item in _lab_sessions(config) if item.get("title") == session_name]
    if not matches:
        raise ControlError(f"session title not found in lab {lab_name}: {session_name}", 66)
    if len(matches) != 1:
        raise ControlError(f"session title is not unique in lab {lab_name}: {session_name}", 65)
    session = matches[0]
    command = [*resolve_cli("opencode"), "attach", f"http://127.0.0.1:{config['port']}",
               "--dir", session["directory"], "--session", session["id"]]
    return subprocess.run(command, cwd=session["directory"], env=opencode_environment({})).returncode


def main(argv: list[str] | None = None, *, prog: str = "labflow lab") -> int:
    args = parser(prog).parse_args(argv)
    try:
        repo = repository_root(Path.cwd())
        if args.command == "run":
            return _run(repo, args.lab_name, args.port)
        if args.command == "ls":
            return _list(repo, args.lab_name)
        return _attach(repo, args.lab_name, args.session_name)
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
