from __future__ import annotations

import fcntl
import json
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import ControlError, validate_identifier

SCHEMA = "labflow.execution/v1"
CONNECT_TEST_SCHEMA = "labflow.connect-test/v1"
PHASES = {"waiting", "preparing", "ready", "active", "idle", "finishing", "finished", "failed", "retired"}


def now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def validate_session_name(value: str) -> str:
    parts = value.split("/")
    if len(parts) != 2:
        raise ControlError(f"invalid session-name: {value!r}", 64)
    validate_identifier(parts[0], "session-name base")
    if not parts[1].isdigit() or int(parts[1]) < 1:
        raise ControlError(f"invalid session-name generation: {value!r}", 64)
    return value


def execution_root(lab_root: Path, session_name: str) -> Path:
    validate_session_name(session_name)
    base, generation = session_name.split("/")
    return lab_root.resolve() / "executions" / base / generation


def lab_config_path(repo: Path, lab_name: str) -> Path:
    validate_identifier(lab_name, "lab-name")
    return repo / "target" / "labs" / lab_name / "config.json"


def create_lab_config(repo: Path, lab_name: str, port: int, root: Path) -> dict[str, Any]:
    validate_identifier(lab_name, "lab-name")
    if not 1 <= port <= 65535:
        raise ControlError("port must be from 1 through 65535", 64)
    lab_root = root.resolve()
    if not lab_root.is_absolute() or not lab_root.is_dir():
        raise ControlError("lab root must be an existing absolute directory", 66)
    path = lab_config_path(repo, lab_name)
    if path.is_file():
        value = load_lab_config(repo, lab_name)
        if value != {"port": port, "root": str(lab_root)}:
            raise ControlError(f"lab {lab_name} is already configured differently")
        return value
    value = {"port": port, "root": str(lab_root)}
    atomic_json(path, value)
    return value


def load_lab_config(repo: Path, lab_name: str) -> dict[str, Any]:
    path = lab_config_path(repo, lab_name)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ControlError(
            f"missing lab {lab_name}; run labflow lab run {lab_name} before continuing",
            75,
        ) from None
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlError(f"invalid lab configuration: {exc}") from None
    if (
        not isinstance(value, dict)
        or set(value) != {"port", "root"}
        or not isinstance(value.get("port"), int)
        or not 1 <= value["port"] <= 65535
        or not isinstance(value.get("root"), str)
        or not Path(value["root"]).is_absolute()
        or not Path(value["root"]).is_dir()
    ):
        raise ControlError("invalid lab configuration")
    return value


def remove_lab_config(repo: Path, lab_name: str, expected: dict[str, Any]) -> None:
    path = lab_config_path(repo, lab_name)
    try:
        current = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return
    except (OSError, json.JSONDecodeError):
        return
    if current == expected:
        path.unlink()
        try:
            path.parent.rmdir()
        except OSError:
            pass


def connect_test_path(lab_root: Path) -> Path:
    return lab_root.resolve() / "control" / "connect-test.json"


def record_connect_test(lab_name: str, lab_root: Path,
                        result: dict[str, Any]) -> dict[str, Any]:
    validate_identifier(lab_name, "lab-name")
    value = {
        "schema": CONNECT_TEST_SCHEMA,
        "lab_name": lab_name,
        "lab_root": str(lab_root.resolve()),
        "tested_at": now(),
        "transport": "opencode-loopback-http",
        "health": result.get("health"),
        "session_id": result.get("session_id"),
        "title": result.get("title"),
    }
    if value["health"] is not True:
        raise ControlError("connection test did not report a healthy daemon")
    if not isinstance(value["session_id"], str) or not value["session_id"].startswith("ses_"):
        raise ControlError("connection test did not create a valid session")
    if not isinstance(value["title"], str):
        raise ControlError("connection test did not create a named session")
    atomic_json(connect_test_path(lab_root), value)
    return value


def load_connect_test(lab_name: str, lab_root: Path) -> dict[str, Any]:
    path = connect_test_path(lab_root)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ControlError(
            f"missing connection test; run labflow host test-connect {lab_name} before start",
            75,
        ) from None
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlError(f"invalid connection test: {exc}") from None
    if (
        not isinstance(value, dict)
        or set(value) != {"schema", "lab_name", "lab_root", "tested_at",
                          "transport", "health", "session_id", "title"}
        or value.get("schema") != CONNECT_TEST_SCHEMA
        or value.get("lab_name") != lab_name
        or value.get("lab_root") != str(lab_root.resolve())
        or value.get("transport") != "opencode-loopback-http"
        or value.get("health") is not True
        or not isinstance(value.get("lab_name"), str)
        or not isinstance(value.get("lab_root"), str)
        or not Path(value["lab_root"]).is_absolute()
        or not isinstance(value.get("tested_at"), str)
        or not isinstance(value.get("session_id"), str)
        or not value["session_id"].startswith("ses_")
        or not isinstance(value.get("title"), str)
    ):
        raise ControlError("invalid connection test receipt")
    return value


def bind_plan(lab_root: Path, plan_id: str, session_name: str) -> Path:
    root = execution_root(lab_root, session_name); root.mkdir(parents=True, exist_ok=True)
    binding = root / "plan"; expected = f"{plan_id}\n"
    if binding.exists():
        if binding.read_text(encoding="utf-8") != expected:
            raise ControlError(f"session {session_name} is bound to another plan")
    else:
        atomic_write(binding, expected.encode(), 0o444)
    return root

def atomic_write(path: Path, content: bytes, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as output:
            output.write(content); output.flush(); os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)


def atomic_json(path: Path, data: Any) -> None:
    atomic_write(path, (json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode())


@contextmanager
def locked(root: Path, exclusive: bool = True) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    with (root / "lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        try: yield
        finally: fcntl.flock(lock, fcntl.LOCK_UN)

def load_state(root: Path) -> dict[str, Any]:
    try: data = json.loads((root / "state.json").read_text(encoding="utf-8"))
    except FileNotFoundError: raise ControlError(f"missing execution state: {root}", 66) from None
    except (OSError, json.JSONDecodeError) as exc: raise ControlError(f"invalid execution state: {exc}") from None
    if not isinstance(data, dict) or data.get("schema") != SCHEMA:
        raise ControlError("unsupported execution state schema")
    if data.get("phase") not in PHASES: raise ControlError("invalid execution phase")
    binding = (root / "plan").read_text(encoding="utf-8")
    if binding != f"{data.get('plan_id')}\n": raise ControlError("execution plan identity mismatch")
    return data


def save_state(root: Path, state: dict[str, Any]) -> None:
    atomic_json(root / "state.json", state)
