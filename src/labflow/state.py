from __future__ import annotations

import fcntl
import json
import os
import re
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import ControlError, validate_identifier

SCHEMA = "labflow.execution/v1"
CONNECT_TEST_SCHEMA = "labflow.connect-test/v1"
PHASES = {"waiting", "preparing", "ready", "active", "idle", "finishing", "finished", "failed", "retired"}
LAB_CONFIG_SCHEMA = "labflow.lab/v1"
TITLE = re.compile(r"[a-z0-9][a-z0-9._-]*@[1-9][0-9]*\Z")


def now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def validate_title(value: str) -> str:
    if not isinstance(value, str) or not TITLE.fullmatch(value) or ".." in value:
        raise ControlError(f"invalid execution title: {value!r}", 64)
    return value


def execution_root(lab_root: Path, title: str) -> Path:
    return lab_root.resolve() / "control" / validate_title(title)


def workspace_root(lab_root: Path, title: str) -> Path:
    return lab_root.resolve() / "ws" / validate_title(title)


def archive_root(lab_root: Path, title: str) -> Path:
    return lab_root.resolve() / "archive" / validate_title(title)


def lab_link_path(repo: Path, lab_name: str) -> Path:
    validate_identifier(lab_name, "lab-name")
    return repo.resolve() / ".labs" / lab_name


def create_lab_config(repo: Path, lab_name: str, port: int, root: Path) -> dict[str, Any]:
    validate_identifier(lab_name, "lab-name")
    if not 1 <= port <= 65535:
        raise ControlError("port must be from 1 through 65535", 64)
    lab_root = root.resolve()
    if not lab_root.is_absolute() or not lab_root.is_dir():
        raise ControlError("lab root must be an existing absolute directory", 66)
    host_workspace = repo.resolve()
    path = lab_link_path(host_workspace, lab_name)
    if path.exists() or path.is_symlink():
        value = load_lab_config(repo, lab_name)
        if value != {
            "schema": LAB_CONFIG_SCHEMA,
            "name": lab_name,
            "port": port,
            "host_workspace": str(host_workspace),
            "root": str(lab_root),
        }:
            raise ControlError(f"lab {lab_name} is already configured differently")
        return value
    value = {
        "schema": LAB_CONFIG_SCHEMA,
        "name": lab_name,
        "port": port,
        "host_workspace": str(host_workspace),
    }
    atomic_json(lab_root / "config.json", value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.symlink_to(lab_root, target_is_directory=True)
    return {**value, "root": str(lab_root)}


def load_lab_config(repo: Path, lab_name: str) -> dict[str, Any]:
    host_workspace = repo.resolve()
    path = lab_link_path(host_workspace, lab_name)
    try:
        if not path.is_symlink():
            raise FileNotFoundError(path)
        lab_root = path.resolve(strict=True)
        value = json.loads((lab_root / "config.json").read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ControlError(
            f"missing lab {lab_name}; run labflow lab run {lab_name} before continuing",
            75,
        ) from None
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlError(f"invalid lab configuration: {exc}") from None
    if (
        not isinstance(value, dict)
        or set(value) != {"schema", "name", "port", "host_workspace"}
        or value.get("schema") != LAB_CONFIG_SCHEMA
        or value.get("name") != lab_name
        or not isinstance(value.get("port"), int)
        or not 1 <= value["port"] <= 65535
        or value.get("host_workspace") != str(host_workspace)
        or not lab_root.is_dir()
    ):
        raise ControlError("invalid lab configuration")
    return {**value, "root": str(lab_root)}


def remove_lab_config(repo: Path, lab_name: str, expected: dict[str, Any]) -> None:
    path = lab_link_path(repo, lab_name)
    if not path.is_symlink():
        return
    expected_root = expected.get("root")
    if isinstance(expected_root, str) and path.resolve(strict=False) == Path(expected_root):
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


def bind_plan(lab_root: Path, plan_id: str, title: str) -> Path:
    root = execution_root(lab_root, title); root.mkdir(parents=True, exist_ok=True)
    binding = root / "plan"; expected = f"{plan_id}\n"
    if binding.exists():
        if binding.read_text(encoding="utf-8") != expected:
            raise ControlError(f"execution {title} is bound to another plan")
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
    validate_title(data.get("title"))
    binding = (root / "plan").read_text(encoding="utf-8")
    if binding != f"{data.get('plan_id')}\n": raise ControlError("execution plan identity mismatch")
    return data


def save_state(root: Path, state: dict[str, Any]) -> None:
    atomic_json(root / "state.json", state)
