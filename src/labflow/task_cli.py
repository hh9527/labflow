#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import sqlite3
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

from .config import ControlError


SCHEMA = "labflow.workflow/v1"
TASK_SCHEMA = "labflow.task-attempt/v1"
WORD = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")
NAME = re.compile(
    r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*"
    r"(?:\.[a-z][a-z0-9]*(?:-[a-z0-9]+)*)*\Z"
)


class TaskError(Exception):
    def __init__(self, message: str, code: int = 65):
        super().__init__(message)
        self.code = code


def _word(value: Any, where: str) -> str:
    if not isinstance(value, str) or not WORD.fullmatch(value):
        raise TaskError(f"invalid {where}: {value!r}")
    return value


def _words(value: Any, where: str) -> list[str]:
    if not isinstance(value, list):
        raise TaskError(f"{where} must be an id array")
    return [_word(item, where) for item in value]


def _name(value: Any, where: str) -> str:
    if not isinstance(value, str) or not NAME.fullmatch(value):
        raise TaskError(f"invalid {where}: {value!r}")
    return value


def _asset_path(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise TaskError(f"{where} path must be nonempty")
    directory = value.endswith("/")
    normalized = value[:-1] if directory else value
    path = PurePosixPath(normalized)
    if (not normalized or path.is_absolute()
            or any(part in ("", ".", "..") for part in normalized.split("/"))):
        raise TaskError(f"unsafe asset path: {value!r}")
    return f"{normalized}/" if directory else normalized


def _assets(value: Any, where: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise TaskError(f"{where} must be an asset array")
    result = []
    seen = set()
    for item in value:
        if not isinstance(item, str):
            raise TaskError(f"{where} must contain paths")
        path = _asset_path(item, where)
        if path in seen:
            raise TaskError(f"duplicate asset path: {path}")
        seen.add(path)
        result.append({"path": path})
    return result


def _keys(value: dict[str, Any], allowed: set[str], where: str) -> None:
    unknown = set(value) - allowed
    if unknown:
        raise TaskError(f"unknown {where} key(s): {', '.join(sorted(unknown))}")


def _artifact_ref(value: Any, where: str) -> tuple[str, bool]:
    if not isinstance(value, str) or not value:
        raise TaskError(f"{where} must be an artifact id")
    optional = value.endswith("?")
    return _name(value[:-1] if optional else value, where), optional


def session_qualification_role(name: str) -> str | None:
    parts = name.split(".")
    return parts[-1] if len(parts) >= 3 and parts[-2] == "sess" else None


def is_session_qualification(name: str) -> bool:
    return session_qualification_role(name) is not None


def _artifact_owner(name: str, roles: list[str]) -> str:
    suffix = name.rsplit(".", 1)[-1] if "." in name else None
    return suffix if suffix in roles else "host"


def validate_workflow(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TaskError("workflow must be an object")
    _keys(value, {"schema", "roles", "artifacts"},
          "workflow")
    if value.get("schema") != SCHEMA:
        raise TaskError("unsupported workflow schema")
    roles = _words(value.get("roles", []), "workflow roles")
    if not roles or len(set(roles)) != len(roles):
        raise TaskError("workflow roles must be a nonempty unique id array")
    raw_artifacts = value.get("artifacts")
    if not isinstance(raw_artifacts, dict) or not raw_artifacts:
        raise TaskError("workflow artifacts must be a nonempty object")

    artifacts: dict[str, dict[str, Any]] = {}
    for raw_name, raw in raw_artifacts.items():
        name = _name(raw_name, "artifact id")
        if not isinstance(raw, dict):
            raise TaskError(f"artifact {name} must be an object")
        _keys(raw, {"id", "desc", "requires", "inputs", "assets", "check", "instruction"},
              f"artifact {name}")
        if raw.get("id", name) != name:
            raise TaskError(f"artifact id does not match its key: {name}")
        description = raw.get("desc")
        if not isinstance(description, str) or not description.strip():
            raise TaskError(f"artifact {name} desc must be nonempty")
        raw_requires = raw.get("requires", [])
        if not isinstance(raw_requires, list):
            raise TaskError(f"artifact {name} requires must be an artifact id array")
        requires = []
        seen = set()
        for item in raw_requires:
            if isinstance(item, dict):
                _keys(item, {"id", "optional"}, f"artifact {name} requires")
                dependency = _name(item.get("id"), f"artifact {name} requires")
                optional = item.get("optional")
                if not isinstance(optional, bool):
                    raise TaskError(f"artifact {name} requires optional must be boolean")
            else:
                dependency, optional = _artifact_ref(item, f"artifact {name} requires")
            if dependency in seen:
                raise TaskError(f"artifact {name} has duplicate requirement: {dependency}")
            seen.add(dependency)
            requires.append({"id": dependency, "optional": optional})
        owner = _artifact_owner(name, roles)
        qualification_role = session_qualification_role(name)
        if qualification_role is not None and qualification_role not in roles:
            raise TaskError(
                f"session qualification {name} names unknown role: {qualification_role}"
            )
        instruction = raw.get("instruction")
        if owner != "host" and (not isinstance(instruction, str) or not instruction.strip()):
            raise TaskError(f"role-owned artifact {name} instruction must be nonempty")
        if owner == "host" and instruction is not None:
            raise TaskError(f"Host-owned artifact {name} cannot have an instruction")
        artifacts[name] = {
            "id": name,
            "desc": description,
            "owner": owner,
            "requires": requires,
            "inputs": (_assets(raw["inputs"], f"artifact {name} inputs")
                       if "inputs" in raw else None),
            "assets": _assets(raw.get("assets", []), f"artifact {name} assets"),
            "check": _assets(raw.get("check", []), f"artifact {name} check"),
            "instruction": instruction,
        }

    for artifact in artifacts.values():
        for dependency in artifact["requires"]:
            if dependency["id"] not in artifacts:
                raise TaskError(
                    f"artifact {artifact['id']} has unknown requirement: {dependency['id']}"
                )
            qualification_role = session_qualification_role(dependency["id"])
            if qualification_role is not None:
                if dependency["optional"]:
                    raise TaskError(
                        f"session qualification requirement cannot be optional: {dependency['id']}"
                    )
                if artifact["owner"] != qualification_role:
                    raise TaskError(
                        f"session qualification {dependency['id']} can only gate "
                        f"artifacts owned by {qualification_role}"
                    )

    for artifact in artifacts.values():
        if artifact["inputs"] is not None:
            continue
        inferred: dict[str, dict[str, Any]] = {}
        for dependency in artifact["requires"]:
            for asset in artifacts[dependency["id"]]["assets"]:
                inferred.setdefault(asset["path"], dict(asset))
        artifact["inputs"] = list(inferred.values())

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise TaskError(f"artifact dependency cycle at: {name}")
        if name in visited:
            return
        visiting.add(name)
        for dependency in artifacts[name]["requires"]:
            if not dependency["optional"]:
                visit(dependency["id"])
        visiting.remove(name)
        visited.add(name)

    for name in artifacts:
        visit(name)

    return {
        "schema": SCHEMA,
        "roles": roles,
        "artifacts": artifacts,
    }


def load_workflow(root: Path) -> dict[str, Any]:
    try:
        manifest = json.loads(
            (root / ".labflow-exec" / "runtime.json").read_text(encoding="utf-8")
        )
    except FileNotFoundError:
        raise TaskError(f"missing .labflow-exec/runtime.json under {root}", 66) from None
    except (OSError, json.JSONDecodeError) as exc:
        raise TaskError(f"invalid execution runtime: {exc}") from None
    if not isinstance(manifest, dict) or manifest.get("schema") != "labflow.project-runtime/v1":
        raise TaskError("invalid project runtime schema")
    return validate_workflow(manifest.get("workflow"))


def role_asset_permissions(workflow: dict[str, Any], role: str) -> dict[str, list[str]]:
    if role not in workflow["roles"]:
        raise TaskError(f"unknown workflow role: {role}", 64)
    read: dict[str, None] = {}
    write: dict[str, None] = {}
    for artifact in workflow["artifacts"].values():
        if artifact["owner"] != role:
            continue
        for asset in artifact["assets"]:
            write.setdefault(asset["path"], None)
            read.setdefault(asset["path"], None)
        for asset in artifact.get("inputs", []):
            read.setdefault(asset["path"], None)
    return {"read": list(read), "write": list(write)}


def artifact_asset_permissions(
    workflow: dict[str, Any], name: str,
) -> dict[str, list[str]]:
    artifact = workflow["artifacts"].get(name)
    if artifact is None:
        raise TaskError(f"unknown artifact: {name}", 64)
    read: dict[str, None] = {}
    write: dict[str, None] = {}
    for asset in artifact["assets"]:
        write.setdefault(asset["path"], None)
        read.setdefault(asset["path"], None)
    for asset in artifact["inputs"]:
        read.setdefault(asset["path"], None)
    return {"read": list(read), "write": list(write)}


def find_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in (current, *current.parents):
        if (candidate / "labflow-plan.toml").is_file() \
                and (candidate / ".labflow-exec" / "runtime.json").is_file():
            return candidate
    raise TaskError("cannot find a prepared Labflow project from current directory", 66)


def _atomic_write(path: Path, content: bytes, minimum_ns: int = 0) -> int:
    previous = path.stat().st_mtime_ns if path.exists() else 0
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        stamp = max(time.time_ns(), previous + 1, minimum_ns + 1)
        os.utime(temporary, ns=(stamp, stamp))
        os.replace(temporary, path)
        return path.stat().st_mtime_ns
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextmanager
def _locked(root: Path) -> Iterator[None]:
    state = _task_root(root)
    state.mkdir(exist_ok=True)
    if state.name == ".labflow-exec":
        yield
        return
    with (state / "lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _asset_state(root: Path, assets: list[dict[str, Any]]) -> dict[str, Any]:
    missing = []
    invalid = []
    for asset in assets:
        name = asset["path"]
        path = root / name.rstrip("/")
        if not path.exists():
            missing.append(name)
        elif path.is_symlink() or (name.endswith("/") and not path.is_dir()) \
                or (not name.endswith("/") and not path.is_file()):
            invalid.append(name)
    return {"ready": not missing and not invalid, "missing": missing, "invalid": invalid}


def _artifact_path(root: Path, name: str) -> Path:
    project_control = root / ".labflow-exec"
    if project_control.is_dir():
        return project_control / "artifacts" / name
    # Standalone workflow evaluation keeps the same timestamp semantics without
    # pretending that the workspace belongs to a laboratory execution.
    return root / "artifacts" / name


def _task_root(root: Path) -> Path:
    project_control = root / ".labflow-exec"
    if project_control.is_dir():
        return project_control
    return root / "tasks"


def _task_database(root: Path) -> Path:
    return _task_root(root) / "states.sqlite"


@contextmanager
def _task_connection(root: Path) -> Iterator[sqlite3.Connection]:
    path = _task_database(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=5)
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("""
        CREATE TABLE IF NOT EXISTS task_records (
            kind TEXT NOT NULL CHECK (kind IN ('active', 'history')),
            identity TEXT NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY (kind, identity)
        )
    """)
    try:
        yield connection
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _read_active(root: Path, role: str) -> dict[str, Any] | None:
    with _task_connection(root) as connection:
        row = connection.execute(
            "SELECT payload FROM task_records WHERE kind = 'active' AND identity = ?",
            (role,),
        ).fetchone()
    if row is None:
        return None
    try:
        value = json.loads(row[0])
    except json.JSONDecodeError as exc:
        raise TaskError(f"invalid active task record for {role}: {exc}") from None
    if not isinstance(value, dict):
        raise TaskError(f"invalid active task record for {role}")
    return value


def _write_task(root: Path, kind: str, identity: str, value: dict[str, Any]) -> None:
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    with _task_connection(root) as connection:
        connection.execute(
            "INSERT OR REPLACE INTO task_records(kind, identity, payload) VALUES (?, ?, ?)",
            (kind, identity, payload),
        )


def _delete_active(root: Path, role: str) -> None:
    with _task_connection(root) as connection:
        connection.execute(
            "DELETE FROM task_records WHERE kind = 'active' AND identity = ?", (role,),
        )


def _last_task_end(root: Path, role: str) -> int:
    with _task_connection(root) as connection:
        rows = connection.execute(
            "SELECT payload FROM task_records WHERE kind = 'history'"
        ).fetchall()
    ended = 0
    for (payload,) in rows:
        try:
            task = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if not isinstance(task, dict) or task.get("role") != role:
            continue
        value = task.get("submitted_at_ns") or task.get("ended_at_ns")
        if isinstance(value, int):
            ended = max(ended, value)
    return ended


def _asset_mtime_ns(root: Path, name: str) -> int:
    path = root / name.rstrip("/")
    try:
        latest = path.stat().st_mtime_ns
    except OSError:
        return 0
    if not path.is_dir():
        return latest
    try:
        for item in path.rglob("*"):
            try:
                latest = max(latest, item.stat().st_mtime_ns)
            except OSError:
                continue
    except OSError:
        pass
    return latest


def _task_response(root: Path, workflow: dict[str, Any], status: dict[str, Any],
                   task: dict[str, Any]) -> dict[str, Any]:
    name = task["artifacts"][0]
    target_stamp = status["artifacts"][name]["stamp_mtime_ns"]
    previous_end = _last_task_end(root, task["role"])
    inputs = []
    assets: dict[str, bool] = {}
    for reference in workflow["artifacts"][name]["requires"]:
        dependency = status["artifacts"][reference["id"]]
        stamp = dependency["stamp_mtime_ns"]
        fresh = None if reference["optional"] and not stamp else stamp > target_stamp
        inputs.append({"name": reference["id"], "fresh": fresh})
    for asset in workflow["artifacts"][name].get("inputs", []):
        assets[asset["path"]] = _asset_mtime_ns(root, asset["path"]) > previous_end

    return {
        "target": {"name": name, "instruction": workflow["artifacts"][name]["instruction"]},
        "requires": inputs,
        "inputs": [{"path": path, "updated": updated} for path, updated in assets.items()],
    }


def task_records(root: Path) -> dict[str, list[dict[str, Any]]]:
    with _task_connection(root) as connection:
        rows = connection.execute(
            "SELECT kind, payload FROM task_records ORDER BY identity"
        ).fetchall()
    active = [json.loads(payload) for kind, payload in rows if kind == "active"]
    history = [json.loads(payload) for kind, payload in rows if kind == "history"]
    history.sort(key=lambda item: (item.get("started_at_ns", 0), item.get("task_id", "")))
    return {"active": active, "history": history}


def _task_inputs_current(status: dict[str, Any], task: dict[str, Any]) -> bool:
    inputs = task.get("inputs")
    artifacts = task.get("artifacts")
    return bool(isinstance(inputs, dict) and isinstance(artifacts, list) and all(
        name in status["artifacts"]
        and status["artifacts"][name]["input_mtime_ns"] == inputs.get(name)
        for name in artifacts
    ))


def _archive_active(root: Path, task: dict[str, Any], status: str,
                    reason: str, ended_at_ns: int | None = None) -> dict[str, Any]:
    archived = dict(task)
    archived.update({
        "status": status,
        "ended_at_ns": time.time_ns() if ended_at_ns is None else ended_at_ns,
        "reason": reason,
    })
    _write_task(root, "history", task["task_id"], archived)
    _delete_active(root, task["role"])
    return archived


def supersede_role_task(root: Path, role: str, reason: str) -> dict[str, Any] | None:
    with _locked(root):
        task = _read_active(root, role)
        if task is None:
            return None
        return _archive_active(root, task, "stale", reason)


def evaluate(root: Path, workflow: dict[str, Any]) -> dict[str, Any]:
    artifacts = workflow["artifacts"]
    values: dict[str, dict[str, Any]] = {}

    def evaluate_one(name: str) -> dict[str, Any]:
        if name in values:
            return values[name]
        artifact = artifacts[name]
        dependencies = []
        for reference in artifact["requires"]:
            if reference["optional"]:
                marker = _artifact_path(root, reference["id"])
                stamp = marker.stat().st_mtime_ns if marker.is_file() else 0
                dependency = {"stamp_mtime_ns": stamp, "current": bool(stamp)}
            else:
                dependency = evaluate_one(reference["id"])
            dependencies.append((reference, dependency))
        assets = _asset_state(root, artifact["assets"])
        checks = _asset_state(root, artifact.get("check", []))
        path = _artifact_path(root, name)
        stamp = path.stat().st_mtime_ns if path.is_file() else 0
        data_dependencies = [
            (reference, dependency) for reference, dependency in dependencies
            if not is_session_qualification(reference["id"])
        ]
        qualification_dependencies = [
            (reference, dependency) for reference, dependency in dependencies
            if is_session_qualification(reference["id"])
        ]
        input_mtime = max((
            dependency["stamp_mtime_ns"] for reference, dependency in data_dependencies
            if not reference["optional"] or dependency["stamp_mtime_ns"]
        ), default=0)
        data_blocked_by = [reference["id"] for reference, dependency in data_dependencies
                           if not reference["optional"] and not dependency["current"]]
        current = bool(
            stamp and assets["ready"] and checks["ready"]
            and not data_blocked_by and stamp > input_mtime
        )
        missing_qualifications = [
            reference["id"] for reference, dependency in qualification_dependencies
            if not dependency["current"]
        ]
        blocked_by = data_blocked_by + ([] if current else missing_qualifications)
        ready = not blocked_by
        values[name] = {
            "id": name,
            "owner": artifact["owner"],
            "description": artifact["desc"],
            "current": current,
            "runnable": bool(artifact["owner"] != "host" and ready and not current),
            "submittable": bool(artifact["owner"] == "host" and ready and assets["ready"]
                               and checks["ready"] and not current),
            "stamp_mtime_ns": stamp,
            "input_mtime_ns": input_mtime,
            "blocked_by": blocked_by,
            "missing_qualifications": missing_qualifications,
            "assets": assets,
            "checks": checks,
        }
        return values[name]

    for name in artifacts:
        evaluate_one(name)
    return {"schema": "labflow.artifact-status/v1", "artifacts": values}


def workflow_status(root: Path, workflow: dict[str, Any]) -> dict[str, Any]:
    with _locked(root):
        return evaluate(root, workflow)


def refresh_artifact(root: Path, workflow: dict[str, Any], name: str, *, force: bool = False) -> dict[str, Any]:
    artifact = workflow["artifacts"].get(name)
    if artifact is None:
        raise TaskError(f"unknown artifact: {name}", 64)
    if artifact["owner"] != "host" and not force:
        raise TaskError(f"role-owned artifact cannot be refreshed by Host: {name}", 64)
    if force and is_session_qualification(name):
        raise TaskError(f"Host cannot refresh a session qualification: {name}", 64)
    with _locked(root):
        value = evaluate(root, workflow)["artifacts"][name]
        blocked_by = [
            dependency for dependency in value["blocked_by"]
            if not force or dependency not in value["missing_qualifications"]
        ]
        if blocked_by:
            raise TaskError(f"artifact inputs are incomplete: {', '.join(blocked_by)}", 75)
        if not value["assets"]["ready"] or not value["checks"]["ready"]:
            raise TaskError(f"artifact assets are incomplete: {name}", 75)
        stamp = _atomic_write(_artifact_path(root, name), b"", value["input_mtime_ns"])
        if force and artifact["owner"] != "host":
            active = _read_active(root, artifact["owner"])
            if active is not None:
                _archive_active(root, active, "stale",
                                f"Host force-refreshed {name}")
    return {"schema": "labflow.artifact/v1", "artifact": name, "mtime_ns": stamp,
            "host_forced": force}


def remove_artifact(root: Path, workflow: dict[str, Any], name: str, *, force: bool = False) -> dict[str, Any]:
    artifact = workflow["artifacts"].get(name)
    if artifact is None:
        raise TaskError(f"unknown artifact: {name}", 64)
    if artifact["owner"] != "host" and not force:
        raise TaskError(f"role-owned artifact cannot be removed by Host: {name}", 64)
    with _locked(root):
        path = _artifact_path(root, name)
        existed = path.is_file()
        path.unlink(missing_ok=True)
        if force and artifact["owner"] != "host":
            active = _read_active(root, artifact["owner"])
            if active is not None:
                _archive_active(root, active, "stale",
                                f"Host force-removed {name}")
    return {"schema": "labflow.artifact/v1", "artifact": name,
            "removed": True, "existed": existed, "host_forced": force}


def restore_artifacts(root: Path, workflow: dict[str, Any], names: list[str]) -> list[dict[str, Any]]:
    """Restore trusted artifact state in dependency order without creating task history."""
    requested = set(names)
    unknown = requested - set(workflow["artifacts"])
    if unknown:
        raise TaskError(f"unknown artifact(s): {', '.join(sorted(unknown))}", 64)
    qualifications = sorted(name for name in requested if is_session_qualification(name))
    if qualifications:
        raise TaskError(
            f"cannot restore session qualifications: {', '.join(qualifications)}", 64
        )
    restored = []
    with _locked(root):
        pending = set(requested)
        while pending:
            progressed = False
            status = evaluate(root, workflow)["artifacts"]
            for name in workflow["artifacts"]:
                if name not in pending:
                    continue
                value = status[name]
                data_blocked_by = [
                    dependency for dependency in value["blocked_by"]
                    if dependency not in value["missing_qualifications"]
                ]
                if data_blocked_by:
                    continue
                if not value["assets"]["ready"] or not value["checks"]["ready"]:
                    raise TaskError(f"cannot restore {name}; assets are incomplete", 75)
                stamp = _atomic_write(_artifact_path(root, name), b"", value["input_mtime_ns"])
                restored.append({"artifact": name, "mtime_ns": stamp})
                pending.remove(name)
                progressed = True
            if not progressed:
                blocked = evaluate(root, workflow)["artifacts"]
                details = "; ".join(
                    f"{name}: {', '.join(
                        dependency for dependency in blocked[name]['blocked_by']
                        if dependency not in blocked[name]['missing_qualifications']
                    )}" for name in sorted(pending)
                )
                raise TaskError(f"cannot restore artifacts; inputs are incomplete: {details}", 75)
    return restored


def clear_session_qualifications(
    root: Path, workflow: dict[str, Any], role: str,
) -> list[str]:
    """Invalidate knowledge tied to a replaced role Session."""
    if role not in workflow["roles"]:
        raise TaskError(f"unknown workflow role: {role}", 64)
    names = [
        name for name in workflow["artifacts"]
        if session_qualification_role(name) == role
    ]
    with _locked(root):
        removed = []
        for name in names:
            path = _artifact_path(root, name)
            if path.is_file():
                path.unlink()
                removed.append(name)
        active = _read_active(root, role)
        if active is not None:
            _archive_active(root, active, "stale", "role Session was replaced")
    return removed


def assign_task(root: Path, workflow: dict[str, Any], role: str,
                preferred: str) -> dict[str, Any] | None:
    """Atomically create or reuse one active Task for Supervisor delivery."""
    if role not in workflow["roles"]:
        raise TaskError(f"unknown workflow role: {role}", 64)
    artifact = workflow["artifacts"].get(preferred)
    if artifact is None:
        raise TaskError(f"unknown artifact: {preferred}", 64)
    if artifact["owner"] != role:
        raise TaskError(f"artifact is not owned by {role}: {preferred}", 64)
    with _locked(root):
        status = evaluate(root, workflow)
        active = _read_active(root, role)
        if active is not None:
            if _task_inputs_current(status, active):
                return _task_response(root, workflow, status, active)
            _archive_active(root, active, "stale",
                            "artifact inputs changed before task delivery")
        if not status["artifacts"][preferred]["runnable"]:
            return None
        started = time.time_ns()
        task = {
            "schema": TASK_SCHEMA,
            "task_id": f"{role}-{started}",
            "role": role,
            "artifacts": [preferred],
            "inputs": {preferred: status["artifacts"][preferred]["input_mtime_ns"]},
            "started_at_ns": started,
            "status": "active",
        }
        _write_task(root, "active", role, task)
        return _task_response(root, workflow, status, task)


def submit(root: Path, workflow: dict[str, Any], role: str, names: list[str]) -> dict[str, Any]:
    if role not in workflow["roles"]:
        raise TaskError(f"unknown workflow role: {role}", 64)
    if not names or len(set(names)) != len(names):
        raise TaskError("submit requires unique artifact ids", 64)
    with _locked(root):
        task = _read_active(root, role)
        if task is None:
            raise TaskError(f"role has no active assigned task: {role}", 75)
        expected = task.get("artifacts")
        if not isinstance(expected, list) or set(names) != set(expected) or len(names) != len(expected):
            raise TaskError(f"submit must contain the complete assigned task: {', '.join(expected or [])}", 64)
        status = evaluate(root, workflow)
        if not _task_inputs_current(status, task):
            _archive_active(root, task, "stale",
                            "artifact inputs changed after task assignment")
            raise TaskError("artifact inputs changed after task assignment", 75)
        values = []
        for name in names:
            artifact = workflow["artifacts"].get(name)
            if artifact is None:
                raise TaskError(f"unknown artifact: {name}", 64)
            if artifact["owner"] != role:
                raise TaskError(f"artifact is not owned by {role}: {name}", 64)
            value = status["artifacts"][name]
            if not value["runnable"]:
                raise TaskError(f"artifact is not runnable: {name}", 75)
            if not value["assets"]["ready"] or not value["checks"]["ready"]:
                raise TaskError(f"artifact assets are incomplete: {name}", 75)
            values.append((name, value))
        refreshed = [{"artifact": name, "mtime_ns": _atomic_write(
            _artifact_path(root, name), b"", value["input_mtime_ns"]
        )} for name, value in values]
        completed = dict(task)
        completed.update({"status": "submitted", "submitted_at_ns": time.time_ns(),
                          "artifacts_refreshed": refreshed})
        _write_task(root, "history", task["task_id"], completed)
        _delete_active(root, role)
    return {"schema": "labflow.agent-submit/v1", "role": role,
            "task_id": task["task_id"], "artifacts": refreshed}


def parser(prog: str = "labflow agent") -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog=prog, description="Work inside a Labflow execution.")
    value.add_argument("--root", type=Path)
    commands = value.add_subparsers(dest="command", required=True)
    commands.add_parser("status")
    return value


def main(argv: list[str] | None = None, *, prog: str = "labflow agent") -> int:
    args = parser(prog).parse_args(argv)
    try:
        root = args.root.resolve() if args.root else find_root(Path.cwd())
        workflow = load_workflow(root)
        result = workflow_status(root, workflow)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (TaskError, ControlError) as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return exc.code
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
