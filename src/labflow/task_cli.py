#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

from .config import ControlError


SCHEMA = "labflow.workflow/v1"
TASK_SCHEMA = "labflow.task-attempt/v1"
DEFAULT_PULL_TIMEOUT = 60.0
IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]*\Z")


class TaskError(Exception):
    def __init__(self, message: str, code: int = 65):
        super().__init__(message)
        self.code = code


def _id(value: Any, where: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise TaskError(f"invalid {where}: {value!r}")
    return value


def _ids(value: Any, where: str) -> list[str]:
    if not isinstance(value, list):
        raise TaskError(f"{where} must be an id array")
    return [_id(item, where) for item in value]


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
        if isinstance(item, str):
            path, level = _asset_path(item, where), 2
        elif isinstance(item, dict):
            _keys(item, {"path", "level"}, where)
            path = _asset_path(item.get("path"), where)
            level = item.get("level", 2)
            if isinstance(level, bool) or not isinstance(level, int) or level not in (0, 1, 2):
                raise TaskError(f"{where} level must be 0, 1, or 2")
        else:
            raise TaskError(f"{where} must contain paths or asset objects")
        if path in seen:
            raise TaskError(f"duplicate asset path: {path}")
        seen.add(path)
        result.append({"path": path, "level": level})
    return result


def _keys(value: dict[str, Any], allowed: set[str], where: str) -> None:
    unknown = set(value) - allowed
    if unknown:
        raise TaskError(f"unknown {where} key(s): {', '.join(sorted(unknown))}")


def _artifact_ref(value: Any, where: str) -> tuple[str, bool]:
    if not isinstance(value, str) or not value:
        raise TaskError(f"{where} must be an artifact id")
    optional = value.endswith("?")
    return _id(value[:-1] if optional else value, where), optional


def _artifact_owner(name: str, roles: list[str]) -> str:
    return next((role for role in roles if name.endswith(f".{role}")), "host")


def validate_workflow(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TaskError("workflow must be an object")
    _keys(value, {"schema", "roles", "artifacts"},
          "workflow")
    if value.get("schema") != SCHEMA:
        raise TaskError("unsupported workflow schema")
    roles = _ids(value.get("roles", []), "workflow roles")
    if not roles or len(set(roles)) != len(roles):
        raise TaskError("workflow roles must be a nonempty unique id array")
    raw_artifacts = value.get("artifacts")
    if not isinstance(raw_artifacts, dict) or not raw_artifacts:
        raise TaskError("workflow artifacts must be a nonempty object")

    artifacts: dict[str, dict[str, Any]] = {}
    for raw_name, raw in raw_artifacts.items():
        name = _id(raw_name, "artifact id")
        if not isinstance(raw, dict):
            raise TaskError(f"artifact {name} must be an object")
        _keys(raw, {"id", "desc", "input", "assets", "instruction"}, f"artifact {name}")
        if raw.get("id", name) != name:
            raise TaskError(f"artifact id does not match its key: {name}")
        description = raw.get("desc")
        if not isinstance(description, str) or not description.strip():
            raise TaskError(f"artifact {name} desc must be nonempty")
        raw_inputs = raw.get("input", [])
        if not isinstance(raw_inputs, list):
            raise TaskError(f"artifact {name} input must be an artifact id array")
        inputs = []
        seen = set()
        for item in raw_inputs:
            if isinstance(item, dict):
                _keys(item, {"id", "optional"}, f"artifact {name} input")
                dependency = _id(item.get("id"), f"artifact {name} input")
                optional = item.get("optional")
                if not isinstance(optional, bool):
                    raise TaskError(f"artifact {name} input optional must be boolean")
            else:
                dependency, optional = _artifact_ref(item, f"artifact {name} input")
            if dependency in seen:
                raise TaskError(f"artifact {name} has duplicate input: {dependency}")
            seen.add(dependency)
            inputs.append({"id": dependency, "optional": optional})
        owner = _artifact_owner(name, roles)
        instruction = raw.get("instruction")
        if owner != "host" and (not isinstance(instruction, str) or not instruction.strip()):
            raise TaskError(f"role-owned artifact {name} instruction must be nonempty")
        if owner == "host" and instruction is not None:
            raise TaskError(f"Host-owned artifact {name} cannot have an instruction")
        artifacts[name] = {
            "id": name,
            "desc": description,
            "owner": owner,
            "input": inputs,
            "assets": _assets(raw.get("assets", []), f"artifact {name} assets"),
            "instruction": instruction,
        }

    for artifact in artifacts.values():
        for dependency in artifact["input"]:
            if dependency["id"] not in artifacts:
                raise TaskError(f"artifact {artifact['id']} has unknown input: {dependency['id']}")

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise TaskError(f"artifact dependency cycle at: {name}")
        if name in visited:
            return
        visiting.add(name)
        for dependency in artifacts[name]["input"]:
            visit(dependency["id"])
        visiting.remove(name)
        visited.add(name)

    for name in artifacts:
        visit(name)

    asset_levels: dict[str, int] = {}
    for artifact in artifacts.values():
        for asset in artifact["assets"]:
            previous = asset_levels.setdefault(asset["path"], asset["level"])
            if previous != asset["level"]:
                raise TaskError(f"asset level differs across artifacts: {asset['path']}")
    return {
        "schema": SCHEMA,
        "roles": roles,
        "artifacts": artifacts,
    }


def load_workflow(root: Path) -> dict[str, Any]:
    runtime = root.parent if root.name == "ws" else root
    try:
        manifest = json.loads((runtime / "experiment.json").read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise TaskError(f"missing experiment.json under {root}", 66) from None
    except (OSError, json.JSONDecodeError) as exc:
        raise TaskError(f"invalid experiment.json: {exc}") from None
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
        for reference in artifact["input"]:
            for asset in workflow["artifacts"][reference["id"]]["assets"]:
                read.setdefault(asset["path"], None)
    return {"read": list(read), "write": list(write)}


def find_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in (current, *current.parents):
        if (candidate / "experiment.json").is_file():
            workspace = candidate / "ws"
            return workspace if workspace.is_dir() and current.is_relative_to(workspace) else candidate
    raise TaskError("cannot find experiment.json from current directory", 66)


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
    execution = root.parent
    if root.name == "ws" and (execution / ".labflow-plan").is_file():
        return execution / "artifacts" / name
    # Standalone workflow evaluation keeps the same timestamp semantics without
    # pretending that the workspace belongs to a laboratory execution.
    return root / "artifacts" / name


def _task_root(root: Path) -> Path:
    execution = root.parent
    if root.name == "ws" and (execution / ".labflow-plan").is_file():
        return execution / "tasks"
    return root / "tasks"


def _active_task_path(root: Path, role: str) -> Path:
    return _task_root(root) / "active" / f"{role}.json"


def _task_history_path(root: Path, task_id: str) -> Path:
    return _task_root(root) / "history" / f"{task_id}.json"


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise TaskError(f"invalid task record {path}: {exc}") from None
    if not isinstance(value, dict):
        raise TaskError(f"invalid task record {path}")
    return value


def _write_json(path: Path, value: dict[str, Any]) -> None:
    _atomic_write(path, (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode())


def _task_response(workflow: dict[str, Any], status: dict[str, Any], task: dict[str, Any]) -> dict[str, Any]:
    name = task["artifacts"][0]
    target_stamp = status["artifacts"][name]["stamp_mtime_ns"]
    inputs = []
    assets: dict[str, bool] = {}
    for reference in workflow["artifacts"][name]["input"]:
        dependency = status["artifacts"][reference["id"]]
        stamp = dependency["stamp_mtime_ns"]
        fresh = None if reference["optional"] and not stamp else stamp > target_stamp
        inputs.append({"name": reference["id"], "fresh": fresh})
        if stamp:
            for asset in workflow["artifacts"][reference["id"]]["assets"]:
                assets[asset["path"]] = assets.get(asset["path"], False) or bool(fresh)

    return {
        "target": {"name": name},
        "inputs": inputs,
        "assets": [{"path": path, "updated": updated} for path, updated in assets.items()],
    }


def task_records(root: Path) -> dict[str, list[dict[str, Any]]]:
    active = []
    history = []
    for path in sorted((_task_root(root) / "active").glob("*.json")):
        value = _read_json(path)
        if value is not None:
            active.append(value)
    for path in sorted((_task_root(root) / "history").glob("*.json")):
        value = _read_json(path)
        if value is not None:
            history.append(value)
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


def _archive_active(root: Path, path: Path, task: dict[str, Any], status: str,
                    reason: str, ended_at_ns: int | None = None) -> dict[str, Any]:
    archived = dict(task)
    archived.update({
        "status": status,
        "ended_at_ns": time.time_ns() if ended_at_ns is None else ended_at_ns,
        "reason": reason,
    })
    _write_json(_task_history_path(root, task["task_id"]), archived)
    path.unlink(missing_ok=True)
    return archived


def supersede_role_task(root: Path, role: str, reason: str) -> dict[str, Any] | None:
    with _locked(root):
        path = _active_task_path(root, role)
        task = _read_json(path)
        if task is None:
            return None
        return _archive_active(root, path, task, "stale", reason)


def evaluate(root: Path, workflow: dict[str, Any]) -> dict[str, Any]:
    artifacts = workflow["artifacts"]
    values: dict[str, dict[str, Any]] = {}

    def evaluate_one(name: str) -> dict[str, Any]:
        if name in values:
            return values[name]
        artifact = artifacts[name]
        dependencies = [(reference, evaluate_one(reference["id"])) for reference in artifact["input"]]
        assets = _asset_state(root, artifact["assets"])
        path = _artifact_path(root, name)
        stamp = path.stat().st_mtime_ns if path.is_file() else 0
        input_mtime = max((dependency["stamp_mtime_ns"] for reference, dependency in dependencies
                           if not reference["optional"] or dependency["stamp_mtime_ns"]), default=0)
        blocked_by = [reference["id"] for reference, dependency in dependencies
                      if (not reference["optional"] and not dependency["current"])
                      or (reference["optional"] and dependency["stamp_mtime_ns"]
                          and not dependency["current"])]
        ready = not blocked_by
        current = bool(stamp and assets["ready"] and ready and stamp > input_mtime)
        values[name] = {
            "id": name,
            "owner": artifact["owner"],
            "description": artifact["desc"],
            "current": current,
            "runnable": bool(artifact["owner"] != "host" and ready and not current),
            "submittable": bool(artifact["owner"] == "host" and ready and assets["ready"] and not current),
            "stamp_mtime_ns": stamp,
            "input_mtime_ns": input_mtime,
            "blocked_by": blocked_by,
            "assets": assets,
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
    with _locked(root):
        value = evaluate(root, workflow)["artifacts"][name]
        if value["blocked_by"]:
            raise TaskError(f"artifact inputs are incomplete: {', '.join(value['blocked_by'])}", 75)
        if not value["assets"]["ready"]:
            raise TaskError(f"artifact assets are incomplete: {name}", 75)
        stamp = _atomic_write(_artifact_path(root, name), b"", value["input_mtime_ns"])
        if force and artifact["owner"] != "host":
            active_path = _active_task_path(root, artifact["owner"])
            active = _read_json(active_path)
            if active is not None:
                _archive_active(root, active_path, active, "stale",
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
            active_path = _active_task_path(root, artifact["owner"])
            active = _read_json(active_path)
            if active is not None:
                _archive_active(root, active_path, active, "stale",
                                f"Host force-removed {name}")
    return {"schema": "labflow.artifact/v1", "artifact": name,
            "removed": True, "existed": existed, "host_forced": force}


def restore_artifacts(root: Path, workflow: dict[str, Any], names: list[str]) -> list[dict[str, Any]]:
    """Restore trusted artifact state in dependency order without creating task history."""
    requested = set(names)
    unknown = requested - set(workflow["artifacts"])
    if unknown:
        raise TaskError(f"unknown artifact(s): {', '.join(sorted(unknown))}", 64)
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
                if value["blocked_by"]:
                    continue
                if not value["assets"]["ready"]:
                    raise TaskError(f"cannot restore {name}; assets are incomplete", 75)
                stamp = _atomic_write(_artifact_path(root, name), b"", value["input_mtime_ns"])
                restored.append({"artifact": name, "mtime_ns": stamp})
                pending.remove(name)
                progressed = True
            if not progressed:
                blocked = evaluate(root, workflow)["artifacts"]
                details = "; ".join(
                    f"{name}: {', '.join(blocked[name]['blocked_by'])}" for name in sorted(pending)
                )
                raise TaskError(f"cannot restore artifacts; inputs are incomplete: {details}", 75)
    return restored


def pull(root: Path, workflow: dict[str, Any], role: str,
         wait: bool, timeout: float | None) -> dict[str, Any] | None:
    if role not in workflow["roles"]:
        raise TaskError(f"unknown workflow role: {role}", 64)
    timeout = DEFAULT_PULL_TIMEOUT if timeout is None else timeout
    if timeout < 0 or timeout > DEFAULT_PULL_TIMEOUT:
        raise TaskError(f"pull timeout must be between 0 and {DEFAULT_PULL_TIMEOUT:g} seconds", 64)
    deadline = time.monotonic() + timeout
    while True:
        with _locked(root):
            status = evaluate(root, workflow)
            active_path = _active_task_path(root, role)
            active = _read_json(active_path)
            if active is not None:
                if _task_inputs_current(status, active):
                    return _task_response(workflow, status, active)
                _archive_active(root, active_path, active, "stale",
                                "artifact inputs changed after pull")
            runnable = [artifact for artifact in workflow["artifacts"].values()
                        if artifact["owner"] == role and status["artifacts"][artifact["id"]]["runnable"]]
            if runnable:
                artifact = runnable[0]
                started = time.time_ns()
                task = {
                    "schema": TASK_SCHEMA,
                    "task_id": f"{role}-{started}",
                    "role": role,
                    "artifacts": [artifact["id"]],
                    "inputs": {
                        artifact["id"]: status["artifacts"][artifact["id"]]["input_mtime_ns"]
                    },
                    "started_at_ns": started,
                    "status": "active",
                }
                _write_json(active_path, task)
                return _task_response(workflow, status, task)
        if not wait or time.monotonic() >= deadline:
            return None
        time.sleep(.2)


def submit(root: Path, workflow: dict[str, Any], role: str, names: list[str]) -> dict[str, Any]:
    if role not in workflow["roles"]:
        raise TaskError(f"unknown workflow role: {role}", 64)
    if not names or len(set(names)) != len(names):
        raise TaskError("submit requires unique artifact ids", 64)
    with _locked(root):
        active_path = _active_task_path(root, role)
        task = _read_json(active_path)
        if task is None:
            raise TaskError(f"role has no active pulled task: {role}", 75)
        expected = task.get("artifacts")
        if not isinstance(expected, list) or set(names) != set(expected) or len(names) != len(expected):
            raise TaskError(f"submit must contain the complete pulled task: {', '.join(expected or [])}", 64)
        status = evaluate(root, workflow)
        if not _task_inputs_current(status, task):
            _archive_active(root, active_path, task, "stale",
                            "artifact inputs changed after pull")
            raise TaskError("artifact inputs changed after pull", 75)
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
            if not value["assets"]["ready"]:
                raise TaskError(f"artifact assets are incomplete: {name}", 75)
            values.append((name, value))
        refreshed = [{"artifact": name, "mtime_ns": _atomic_write(
            _artifact_path(root, name), b"", value["input_mtime_ns"]
        )} for name, value in values]
        completed = dict(task)
        completed.update({"status": "submitted", "submitted_at_ns": time.time_ns(),
                          "artifacts_refreshed": refreshed})
        _write_json(_task_history_path(root, task["task_id"]), completed)
        active_path.unlink(missing_ok=True)
    return {"schema": "labflow.agent-submit/v1", "role": role,
            "task_id": task["task_id"], "artifacts": refreshed}


def parser(prog: str = "labflow agent") -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog=prog, description="Work inside a Labflow execution.")
    value.add_argument("--root", type=Path)
    commands = value.add_subparsers(dest="command", required=True)
    pull_command = commands.add_parser("pull")
    pull_command.add_argument("role")
    pull_command.add_argument("--no-wait", action="store_true")
    pull_command.add_argument(
        "--timeout", type=float, default=DEFAULT_PULL_TIMEOUT,
        help="return a wait record after this many seconds; range 0..60 (default: 60)",
    )
    submit_command = commands.add_parser("submit")
    submit_command.add_argument("role")
    submit_command.add_argument("artifacts", nargs="+")
    commands.add_parser("status")
    start_command = commands.add_parser(
        "start-problem", help="copy one prepared Benchmark problem into the active channel"
    )
    start_command.add_argument("problem")
    end_command = commands.add_parser(
        "end-problem", help="archive and clear the active Benchmark problem channel"
    )
    end_command.add_argument("outcome", choices=("ok", "error", "cancel"))
    return value


def main(argv: list[str] | None = None, *, prog: str = "labflow agent") -> int:
    args = parser(prog).parse_args(argv)
    try:
        root = args.root.resolve() if args.root else find_root(Path.cwd())
        if args.command == "pull":
            workflow = load_workflow(root)
            result = pull(root, workflow, _id(args.role, "role"), not args.no_wait, args.timeout)
        elif args.command == "submit":
            workflow = load_workflow(root)
            result = submit(root, workflow, _id(args.role, "role"),
                            [_id(name, "artifact") for name in args.artifacts])
        elif args.command == "status":
            workflow = load_workflow(root)
            result = workflow_status(root, workflow)
        elif args.command == "start-problem":
            from .benchmark_mode import start_problem
            result = start_problem(root, args.problem)
        else:
            from .benchmark_mode import end_problem
            result = end_problem(root, args.outcome)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (TaskError, ControlError) as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return exc.code
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
