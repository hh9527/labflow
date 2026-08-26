from __future__ import annotations

import hashlib
import json
import stat
import subprocess
import time
from pathlib import Path
from typing import Any

from .config import ControlError, Manifest, sha256, validate_identifier
from .external import resolve_command
from .observe import latest_assistant
from .state import atomic_json, atomic_write, load_state, locked, now, save_state


def _regular_files(root: Path, relative_paths: list[str]) -> list[tuple[str, Path]]:
    files: list[tuple[str, Path]] = []
    for relative in relative_paths:
        source = root / relative
        if source.is_symlink() or not source.exists():
            raise ControlError(f"missing or unsafe bundle input: {relative}", 66)
        candidates = [source] if source.is_file() else sorted(source.rglob("*"))
        for child in candidates:
            if child.is_symlink():
                raise ControlError(f"unsafe symlink in bundle: {child}", 66)
            if child.is_dir():
                continue
            if not child.is_file():
                raise ControlError(f"unsupported bundle input: {child}", 66)
            files.append((child.relative_to(root).as_posix(), child))
    names = [name for name, _path in files]
    if len(names) != len(set(names)):
        raise ControlError("bundle paths overlap", 64)
    return sorted(files)


def _digest(items: list[dict[str, Any]]) -> str:
    encoded = json.dumps(items, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def install_bundle(root: Path, state: dict[str, Any], manifest: Manifest,
                   source_value: str | None) -> dict[str, Any]:
    if manifest.execution["kind"] != "thread-service":
        if source_value is not None:
            raise ControlError("--bundle is only valid for a thread-service plan", 64)
        return state
    if source_value is None:
        raise ControlError("thread-service start requires --bundle", 64)
    source = Path(source_value).expanduser().resolve()
    if state.get("bundle"):
        record_path = root / "bundle.json"
        try:
            record = json.loads(record_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ControlError(f"invalid bundle record: {exc}") from None
        if record.get("source") != str(source):
            raise ControlError("execution is already installed from another bundle", 64)
        verify_bundle(state)
        return state
    if not source.is_dir():
        raise ControlError(f"bundle is not a directory: {source_value}", 66)
    files = _regular_files(source, manifest.execution["bundle"]["paths"])
    workspace = Path(state["workspace"])
    inventory = []
    for relative, source_file in files:
        destination = workspace / relative
        if destination.exists():
            raise ControlError(f"bundle would replace plan-owned input: {relative}", 64)
        mode = stat.S_IMODE(source_file.stat().st_mode)
        atomic_write(destination, source_file.read_bytes(), mode)
        inventory.append({"path": relative, "bytes": destination.stat().st_size,
                          "sha256": sha256(destination)})
    record = {
        "schema": "labflow.thread-bundle/v1",
        "source": str(source),
        "installed_at": now(),
        "digest": _digest(inventory),
        "files": inventory,
    }
    atomic_json(root / "bundle.json", record)
    with locked(root):
        current = load_state(root)
        current["bundle"] = {key: record[key] for key in ("digest", "files", "installed_at")}
        current["thread_service"] = {"baseline": None, "active": None}
        save_state(root, current)
        return current


def verify_bundle(state: dict[str, Any]) -> str:
    bundle = state.get("bundle")
    if not isinstance(bundle, dict) or not isinstance(bundle.get("files"), list):
        raise ControlError("thread-service bundle is not installed", 75)
    workspace = Path(state["workspace"])
    current = []
    for item in bundle["files"]:
        path = workspace / item["path"]
        if not path.is_file() or path.is_symlink():
            raise ControlError(f"bundle file is missing: {item['path']}", 66)
        current.append({"path": item["path"], "bytes": path.stat().st_size,
                        "sha256": sha256(path)})
    digest = _digest(current)
    if digest != bundle.get("digest"):
        raise ControlError("thread-service bundle changed after installation", 65)
    return digest


def _completed_assistant(client: Any, session_id: str) -> dict[str, Any]:
    status = client.statuses().get(session_id, {"type": "idle"})
    if status.get("type") != "idle":
        raise ControlError(f"session is {status.get('type')}; operation requires idle", 75)
    latest = latest_assistant(client.session_messages(session_id))
    info = latest.get("info", {}) if latest else {}
    if (not latest or info.get("finish") != "stop"
            or info.get("time", {}).get("completed") is None):
        raise ControlError("session has no completed assistant answer", 75)
    return latest


def approve_baseline(context: Any, role: str) -> dict[str, Any]:
    from .lifecycle import reconcile

    context.state, _messages = reconcile(context)
    execution = context.manifest.execution
    if execution["kind"] != "thread-service" or role != execution["role"]:
        raise ControlError(f"unknown thread-service role: {role}", 64)
    service = context.state.get("thread_service", {})
    if service.get("baseline"):
        raise ControlError("baseline is already approved", 64)
    session_id = context.state.get("session_id")
    if not isinstance(session_id, str):
        raise ControlError("thread-service has no baseline session", 75)
    latest = _completed_assistant(context.client(), session_id)
    workspace = Path(context.state["workspace"])
    missing = [pattern for pattern in execution["baseline"]["checks"]
               if not any(path.is_file() and not path.is_symlink()
                          for path in workspace.glob(pattern))]
    if missing:
        raise ControlError(f"baseline checks failed: {', '.join(missing)}", 65)
    command = execution["baseline"]["command"]
    result = subprocess.run(resolve_command(command, workspace), cwd=workspace, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise ControlError(f"baseline command failed ({result.returncode}): "
                           f"{result.stderr.strip() or result.stdout.strip()}", 65)
    record = {
        "schema": "labflow.thread-baseline/v1",
        "role": role,
        "session_id": session_id,
        "message_id": latest["info"]["id"],
        "bundle_digest": verify_bundle(context.state),
        "approved_at": now(),
        "command": command,
        "stdout": result.stdout[-4000:],
    }
    atomic_json(context.root / "baseline.json", record)
    with locked(context.root):
        state = load_state(context.root)
        state["thread_service"]["baseline"] = record
        state["phase"] = "idle"
        save_state(context.root, state)
        context.state = state
    return record


def _baseline(context: Any, role: str) -> dict[str, Any]:
    execution = context.manifest.execution
    if execution["kind"] != "thread-service" or role != execution["role"]:
        raise ControlError(f"unknown thread-service role: {role}", 64)
    baseline = context.state.get("thread_service", {}).get("baseline")
    if not isinstance(baseline, dict):
        raise ControlError("baseline has not been approved", 75)
    if verify_bundle(context.state) != baseline.get("bundle_digest"):
        raise ControlError("approved baseline bundle is stale", 65)
    client = context.client()
    latest = _completed_assistant(client, baseline["session_id"])
    if latest.get("info", {}).get("id") != baseline.get("message_id"):
        raise ControlError("baseline session changed after approval", 65)
    messages = client.session_messages(baseline["session_id"])
    if not messages or messages[-1].get("info", {}).get("id") != baseline.get("message_id"):
        raise ControlError("baseline session changed after approval", 65)
    return baseline


def _copy_input(context: Any, role: str, name: str, source_value: str, index: int,
                kind: str) -> dict[str, Any]:
    source = Path(source_value).expanduser().resolve()
    if not source.is_file() or source.is_symlink():
        raise ControlError(f"missing or unsafe {kind} file: {source_value}", 66)
    try:
        text = source.read_text(encoding="utf-8")
    except UnicodeError:
        raise ControlError(f"{kind} file must be UTF-8: {source_value}", 65) from None
    relative = Path("thread-inputs") / role / name / f"{index:03d}-{kind}.md"
    destination = Path(context.state["workspace"]) / relative
    if destination.exists():
        raise ControlError(f"thread input already exists: {relative}", 64)
    atomic_write(destination, source.read_bytes(), 0o444)
    return {"kind": kind, "path": relative.as_posix(), "sha256": sha256(destination),
            "text": text, "created_at": now()}


def _record_path(context: Any, role: str, name: str) -> Path:
    return context.root / "threads" / role / f"{name}.json"


def open_thread(context: Any, role: str, name: str, problem_file: str) -> dict[str, Any]:
    from .lifecycle import next_session_title

    validate_identifier(name, "thread-name")
    baseline = _baseline(context, role)
    service = context.state["thread_service"]
    if service.get("active"):
        raise ControlError(f"role {role} already has an active thread", 75)
    record_path = _record_path(context, role, name)
    if record_path.exists():
        raise ControlError(f"thread already exists: {name}", 64)
    problem = _copy_input(context, role, name, problem_file, 0, "problem")
    client = context.client()
    title = next_session_title(client, f"{context.state['session_base']}.{name}",
                               context.state["lab_root"])
    response = client.fork_session(baseline["session_id"])
    session_id = response.get("id") if isinstance(response, dict) else None
    if not isinstance(session_id, str) or not session_id.startswith("ses_"):
        raise ControlError("opencode returned an invalid fork session identity", 69)
    client.update_session(session_id, {"title": title})
    opened_ms = int(time.time() * 1000)
    record = {"schema": "labflow.thread/v1", "role": role, "name": name,
              "session_id": session_id, "status": "active", "opened_at": now(),
              "title": title,
              "opened_at_ms": opened_ms, "closed_at": None, "inputs": [problem]}
    atomic_json(record_path, record)
    with locked(context.root):
        state = load_state(context.root)
        state["thread_service"]["active"] = {
            "role": role, "name": name, "session_id": session_id,
        }
        save_state(context.root, state)
        context.state = state
    client.prompt_session(session_id, problem["text"], agent=role)
    return record


def _active_record(context: Any, role: str, name: str | None = None) -> tuple[Path, dict[str, Any]]:
    active = context.state.get("thread_service", {}).get("active")
    if not isinstance(active, dict) or active.get("role") != role:
        raise ControlError(f"role {role} has no active thread", 75)
    if name is not None and active.get("name") != name:
        raise ControlError(f"active thread is {active.get('name')}, not {name}", 64)
    path = _record_path(context, role, active["name"])
    try:
        return path, json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlError(f"invalid thread record: {exc}") from None


def comment_thread(context: Any, role: str, name: str, comment_file: str) -> dict[str, Any]:
    path, record = _active_record(context, role, name)
    _completed_assistant(context.client(), record["session_id"])
    item = _copy_input(context, role, name, comment_file, len(record["inputs"]), "comment")
    record["inputs"].append(item)
    atomic_json(path, record)
    context.client().prompt_session(record["session_id"], item["text"], agent=role)
    return record


def close_thread(context: Any, role: str) -> dict[str, Any]:
    path, record = _active_record(context, role)
    latest = _completed_assistant(context.client(), record["session_id"])
    archived_ms = int(time.time() * 1000)
    context.client().update_session(record["session_id"], {"time": {"archived": archived_ms}})
    record["status"] = "closed"
    record["closed_at"] = now()
    record["closed_at_ms"] = archived_ms
    record["final_message_id"] = latest["info"]["id"]
    atomic_json(path, record)
    with locked(context.root):
        state = load_state(context.root)
        state["thread_service"]["active"] = None
        save_state(context.root, state)
        context.state = state
    return record


def thread_records(context: Any) -> list[dict[str, Any]]:
    directory = context.root / "threads"
    records = []
    if directory.is_dir():
        for path in sorted(directory.glob("*/*.json")):
            try:
                records.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError) as exc:
                raise ControlError(f"invalid thread record: {path}: {exc}") from None
    return records
