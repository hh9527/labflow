from __future__ import annotations

import hashlib
import json
import stat
from pathlib import Path
from typing import Any

from .config import ControlError, Manifest, sha256
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
    if manifest.execution["kind"] != "benchmark-mode":
        if source_value is not None:
            raise ControlError("--bundle is only valid for a benchmark-mode plan", 64)
        return state
    declared = manifest.execution.get("bundle")
    if declared is None:
        if source_value is not None:
            raise ControlError("benchmark-mode plan does not accept --bundle", 64)
        return state
    if source_value is None:
        raise ControlError("benchmark-mode plan requires --bundle", 64)
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
    files = _regular_files(source, declared["paths"])
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
        "schema": "labflow.benchmark-bundle/v1",
        "source": str(source),
        "installed_at": now(),
        "digest": _digest(inventory),
        "files": inventory,
    }
    atomic_json(root / "bundle.json", record)
    with locked(root):
        current = load_state(root)
        current["bundle"] = {key: record[key] for key in ("digest", "files", "installed_at")}
        save_state(root, current)
        return current


def verify_bundle(state: dict[str, Any]) -> str | None:
    if state.get("execution", {}).get("bundle") is None:
        return None
    bundle = state.get("bundle")
    if not isinstance(bundle, dict) or not isinstance(bundle.get("files"), list):
        raise ControlError("benchmark-mode bundle is not installed", 75)
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
        raise ControlError("benchmark-mode bundle changed after installation", 65)
    return digest
