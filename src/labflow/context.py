from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .client import Client
from .config import ControlError, Manifest, load_manifest, repository_root
from .state import execution_root, load_lab_config, load_state, validate_session_name


@dataclass
class Context:
    repo: Path
    root: Path
    state: dict[str, Any]
    manifest: Manifest

    def client(self) -> Client:
        return Client(self.state["server_url"], self.state["workspace"], self.state["session_id"])

    def rounds(self) -> list[dict[str, Any]]:
        result = []
        directory = self.root / "rounds"
        if directory.exists():
            for path in sorted(directory.glob("*.json")):
                try: result.append(json.loads(path.read_text(encoding="utf-8")))
                except (OSError, json.JSONDecodeError): raise ControlError(f"invalid round record: {path}") from None
        return result


def resolve(lab_name: str, session_name: str, cwd: Path | None = None) -> Context:
    validate_session_name(session_name)
    repo = repository_root(cwd)
    lab = load_lab_config(repo, lab_name)
    root = execution_root(Path(lab["root"]), session_name)
    state = load_state(root)
    if state.get("session_name") != session_name: raise ControlError("session name mismatch")
    if state.get("lab_name") != lab_name or state.get("lab_root") != lab["root"]:
        raise ControlError("execution lab identity mismatch")
    return Context(repo, root, state, load_manifest(repo, state["plan_id"]))
