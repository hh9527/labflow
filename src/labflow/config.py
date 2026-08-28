from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ControlError(Exception):
    def __init__(self, message: str, code: int = 65):
        super().__init__(message)
        self.code = code


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class Manifest:
    plan_id: str
    root: Path
    roles: dict[str, dict[str, Any]]
    workflow: dict[str, Any]
    execution: dict[str, Any]
