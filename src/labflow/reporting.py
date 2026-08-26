from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from .config import ControlError
from .context import Context
from .external import resolve_cli
from .state import atomic_json, atomic_write, locked, now


NOTICE = "> 机械过程观察，未经 Host 验收。\n\n"


def submit_report(context: Context, body_file: Path) -> dict[str, Any]:
    try:
        body = body_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise ControlError(f"cannot read report body: {exc}", 66) from None
    if not body.strip():
        raise ControlError("report body must not be empty", 64)
    if len(body.encode()) > 256 * 1024:
        raise ControlError("report body exceeds 256 KiB", 64)
    content = NOTICE + body.rstrip() + "\n"
    directory = context.root / "reports"
    with locked(context.root):
        directory.mkdir(exist_ok=True)
        numbers = [int(path.stem) for path in directory.glob("[0-9][0-9][0-9].md")]
        number = max(numbers, default=-1) + 1
        name = f"{number:03d}"
        stored = directory / f"{name}.md"
        atomic_write(stored, content.encode())
        record: dict[str, Any] = {
            "schema": "labflow.report/v1",
            "session_name": context.state["session_name"],
            "number": number,
            "created_at": now(),
            "body": stored.name,
            "sinks": [],
        }
        atomic_json(directory / f"{name}.json", record)

    failures = 0
    for sink in context.state.get("reporting", {"sinks": []}).get("sinks", []):
        result = subprocess.run(
            [
                *resolve_cli("gh"),
                "issue",
                "comment",
                str(sink["issue"]),
                "--repo",
                sink["repository"],
                "--body-file",
                str(stored),
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        outcome = {
            "kind": sink["kind"],
            "repository": sink["repository"],
            "issue": sink["issue"],
            "exit": result.returncode,
            "output": result.stdout.strip(),
            "error": result.stderr.strip(),
        }
        record["sinks"].append(outcome)
        failures += int(result.returncode != 0)
    record["status"] = "error" if failures else "ok"
    atomic_json(directory / f"{name}.json", record)
    return record
