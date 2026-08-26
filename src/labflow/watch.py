from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .context import Context
from .state import atomic_json, load_state, locked, now


FILE_TOOLS = {"read", "write", "edit", "delete", "glob", "grep", "list"}


def _fingerprint(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _role(message: dict[str, Any], fallback: str) -> str:
    info = message.get("info", {})
    return str(info.get("agent") or info.get("mode") or fallback)


def _tool_event(session: str, role: str, message_id: str, index: int, part: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    if part.get("type") != "tool":
        return None
    state = part.get("state", {})
    status = state.get("status")
    if status not in ("pending", "running", "completed", "error"):
        return None
    tool = str(part.get("tool", "tool"))
    finished = status in ("completed", "error")
    if tool == "task":
        kind = "task_result" if finished else "task_start"
    elif tool == "bash":
        kind = "command_result" if finished else "command_start"
    elif tool.lower() in FILE_TOOLS:
        kind = "file_result" if finished else "file_start"
    else:
        kind = "tool_result" if finished else "tool_start"
    identity = {
        "session": session,
        "message": message_id,
        "part": part.get("id", index),
        "status": status,
    }
    event: dict[str, Any] = {
        "time": now(),
        "role": role,
        "kind": kind,
        "tool": tool,
        "status": status,
    }
    inputs = state.get("input", {})
    if tool == "bash":
        event["command"] = inputs.get("command", "")
    elif inputs:
        event["input"] = inputs
    if finished:
        event["exit"] = state.get("metadata", {}).get("exit")
    return _fingerprint(identity), event


def message_events(session: str, role: str, messages: Iterable[dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
    output = []
    for message in messages:
        message_id = str(message.get("info", {}).get("id", ""))
        actual_role = _role(message, role)
        for index, part in enumerate(message.get("parts", [])):
            event = _tool_event(session, actual_role, message_id, index, part)
            if event:
                output.append(event)
    return output


def acp_events(raw: dict[str, Any], roles: dict[str, str]) -> list[tuple[str, dict[str, Any]]]:
    event_type = raw.get("type")
    properties = raw.get("properties", {})
    session = str(properties.get("sessionID", ""))
    role = roles.get(session, "coordinator")
    if event_type == "message.part.updated":
        message_id = str(properties.get("messageID", ""))
        part = properties.get("part", {})
        event = _tool_event(session, role, message_id, 0, part)
        return [event] if event else []
    if event_type == "session.status":
        status = properties.get("status", properties.get("type"))
        identity = {"session": session, "status": status}
        return [(_fingerprint(identity), {"time": now(), "role": role, "kind": "role_status", "status": status})]
    if event_type in ("permission.asked", "permission.replied", "session.error"):
        identity = {"type": event_type, "session": session, "properties": properties}
        return [(_fingerprint(identity), {
            "time": now(), "role": role, "kind": "infrastructure_permission_error"
            if event_type.startswith("permission.") else "session_error", "event": event_type,
        })]
    return []


@dataclass
class WatchWindow:
    started: float
    debounce: float
    timeout: float
    events: list[dict[str, Any]] = field(default_factory=list)
    fingerprints: list[str] = field(default_factory=list)
    last_progress: float | None = None

    def add(self, fingerprint: str, event: dict[str, Any], observed: float) -> None:
        if fingerprint in self.fingerprints:
            return
        self.fingerprints.append(fingerprint)
        self.events.append(event)
        self.last_progress = observed

    def reason(self, current: float, finished: bool = False) -> str | None:
        if finished:
            return "experiment_finished"
        if self.last_progress is not None and current - self.last_progress >= self.debounce:
            return "debounced"
        if self.last_progress is None and current - self.started >= self.timeout:
            return "timeout"
        return None

    def remaining(self, current: float) -> float:
        deadline = (self.last_progress + self.debounce) if self.last_progress is not None else (self.started + self.timeout)
        return max(0.0, deadline - current)


def _cursor(context: Context) -> tuple[int, set[str]]:
    path = context.root / "observer" / "cursor.json"
    if not path.exists():
        return 0, set()
    data = json.loads(path.read_text(encoding="utf-8"))
    return int(data.get("sequence", 0)), set(data.get("seen", []))


def _snapshot(context: Context) -> tuple[dict[str, str], list[tuple[str, dict[str, Any]]]]:
    client = context.client()
    roles = {context.state["session_id"]: "coordinator"}
    output = message_events(context.state["session_id"], "coordinator", client.messages())
    for child in client.children():
        session = child.get("id")
        if not isinstance(session, str):
            continue
        role = str(child.get("agent") or child.get("title") or session)
        roles[session] = role
        output.extend(message_events(session, role, client.session_messages(session)))
    return roles, output


def watch_progress(context: Context, debounce: int, timeout: int) -> dict[str, Any]:
    sequence, seen = _cursor(context)
    started_at = now()
    window = WatchWindow(time.monotonic(), debounce, timeout)
    roles, snapshot = _snapshot(context)
    observed = time.monotonic()
    for fingerprint, event in snapshot:
        if fingerprint not in seen:
            window.add(fingerprint, event, observed)

    reason = None
    while reason is None:
        state = load_state(context.root)
        reason = window.reason(time.monotonic(), state["phase"] in ("finished", "failed", "retired"))
        if reason:
            break
        wait = min(1.0, window.remaining(time.monotonic()))
        for raw in context.client().events(timeout=max(0.05, wait)):
            for fingerprint, event in acp_events(raw, roles):
                if fingerprint not in seen:
                    window.add(fingerprint, event, time.monotonic())
            if window.reason(time.monotonic()):
                break

    next_sequence = sequence + 1
    seen.update(window.fingerprints)
    with locked(context.root):
        current_sequence, _ = _cursor(context)
        if current_sequence != sequence:
            from .config import ControlError
            raise ControlError("another observer advanced the watch cursor", 75)
        atomic_json(context.root / "observer" / "cursor.json", {
            "schema": "labflow.watch-cursor/v1",
            "sequence": next_sequence,
            "seen": sorted(seen),
            "updated_at": now(),
        })
    return {
        "session_name": context.state["session_name"],
        "started_at": started_at,
        "ended_at": now(),
        "reason": reason,
        "events": window.events,
        "next_cursor": str(next_sequence),
    }
