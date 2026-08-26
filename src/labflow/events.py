from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import ControlError
from .context import Context
from .observe import text_parts
from .task_cli import task_records
from .timeline_store import read as read_timeline


def _ms_from_ns(value: Any) -> int | None:
    return value // 1_000_000 if isinstance(value, int) else None


def _brief(value: str, limit: int = 240) -> str:
    text = " ".join(value.split())
    return text if len(text) <= limit else text[:limit - 3] + "..."


def _since(event: dict[str, Any], since: int) -> bool:
    updated = event.get("at")
    return isinstance(updated, int) and updated >= since


def _thinking_events(
    session: str, message_id: str, role: str, created: int, completed: int | None,
    tool_intervals: list[tuple[int, int | None]],
) -> list[dict[str, Any]]:
    spans: list[tuple[int, int | None]] = []
    cursor: int | None = created
    for started, ended in sorted(tool_intervals, key=lambda value: value[0]):
        if cursor is None:
            break
        if started > cursor:
            spans.append((cursor, started))
        cursor = max(cursor, ended) if ended is not None else None
    if cursor is not None:
        if completed is None:
            spans.append((cursor, None))
        elif completed > cursor:
            spans.append((cursor, completed))
    result = []
    for index, (start, end) in enumerate(spans):
        if end is not None and end - start < 1000:
            continue
        event = {
            "id": f"thinking:{session}:{message_id}:{index}",
            "type": "thinking",
            "created_at": start,
            "at": end or start,
            "role": role,
            "status": "completed" if end is not None else "active",
        }
        if end is not None:
            event["end_at"] = end
        result.append(event)
    return result


def _message_events(session: str, role: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for message in messages:
        info = message.get("info", {})
        if info.get("role") != "assistant":
            continue
        message_id = info.get("id")
        created = info.get("time", {}).get("created")
        completed = info.get("time", {}).get("completed")
        if not isinstance(message_id, str) or not isinstance(created, (int, float)):
            continue
        created_ms = int(created)
        completed_ms = int(completed) if isinstance(completed, (int, float)) else None
        texts = text_parts(message)
        tokens = info.get("tokens", {})
        fresh_tokens = sum(tokens.get(name, 0) or 0
                           for name in ("input", "output", "reasoning"))
        if texts and completed_ms is not None:
            result.append({
                "id": f"reply:{session}:{message_id}",
                "type": "reply",
                "created_at": completed_ms,
                "at": completed_ms,
                "end_at": completed_ms,
                "role": role,
                "finish": info.get("finish"),
                "summary": _brief("\n".join(texts)),
                "tokens": fresh_tokens,
            })

        tool_intervals = []
        for index, part in enumerate(message.get("parts", [])):
            if part.get("type") != "tool":
                continue
            state = part.get("state", {})
            timing = state.get("time", {})
            started = timing.get("start")
            ended = timing.get("end")
            action_created = int(started) if isinstance(started, (int, float)) else created_ms
            action_end = int(ended) if isinstance(ended, (int, float)) else None
            tool_intervals.append((action_created, action_end))
            part_id = str(part.get("id", index))
            tool = str(part.get("tool", "tool"))
            event = {
                "id": f"action:{session}:{message_id}:{part_id}",
                "type": "action",
                "created_at": action_created,
                "at": action_end or action_created,
                "role": role,
                "action": tool,
                "status": state.get("status"),
            }
            if action_end is not None:
                event["end_at"] = action_end
            if tool == "bash":
                command = state.get("input", {}).get("command")
                if isinstance(command, str):
                    event["summary"] = _brief(command)
            if state.get("status") in ("completed", "error"):
                exit_code = state.get("metadata", {}).get("exit")
                if exit_code is not None:
                    event["exit"] = exit_code
            result.append(event)
        result.extend(_thinking_events(
            session, message_id, role, created_ms, completed_ms, tool_intervals,
        ))
    return result


def _task_events(workspace: Path) -> list[dict[str, Any]]:
    result = []
    records = task_records(workspace)
    for record in (*records["history"], *records["active"]):
        task_id = record.get("task_id")
        started = _ms_from_ns(record.get("started_at_ns"))
        if not isinstance(task_id, str) or started is None:
            continue
        ended = _ms_from_ns(record.get("submitted_at_ns") or record.get("ended_at_ns"))
        event = {
            "id": f"task:{task_id}",
            "type": "task",
            "created_at": started,
            "at": ended or started,
            "role": record.get("role"),
            "status": record.get("status"),
            "artifacts": record.get("artifacts", []),
        }
        if ended is not None:
            event["end_at"] = ended
        result.append(event)
    return result


def _json_events(directory: Path, kind: str) -> list[tuple[Path, dict[str, Any]]]:
    result = []
    if not directory.is_dir():
        return result
    for path in sorted(directory.glob("*.json")):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raise ControlError(f"invalid {kind} event: {path}") from None
        result.append((path, value))
    return result


def project_events(context: Context, since_ms: int) -> list[dict[str, Any]]:
    lab_root = context.state.get("lab_root")
    title = context.state.get("title")
    database = (Path(lab_root) / "timeline.sqlite3"
                if isinstance(lab_root, str) and isinstance(title, str) else None)
    has_timeline = database is not None and database.is_file()
    workspace = Path(context.state["workspace"])
    result = _task_events(workspace)
    for path, value in _json_events(context.root / "artifact-events", "artifact"):
        created = _ms_from_ns(value.get("mtime_ns"))
        if created is not None:
            result.append({
                "id": f"artifact:{value.get('number', path.stem.split('-', 1)[0])}",
                "type": "artifact",
                "created_at": created,
                "at": created,
                "artifact": value.get("artifact"),
                "action": value.get("reason"),
            })
    for _path, value in _json_events(context.root / "host-interventions", "Host intervention"):
        created = _ms_from_ns(value.get("recorded_at_ns"))
        if created is not None:
            result.append({
                "id": f"host-action:{value['recorded_at_ns']}",
                "type": "host_action",
                "created_at": created,
                "at": created,
                "action": value.get("kind"),
                "targets": len(value.get("targets", [])),
            })
    if has_timeline:
        result.extend(read_timeline(database, title, since_ms))
    else:
        result.extend(_backend_events(context))
    result = [event for event in result if _since(event, since_ms)]
    order = {"thinking": 0, "action": 1, "reply": 2, "task": 3,
             "artifact": 4, "host_action": 5}
    result.sort(key=lambda event: (
        event["at"], event.get("created_at", event["at"]),
        order.get(event["type"], 99), event["id"]
    ))
    return result


def _backend_events(context: Context) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    try:
        client = context.client()
        if context.state.get("execution", {}).get("kind") == "benchmark-mode":
            execution = context.state["execution"]
            seen = set()
            for item in context.state.get("benchmark", {}).get("sessions", []):
                session = item.get("id")
                role = item.get("agent")
                if isinstance(session, str) and isinstance(role, str) and session not in seen:
                    result.extend(_message_events(
                        session, role, client.session_messages(session)
                    ))
                    seen.add(session)
            for record in context.state.get("benchmark", {}).get("problems", []):
                for key, role in (("questioner_session_id", execution["questioner"]),
                                  ("answerer_session_id", execution["answerer"])):
                    session = record.get(key)
                    if isinstance(session, str) and session not in seen:
                        result.extend(_message_events(
                            session, role, client.session_messages(session)
                        ))
                        seen.add(session)
        else:
            sessions = [(child.get("id"),
                         str(child.get("agent") or child.get("title") or child.get("id")))
                        for child in client.children()]
            for session, role in sessions:
                if not isinstance(session, str):
                    continue
                result.extend(_message_events(session, role, client.session_messages(session)))
    except ControlError:
        # Local task/artifact history remains queryable after the external runtime exits.
        pass
    return result


def _optional_host_inputs(workflow: dict[str, Any]) -> set[str]:
    uses: dict[str, list[bool]] = {}
    for artifact in workflow["artifacts"].values():
        for reference in artifact.get("input", []):
            uses.setdefault(reference["id"], []).append(reference["optional"])
    return {name for name, flags in uses.items() if flags and all(flags)}


def _ready_host_artifacts(workflow: dict[str, Any], artifacts: dict[str, Any]) -> list[str]:
    return [name for name in workflow["artifacts"]
            if artifacts["artifacts"][name]["owner"] == "host"
            and not artifacts["artifacts"][name]["current"]
            and not artifacts["artifacts"][name]["blocked_by"]]


def pending_requests(workflow: dict[str, Any], artifacts: dict[str, Any]) -> list[str]:
    optional = _optional_host_inputs(workflow)
    return [name for name in _ready_host_artifacts(workflow, artifacts) if name not in optional]


def pending_optional_requests(workflow: dict[str, Any], artifacts: dict[str, Any]) -> list[str]:
    optional = _optional_host_inputs(workflow)
    return [name for name in _ready_host_artifacts(workflow, artifacts) if name in optional]


def _find_message(context: Context, session: str, message_id: str) -> dict[str, Any]:
    for message in context.client().session_messages(session):
        if message.get("info", {}).get("id") == message_id:
            return message
    raise ControlError(f"unknown event message: {message_id}", 66)


def event_detail(context: Context, event_id: str) -> dict[str, Any]:
    if (not event_id or len(event_id) > 1024
            or any(ord(character) < 32 or ord(character) == 127 for character in event_id)):
        raise ControlError(f"invalid event id: {event_id}", 64)
    lab_root = context.state.get("lab_root")
    title = context.state.get("title")
    if isinstance(lab_root, str) and isinstance(title, str):
        values = read_timeline(Path(lab_root) / "timeline.sqlite3", title, 0, event_id)
        if values:
            return {"id": event_id, "type": values[0]["type"], "detail": values[0]}
    parts = event_id.split(":")
    kind = parts[0]
    workspace = Path(context.state["workspace"])
    if kind == "task" and len(parts) == 2:
        for record in (*task_records(workspace)["history"], *task_records(workspace)["active"]):
            if record.get("task_id") == parts[1]:
                return {"id": event_id, "type": kind, "detail": record}
    elif kind == "artifact" and len(parts) == 2:
        for path, value in _json_events(context.root / "artifact-events", "artifact"):
            if str(value.get("number", path.stem.split('-', 1)[0])) == parts[1]:
                return {"id": event_id, "type": kind, "detail": value}
    elif kind == "host-action" and len(parts) == 2:
        for _path, value in _json_events(context.root / "host-interventions", "Host intervention"):
            if str(value.get("recorded_at_ns")) == parts[1]:
                return {"id": event_id, "type": "host_action", "detail": value}
    elif kind == "reply" and len(parts) == 3:
        message = _find_message(context, parts[1], parts[2])
        info = message.get("info", {})
        role = str(info.get("agent") or info.get("mode") or "unknown")
        event = next((value for value in _message_events(parts[1], role, [message])
                      if value["id"] == event_id), None)
        if event is None:
            raise ControlError(f"unknown event id: {event_id}", 66)
        detail: dict[str, Any] = {"event": event, "info": info, "text": text_parts(message)}
        return {"id": event_id, "type": kind, "detail": detail}
    elif kind == "thinking" and len(parts) == 4 and parts[3].isdigit():
        message = _find_message(context, parts[1], parts[2])
        info = message.get("info", {})
        role = str(info.get("agent") or info.get("mode") or "unknown")
        event = next((value for value in _message_events(parts[1], role, [message])
                      if value["id"] == event_id), None)
        if event is None:
            raise ControlError(f"unknown event id: {event_id}", 66)
        return {"id": event_id, "type": kind, "detail": {
            "event": event, "info": info,
        }}
    elif kind == "action" and len(parts) == 4:
        message = _find_message(context, parts[1], parts[2])
        for index, part in enumerate(message.get("parts", [])):
            if str(part.get("id", index)) == parts[3] and part.get("type") == "tool":
                return {"id": event_id, "type": kind, "detail": part}
    raise ControlError(f"unknown event id: {event_id}", 66)
