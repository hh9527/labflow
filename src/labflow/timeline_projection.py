from __future__ import annotations

from pathlib import Path
from typing import Any


def _brief(value: str, limit: int = 240) -> str:
    text = " ".join(value.split())
    return text if len(text) <= limit else text[:limit - 3] + "..."


def _integer(value: Any) -> int | None:
    return int(value) if isinstance(value, (int, float)) else None


def _tool_interval(part: dict[str, Any]) -> tuple[int, int] | None:
    timing = part.get("state", {}).get("time", {})
    start, end = _integer(timing.get("start")), _integer(timing.get("end"))
    return (start, end) if start is not None and end is not None and end >= start else None


def _paths(part: dict[str, Any]) -> list[str]:
    if part.get("tool", "").lower() not in {"write", "edit", "apply_patch"}:
        return []
    inputs = part.get("state", {}).get("input", {})
    candidates: list[Any] = [
        inputs.get("filePath"), inputs.get("file_path"), inputs.get("path"),
    ]
    if isinstance(inputs.get("paths"), list):
        candidates.extend(inputs["paths"])
    result = []
    for candidate in candidates:
        if not isinstance(candidate, str) or not candidate:
            continue
        value = Path(candidate).as_posix()
        if value not in result:
            result.append(value)
    return result


def _event_id(kind: str, execution: str, session: str,
              message_id: str, suffix: str) -> str:
    return f"{kind}:{execution}:{session}:{message_id}:{suffix}"


def closed_message_events(execution: str, session: str, role: str | None,
                          message: dict[str, Any]) -> list[dict[str, Any]]:
    """Project one completed assistant message without exposing reasoning text."""
    info = message.get("info", {})
    if info.get("role") != "assistant":
        return []
    message_id = info.get("id")
    timing = info.get("time", {})
    created, completed = _integer(timing.get("created")), _integer(timing.get("completed"))
    if not isinstance(message_id, str) or created is None or completed is None or completed < created:
        return []

    base = {"execution": execution, "session": session, "role": role}
    result: list[dict[str, Any]] = []
    intervals: list[tuple[int, int]] = []
    for index, part in enumerate(message.get("parts", [])):
        if part.get("type") != "tool":
            continue
        interval = _tool_interval(part)
        state = part.get("state", {})
        status = state.get("status")
        if interval is None or status not in ("completed", "error"):
            continue
        start, end = interval
        intervals.append((max(created, start), min(completed, end)))
        tool = str(part.get("tool", "tool"))
        metadata = state.get("metadata", {})
        exit_code = metadata.get("exit")
        success = status == "completed" and exit_code in (None, 0)
        record: dict[str, Any] = {
            **base,
            "id": _event_id("action", execution, session, message_id,
                            str(part.get("id", index))),
            "type": "action",
            "at": start,
            "duration": end - start,
            "action": "shell" if tool == "bash" else tool,
            "success": success,
        }
        if tool == "bash":
            command = state.get("input", {}).get("command")
            if isinstance(command, str):
                record["command"] = command
        if isinstance(exit_code, int):
            record["exit_code"] = exit_code
        paths = _paths(part)
        if paths:
            record["paths"] = paths
        if not success:
            output = state.get("output")
            if isinstance(output, str) and output.strip():
                record["summary"] = _brief(output)
        result.append(record)

    spans = []
    cursor = created
    for start, end in sorted(interval for interval in intervals if interval[1] >= interval[0]):
        if start > cursor:
            spans.append((cursor, start))
        cursor = max(cursor, end)
    if completed > cursor:
        spans.append((cursor, completed))

    tokens = info.get("tokens", {})
    reasoning = tokens.get("reasoning")
    for index, (start, end) in enumerate(spans):
        if end <= start:
            continue
        record = {
            **base,
            "id": _event_id("thinking", execution, session, message_id, str(index)),
            "type": "thinking",
            "at": start,
            "duration": end - start,
        }
        # Message-level reasoning usage is exactly attributable only when there is one span.
        if len(spans) == 1 and isinstance(reasoning, int):
            record["tokens"] = reasoning
        result.append(record)

    texts = [str(part.get("text", "")) for part in message.get("parts", [])
             if part.get("type") == "text" and str(part.get("text", "")).strip()]
    if texts:
        cache = tokens.get("cache", {})
        reply = {
            **base,
            "id": _event_id("reply", execution, session, message_id, "complete"),
            "type": "reply",
            "at": completed,
            "duration": 0,
            "summary": _brief("\n".join(texts)),
        }
        output = tokens.get("output")
        if isinstance(output, int):
            reply["tokens"] = output
        for source, target in (
            (tokens.get("input"), "input_tokens"),
            (tokens.get("output"), "output_tokens"),
            (tokens.get("reasoning"), "reasoning_tokens"),
            (cache.get("read"), "cache_read_tokens"),
            (cache.get("write"), "cache_write_tokens"),
        ):
            if isinstance(source, int):
                reply[target] = source
        result.append(reply)
    return result

