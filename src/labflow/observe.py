from __future__ import annotations

from pathlib import Path
from typing import Any


def failed_tool(part: dict[str, Any]) -> bool:
    state = part.get("state", {})
    return part.get("type") == "tool" and (state.get("status") == "error" or state.get("metadata", {}).get("exit", 0) not in (None, 0))


def assistant_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [m for m in messages if m.get("info", {}).get("role") == "assistant"]


def latest_assistant(messages: list[dict[str, Any]]) -> dict[str, Any] | None:
    values = assistant_messages(messages)
    return values[-1] if values else None


def text_parts(message: dict[str, Any]) -> list[str]:
    return [str(p.get("text", "")) for p in message.get("parts", []) if p.get("type") == "text"]


def summarize(messages: list[dict[str, Any]]) -> dict[str, Any]:
    parts = [p for message in messages for p in message.get("parts", [])]
    assistants = assistant_messages(messages)
    created = [m.get("info", {}).get("time", {}).get("created") for m in messages]
    completed = [m.get("info", {}).get("time", {}).get("completed") for m in messages]
    created = [x for x in created if isinstance(x, (int, float))]
    completed = [x for x in completed if isinstance(x, (int, float))]
    first, last = (min(created) if created else None), (max(completed) if completed else None)
    return {
        "messages": len(messages),
        "user_messages": sum(m.get("info", {}).get("role") == "user" for m in messages),
        "assistant_steps": len(assistants),
        "completed_assistant_steps": sum(m.get("info", {}).get("time", {}).get("completed") is not None for m in assistants),
        "length_finishes": sum(m.get("info", {}).get("finish") == "length" for m in messages),
        "tool_calls": sum(p.get("type") == "tool" for p in parts),
        "tool_failures": sum(failed_tool(p) for p in parts),
        "bash_calls": sum(p.get("type") == "tool" and p.get("tool") == "bash" for p in parts),
        "bash_failures": sum(p.get("tool") == "bash" and failed_tool(p) for p in parts),
        "writes": sum(p.get("type") == "tool" and p.get("tool") in ("write", "edit") for p in parts),
        "reads": sum(p.get("type") == "tool" and p.get("tool") == "read" for p in parts),
        "grep_failures": sum(p.get("tool") == "grep" and failed_tool(p) for p in parts),
        "first_created": first, "last_completed": last,
        "duration_ms": last - first if first is not None and last is not None else None,
        "reasoning_tokens": sum(m.get("info", {}).get("tokens", {}).get("reasoning", 0) or 0 for m in assistants),
    }


def failures(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for message in messages:
        for part in message.get("parts", []):
            if not failed_tool(part): continue
            state = part.get("state", {}); metadata = state.get("metadata", {})
            item = {"tool": part.get("tool"), "status": state.get("status"), "exit": metadata.get("exit"),
                    "input": state.get("input", {}), "output": " | ".join(str(state.get("output", "")).splitlines()[:3])[:1200]}
            key = repr(item)
            if key not in grouped: grouped[key] = {"count": 0, **item}
            grouped[key]["count"] += 1
    return sorted(grouped.values(), key=lambda x: -x["count"])


def list_files(workspace: Path, roots: tuple[str, ...]) -> list[dict[str, Any]]:
    result = []
    seen: set[Path] = set()
    for relative in roots:
        root = workspace / relative
        candidates = root.rglob("*") if root.is_dir() else (root,)
        for path in sorted(candidates):
            if path in seen or not path.is_file() or path.is_symlink(): continue
            seen.add(path); stat = path.stat()
            result.append({"path": path.relative_to(workspace).as_posix(), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns})
    result.sort(key=lambda item: item["path"])
    return result


def normalized(state: dict[str, Any], messages: list[dict[str, Any]], status: dict[str, Any], rounds: list[dict[str, Any]], observe_roots: tuple[str, ...], validation: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    workspace = Path(state["workspace"])
    return {
        "meta": {"schema": "labflow.observation/v1", "session_name": state["session_name"], "plan_id": state["plan_id"]},
        "state": state, "status": status, "rounds": rounds, "messages": messages,
        "events": [], "summary": summarize(messages), "failures": failures(messages),
        "files": list_files(workspace, observe_roots) if workspace.exists() else [], "validation": validation or [],
    }


def recent(messages: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    output = []
    for message in assistant_messages(messages)[-count:]:
        parts = []
        for part in message.get("parts", []):
            if part.get("type") == "tool":
                state = part.get("state", {}); parts.append({"type": "tool", "tool": part.get("tool"), "status": state.get("status"), "input": state.get("input", {}), "exit": state.get("metadata", {}).get("exit"), "output": str(state.get("output", ""))[:1200]})
            elif part.get("type") in ("reasoning", "text"):
                parts.append({"type": part["type"], "text": str(part.get("text", ""))[-2400:]})
        output.append({"message_id": message.get("info", {}).get("id"), "completed": message.get("info", {}).get("time", {}).get("completed"), "finish": message.get("info", {}).get("finish"), "parts": parts})
    return output


def timeline(messages: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    result = []
    for message in messages[-count:]:
        info = message.get("info", {})
        result.append({"message_id": info.get("id"), "role": info.get("role"), "created": info.get("time", {}).get("created"),
                       "completed": info.get("time", {}).get("completed"), "finish": info.get("finish"), "error": info.get("error"),
                       "model": info.get("modelID"), "tokens": info.get("tokens"), "parts": [{"type": p.get("type"), **({"tool": p.get("tool"), "status": p.get("state", {}).get("status"), "exit": p.get("state", {}).get("metadata", {}).get("exit")} if p.get("type") == "tool" else {})} for p in message.get("parts", [])]})
    return result
