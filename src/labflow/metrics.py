from __future__ import annotations

import fnmatch
import re
import time
from pathlib import Path
from typing import Any, Callable


def _tokens(messages: list[dict[str, Any]]) -> dict[str, int]:
    result = {"input": 0, "output": 0, "reasoning": 0, "cache_read": 0, "cache_write": 0}
    for message in messages:
        values = message.get("info", {}).get("tokens", {})
        for name in ("input", "output", "reasoning"):
            result[name] += values.get(name, 0) or 0
        cache = values.get("cache", {})
        result["cache_read"] += cache.get("read", 0) or 0
        result["cache_write"] += cache.get("write", 0) or 0
    result["fresh"] = result["input"] + result["output"] + result["reasoning"]
    return result


def _message_interval(message: dict[str, Any], now_ms: int) -> tuple[int, int] | None:
    values = message.get("info", {}).get("time", {})
    start = values.get("created")
    end = values.get("completed")
    if not isinstance(start, (int, float)):
        return None
    return int(start), int(end) if isinstance(end, (int, float)) else now_ms


def _overlap(left: tuple[int, int], right: tuple[int, int]) -> int:
    return max(0, min(left[1], right[1]) - max(left[0], right[0]))


def _longest_thinking_ms(message: dict[str, Any], window: tuple[int, int], now_ms: int) -> int:
    interval = _message_interval(message, now_ms)
    if interval is None:
        return 0
    start = max(interval[0], window[0])
    end = min(interval[1], window[1])
    if end <= start:
        return 0
    tools = []
    for part in message.get("parts", []):
        if part.get("type") != "tool":
            continue
        values = part.get("state", {}).get("time", {})
        tool_start, tool_end = values.get("start"), values.get("end")
        if isinstance(tool_start, (int, float)) and isinstance(tool_end, (int, float)):
            tools.append((max(start, int(tool_start)), min(end, int(tool_end))))
    cursor = start
    longest = 0
    for tool_start, tool_end in sorted(tools):
        if tool_end <= cursor:
            continue
        longest = max(longest, max(0, tool_start - cursor))
        cursor = max(cursor, tool_end)
    return max(longest, max(0, end - cursor))


def _command_metrics(
    messages: list[dict[str, Any]], definitions: dict[str, list[str]]
) -> dict[str, dict[str, int]]:
    result = {name: {"count": 0, "elapsed_ms": 0} for name in definitions}
    for message in messages:
        for part in message.get("parts", []):
            if part.get("type") != "tool" or part.get("tool") != "bash":
                continue
            state = part.get("state", {})
            command = state.get("input", {}).get("command")
            if not isinstance(command, str):
                continue
            timing = state.get("time", {})
            start, end = timing.get("start"), timing.get("end")
            elapsed = (max(0, int(end) - int(start))
                       if isinstance(start, (int, float)) and isinstance(end, (int, float)) else 0)
            for name, patterns in definitions.items():
                if any(fnmatch.fnmatchcase(command.strip(), pattern) for pattern in patterns):
                    result[name]["count"] += 1
                    result[name]["elapsed_ms"] += elapsed
    return result


def summarize_thread_metrics(
    messages: list[dict[str, Any]], definitions: dict[str, list[str]],
    now_ms: int | None = None,
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    rounds: list[dict[str, Any]] = []
    current: list[dict[str, Any]] | None = None
    round_started: int | None = None
    for message in messages:
        info = message.get("info", {})
        if info.get("role") == "user":
            if current is not None:
                rounds.append(_thread_round(len(rounds) + 1, round_started, current,
                                            definitions, now_ms))
            current = []
            created = info.get("time", {}).get("created")
            round_started = int(created) if isinstance(created, (int, float)) else None
        elif info.get("role") == "assistant" and current is not None:
            current.append(message)
    if current is not None:
        rounds.append(_thread_round(len(rounds) + 1, round_started, current,
                                    definitions, now_ms))
    assistants = [message for message in messages
                  if message.get("info", {}).get("role") == "assistant"]
    timing = _time(assistants)
    commands = _command_metrics(assistants, definitions)
    start = timing["first_created"] or now_ms
    end = timing["last_completed"] or now_ms
    return {
        "rounds": rounds,
        "assistant_messages": len(assistants),
        "time": timing,
        "tokens": _tokens(assistants),
        "longest_thinking_ms": max(
            (_longest_thinking_ms(message, (start, end), now_ms) for message in assistants),
            default=0,
        ),
        "commands": commands,
        "command_count": sum(value["count"] for value in commands.values()),
        "command_elapsed_ms": sum(value["elapsed_ms"] for value in commands.values()),
    }


def _thread_round(number: int, started: int | None, messages: list[dict[str, Any]],
                  definitions: dict[str, list[str]], now_ms: int) -> dict[str, Any]:
    timing = _time(messages)
    start = started if started is not None else (timing["first_created"] or now_ms)
    end = timing["last_completed"] or now_ms
    commands = _command_metrics(messages, definitions)
    return {
        "round": number,
        "elapsed_ms": max(0, end - start),
        "assistant_messages": len(messages),
        "tokens": _tokens(messages),
        "longest_thinking_ms": max(
            (_longest_thinking_ms(message, (start, end), now_ms) for message in messages),
            default=0,
        ),
        "commands": commands,
        "command_count": sum(value["count"] for value in commands.values()),
        "command_elapsed_ms": sum(value["elapsed_ms"] for value in commands.values()),
    }


def collect_task_metrics(
    records: dict[str, list[dict[str, Any]]],
    messages_by_role: dict[str, list[dict[str, Any]]],
    command_definitions: dict[str, dict[str, list[str]]] | None = None,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    tasks = [*records.get("history", []), *records.get("active", [])]
    normalized = []
    for task in tasks:
        started_ns = task.get("started_at_ns")
        if not isinstance(started_ns, int):
            continue
        ended_ns = task.get("submitted_at_ns") or task.get("ended_at_ns")
        start = started_ns // 1_000_000
        end = ended_ns // 1_000_000 if isinstance(ended_ns, int) else now_ms
        normalized.append({"record": task, "window": (start, max(start, end)), "messages": []})

    # A message belongs to at most one task. This avoids double-counting when a
    # single assistant turn submits one task and immediately pulls the next.
    for role, messages in messages_by_role.items():
        role_tasks = [item for item in normalized if item["record"].get("role") == role]
        for message in messages:
            if message.get("info", {}).get("role") != "assistant":
                continue
            interval = _message_interval(message, now_ms)
            if interval is None:
                continue
            candidates = [(_overlap(interval, item["window"]), item) for item in role_tasks]
            overlap, selected = max(candidates, key=lambda item: (item[0], item[1]["window"][0]),
                                    default=(0, None))
            if overlap and selected is not None:
                selected["messages"].append(message)

    result = []
    for item in normalized:
        task = item["record"]
        messages = item["messages"]
        commands = _command_metrics(
            messages, (command_definitions or {}).get(str(task.get("role")), {})
        )
        result.append({
            "task_id": task.get("task_id"),
            "role": task.get("role"),
            "artifacts": task.get("artifacts", []),
            "status": task.get("status"),
            "started_at_ns": task.get("started_at_ns"),
            "ended_at_ns": task.get("submitted_at_ns") or task.get("ended_at_ns"),
            "elapsed_ms": item["window"][1] - item["window"][0],
            "tokens": _tokens(messages),
            "longest_thinking_ms": max(
                (_longest_thinking_ms(message, item["window"], now_ms) for message in messages),
                default=0,
            ),
            "commands": commands,
            "command_count": sum(value["count"] for value in commands.values()),
            "command_elapsed_ms": sum(value["elapsed_ms"] for value in commands.values()),
            "assistant_messages": len(messages),
        })
    result.sort(key=lambda item: (item["started_at_ns"] or 0, item["task_id"] or ""))
    return result


def _time(messages: list[dict[str, Any]]) -> dict[str, int | None]:
    starts = [m.get("info", {}).get("time", {}).get("created") for m in messages]
    ends = [m.get("info", {}).get("time", {}).get("completed") for m in messages]
    starts = [value for value in starts if isinstance(value, (int, float))]
    ends = [value for value in ends if isinstance(value, (int, float))]
    active = sum(
        max(0, info["completed"] - info["created"] - _task_wait_ms(message))
        for message in messages
        if isinstance((info := message.get("info", {}).get("time", {})).get("created"), (int, float))
        and isinstance(info.get("completed"), (int, float))
    )
    first = min(starts) if starts else None
    last = max(ends) if ends else None
    span = last - first if first is not None and last is not None else None
    return {"first_created": first, "last_completed": last, "active_ms": active, "span_ms": span}


def _task_wait_ms(message: dict[str, Any]) -> int:
    info = message.get("info", {}).get("time", {})
    created = info.get("created")
    completed = info.get("completed")
    if not isinstance(created, (int, float)) or not isinstance(completed, (int, float)):
        return 0
    cursor = created
    waiting = 0
    for part in message.get("parts", []):
        if part.get("type") != "tool":
            continue
        state = part.get("state", {})
        end = state.get("time", {}).get("end")
        if not isinstance(end, (int, float)):
            continue
        command = state.get("input", {}).get("command", "")
        if (part.get("tool") == "bash" and isinstance(command, str)
                and re.search(
                    r"(?:(?:^|/)labflow agent (?:pull|next)|(?:^|\s)labflow agent pull) "
                    r"[a-z0-9._-]+(?:\s|$)",
                    command,
                )):
            waiting += max(0, min(end, completed) - cursor)
        cursor = max(cursor, end)
    return min(int(waiting), int(completed - created))


def _matches(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def _write_paths(message: dict[str, Any]) -> list[str]:
    result = []
    for part in message.get("parts", []):
        if part.get("type") != "tool" or part.get("tool") not in ("write", "edit", "apply_patch"):
            continue
        values = part.get("state", {}).get("input", {})
        path = values.get("filePath") or values.get("file_path") or values.get("path")
        if isinstance(path, str): result.append(path)
    return result


def _relative_tool_path(path: str, workspace: Path) -> str | None:
    value = Path(path)
    if value.is_absolute():
        try: return value.relative_to(workspace).as_posix()
        except ValueError: return None
    return value.as_posix().removeprefix("./")


def _phase_messages(
    messages: list[dict[str, Any]], workspace: Path, definition: dict[str, Any] | None
) -> list[tuple[str, str, list[dict[str, Any]]]]:
    assistants = [message for message in messages if message.get("info", {}).get("role") == "assistant"]
    if definition is None:
        return [("unclassified", "unclassified", assistants)] if assistants else []

    learning_names = definition.get("learning_phases", [])
    work_phases = definition.get("work_phases") or [{
        "name": definition.get("work_phase", "work"),
        "files": definition.get("work_files", []),
    }]
    turn = -1
    assistant_turns: list[tuple[int, dict[str, Any]]] = []
    for message in messages:
        role = message.get("info", {}).get("role")
        if role == "user":
            turn += 1
        elif role == "assistant":
            assistant_turns.append((max(turn, 0), message))

    boundaries = []
    for order, phase in enumerate(work_phases):
        for index, (_turn, message) in enumerate(assistant_turns):
            paths = (_relative_tool_path(path, workspace) for path in _write_paths(message))
            if any(
                relative is not None and _matches(relative, phase["files"])
                for relative in paths
            ):
                boundaries.append((index, order, phase["name"]))
                break
    boundaries.sort()

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    order: list[tuple[str, str]] = []
    for index, (message_turn, message) in enumerate(assistant_turns):
        active_work = [boundary for boundary in boundaries if boundary[0] <= index]
        if active_work:
            key = (active_work[-1][2], "work")
        else:
            name = learning_names[message_turn] if message_turn < len(learning_names) else f"learning_{message_turn + 1}"
            key = (name, "learning")
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append(message)
    return [(name, kind, grouped[(name, kind)]) for name, kind in order]


def _file_metric(path: Path) -> dict[str, int]:
    data = path.read_bytes()
    return {"files": 1, "lines": len(data.splitlines()), "bytes": len(data)}


def _add_metric(left: dict[str, int], right: dict[str, int]) -> dict[str, int]:
    return {name: left.get(name, 0) + right.get(name, 0) for name in ("files", "lines", "bytes")}


def _artifact_metrics(
    workspace: Path, definition: dict[str, Any]
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    result: dict[str, Any] = {}
    warnings = []
    for kind, categories in definition.get("artifacts", {}).items():
        kind_files: set[Path] = set()
        category_values = {}
        for category, patterns in categories.items():
            paths: set[Path] = set()
            for pattern in patterns:
                matched = {path for path in workspace.glob(pattern)
                           if path.is_file() and not path.is_symlink()}
                if not matched:
                    warnings.append({
                        "kind": "artifact_pattern_no_match",
                        "category": f"{kind}.{category}",
                        "pattern": pattern,
                    })
                paths.update(matched)
            metric = {"files": 0, "lines": 0, "bytes": 0}
            for path in sorted(paths):
                metric = _add_metric(metric, _file_metric(path))
            category_values[category] = metric
            kind_files.update(paths)
        total = {"files": 0, "lines": 0, "bytes": 0}
        for path in sorted(kind_files):
            total = _add_metric(total, _file_metric(path))
        result[kind] = {"categories": category_values, "total": total}
    return result, warnings


def _sum_values(values: list[dict[str, int]], names: tuple[str, ...]) -> dict[str, int]:
    return {name: sum(value.get(name, 0) for value in values) for name in names}


def collect_metrics(
    title: str,
    execution_phase: str,
    workspace: Path,
    children: list[dict[str, Any]],
    load_messages: Callable[[str], list[dict[str, Any]]],
    config: dict[str, Any],
    task_records: dict[str, list[dict[str, Any]]] | None = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    configured = config.get("roles", {})
    roles = []
    messages_by_role: dict[str, list[dict[str, Any]]] = {}
    children_by_role: dict[str, list[dict[str, Any]]] = {}
    for child in children:
        agent = child.get("agent")
        session_id = child.get("id") or child.get("session_id")
        if not isinstance(agent, str) or not isinstance(session_id, str):
            continue
        children_by_role.setdefault(agent, []).append(child)
        messages_by_role.setdefault(agent, []).extend(load_messages(session_id))
    for agent, role_children in children_by_role.items():
        messages = messages_by_role[agent]
        messages.sort(key=lambda message: message.get("info", {}).get("time", {}).get("created", 0))
        definition = configured.get(agent)
        phases = []
        for name, kind, phase_messages in _phase_messages(messages, workspace, definition):
            phases.append({"name": name, "kind": kind, "messages": len(phase_messages),
                           "time": _time(phase_messages), "tokens": _tokens(phase_messages)})
        assistant = [m for m in messages if m.get("info", {}).get("role") == "assistant"]
        time = _time(assistant)
        elapsed = time["span_ms"]
        time["waiting_ms"] = max(0, elapsed - time["active_ms"]) if elapsed is not None else None
        latest_child = role_children[-1]
        model = latest_child.get("model", {})
        artifacts, metric_warnings = _artifact_metrics(workspace, definition or {})
        work_fresh = sum(phase["tokens"]["fresh"] for phase in phases if phase["kind"] == "work")
        code_lines = artifacts.get("code", {}).get("total", {}).get("lines", 0)
        roles.append({
            "agent": agent,
            "session_id": latest_child.get("id") or latest_child.get("session_id"),
            "session_ids": [child.get("id") or child.get("session_id") for child in role_children],
            "title": latest_child.get("title"),
            "model": {"provider": model.get("providerID"), "id": model.get("id"), "variant": model.get("variant")},
            "classification": {
                "configured": definition is not None,
                "work_boundary_observed": any(phase["kind"] == "work" for phase in phases) if definition else None,
            },
            "time": time,
            "tokens": _tokens(assistant),
            "phases": phases,
            "artifacts": artifacts,
            "warnings": metric_warnings,
            "productivity": {
                "code_lines_per_1k_work_fresh_tokens": round(code_lines * 1000 / work_fresh, 3) if work_fresh else None,
            },
        })
    roles.sort(key=lambda item: item["agent"])

    tasks = collect_task_metrics(
        task_records or {"active": [], "history": []}, messages_by_role,
        {role: definition.get("commands", {}) for role, definition in configured.items()}, now_ms,
    )
    for role in roles:
        role_tasks = [task for task in tasks if task["role"] == role["agent"]]
        role["tasks"] = role_tasks
        role["latest_task"] = role_tasks[-1] if role_tasks else None
        if (role_tasks and role["classification"]["configured"]
                and not role["classification"]["work_boundary_observed"]
                and any(not all(name.startswith("lang-learn.") for name in task["artifacts"])
                        for task in role_tasks)):
            role["warnings"].append({
                "kind": "work_boundary_not_observed",
                "message": "task records contain non-learning work but no configured work file was observed",
            })

    token_names = ("input", "output", "reasoning", "cache_read", "cache_write", "fresh")
    token_total = _sum_values([role["tokens"] for role in roles], token_names)
    phase_totals = {}
    for kind in ("learning", "work", "unclassified"):
        selected = [phase for role in roles for phase in role["phases"] if phase["kind"] == kind]
        phase_totals[kind] = {
            "active_ms": sum(phase["time"]["active_ms"] for phase in selected),
            "tokens": _sum_values([phase["tokens"] for phase in selected], token_names),
        }
    artifact_totals = {}
    for kind in ("code", "documents"):
        values = [role["artifacts"][kind]["total"] for role in roles if kind in role["artifacts"]]
        artifact_totals[kind] = _sum_values(values, ("files", "lines", "bytes"))
    starts = [role["time"]["first_created"] for role in roles if role["time"]["first_created"] is not None]
    ends = [role["time"]["last_completed"] for role in roles if role["time"]["last_completed"] is not None]
    first = min(starts) if starts else None
    last = max(ends) if ends else None
    return {
        "schema": "labflow.stats/v1",
        "title": title,
        "execution_phase": execution_phase,
        "roles": roles,
        "aggregate": {
            "time": {
                "first_created": first,
                "last_completed": last,
                "span_ms": last - first if first is not None and last is not None else None,
            },
            "active_ms": sum(role["time"]["active_ms"] for role in roles),
            "tokens": token_total,
            "phases": phase_totals,
            "artifacts": artifact_totals,
            "productivity": {
                "code_lines_per_1k_work_fresh_tokens": round(
                    artifact_totals["code"]["lines"] * 1000 / phase_totals["work"]["tokens"]["fresh"], 3
                ) if phase_totals["work"]["tokens"]["fresh"] else None,
            },
        },
        "tasks": tasks,
        "task_token_accounting": (
            "Each assistant message is assigned to the task window with the greatest time overlap; "
            "tokens are not split within a message."
        ),
    }
