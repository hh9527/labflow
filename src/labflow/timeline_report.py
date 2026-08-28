from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .config import ControlError
from .state import atomic_write
from .timeline_store import report_events


def _load_cursor(path: Path) -> int:
    try:
        text = path.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        return 0
    except OSError as exc:
        raise ControlError(f"cannot read report cursor: {exc}", 73) from None
    try:
        value = int(text)
    except ValueError:
        raise ControlError("invalid report cursor", 65) from None
    if value < 0:
        raise ControlError("invalid report cursor", 65)
    return value


def _duration(milliseconds: int) -> str:
    seconds = milliseconds / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, seconds = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def _timestamp(milliseconds: int) -> str:
    return datetime.fromtimestamp(milliseconds / 1000).astimezone().strftime(
        "[%y-%m-%d %H:%M:%S]"
    )


def format_events(events: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    opened: list[dict[str, Any]] = []
    resolved: list[dict[str, Any]] = []
    for event in events:
        kind, task = event["type"], event["task"]
        timestamp = _timestamp(int(event["at"]))
        if kind == "task_started":
            lines.append(f"{timestamp} {task} 已开始")
        elif kind == "task_completed":
            total = sum(int(event[key]) for key in (
                "input_tokens", "output_tokens", "reasoning_tokens",
            ))
            label = "已完成" if event.get("status") == "submitted" else "已结束"
            lines.append(
                f"{timestamp} {task} {label}（耗时 {_duration(event['duration_ms'])}，"
                f"Token {total:,}，最长思考 {_duration(event['longest_thinking_ms'])}）"
            )
        elif kind == "host_request_opened":
            opened.append(event)
        elif kind == "host_request_resolved":
            resolved.append(event)
    if opened:
        names = "、".join(dict.fromkeys(event["task"] for event in opened))
        lines.append(f"{_timestamp(max(int(event['at']) for event in opened))} "
                     f"{names} 等待 Host 处理")
    if resolved:
        names = "、".join(dict.fromkeys(event["task"] for event in resolved))
        lines.append(f"{_timestamp(max(int(event['at']) for event in resolved))} "
                     f"{names} 已由 Host 处理")
    return "\n".join(lines)


class TimelineReporter:
    def __init__(self, home: Path, execution: str, *, debounce: float = 5.0,
                 max_debounce: float = 15.0,
                 clock: Callable[[], float] = time.monotonic):
        self.path = home / "events.sqlite"
        self.cursor_path = home / "report-cursor"
        self.execution = execution
        self.debounce = debounce
        self.max_debounce = max_debounce
        self.clock = clock
        self.cursor = _load_cursor(self.cursor_path)
        self.pending: list[dict[str, Any]] = []
        self.first_seen: float | None = None
        self.last_seen: float | None = None

    def poll(self) -> str | None:
        self.cursor, values = report_events(self.path, self.execution, self.cursor)
        now = self.clock()
        if values:
            self.pending.extend(values)
            self.first_seen = now if self.first_seen is None else self.first_seen
            self.last_seen = now
        if not self.pending or self.first_seen is None or self.last_seen is None:
            return None
        if (now - self.last_seen < self.debounce
                and now - self.first_seen < self.max_debounce):
            return None
        return format_events(self.pending)

    def commit(self) -> None:
        try:
            atomic_write(self.cursor_path, f"{self.cursor}\n".encode("ascii"))
        except OSError as exc:
            raise ControlError(f"cannot write report cursor: {exc}", 73) from None
        self.pending.clear()
        self.first_seen = None
        self.last_seen = None
