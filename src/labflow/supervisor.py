from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import sys
import time
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from .client import Client, OpenCodeNotFound
from .config import ControlError
from .events import pending_optional_requests, pending_requests
from .runtime_opencode import dag_hash, resume_prompt
from .state import atomic_json
from .task_cli import (
    TaskError, assign_task, clear_session_qualifications, submit, task_records,
    supersede_role_task, workflow_status,
)
from .timeline_projection import closed_message_events
from .timeline_report import TimelineReporter
from .timeline_store import TimelineWriter, task_statistics


@dataclass(frozen=True)
class LifecycleEvent:
    kind: str
    execution: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Effect:
    key: str
    kind: str
    execution: str
    role: str
    title: str
    backend_id: str | None = None
    artifact: str | None = None
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class EffectState:
    status: str
    execution: str
    error: str | None = None


@dataclass
class SessionState:
    role: str
    title: str
    backend_id: str | None = None
    status: str = "missing"
    idle_epoch: int = 0
    missing_epoch: int = 0
    observed: bool = False
    error: str | None = None
    completed_turn_id: str | None = None
    completed_turn_at: int = 0
    completed_turn_reply: str = ""
    error_kind: str | None = None


@dataclass
class ActiveTaskState:
    task_id: str
    artifact: str
    started_at_ns: int


@dataclass
class FailureState:
    task_id: str
    kind: str
    count: int
    first_at: int
    last_at: int


@dataclass
class ExecutionState:
    title: str
    workspace: str
    root_session_id: str | None
    active: bool
    dag: bool
    roles: tuple[str, ...]
    workflow: dict[str, Any] | None = None
    dag_revision: str | None = None
    artifacts: dict[str, int] = field(default_factory=dict)
    runnable: dict[str, tuple[tuple[str, int], ...]] = field(default_factory=dict)
    requests: tuple[str, ...] = ()
    optional_requests: tuple[str, ...] = ()
    request_versions: dict[str, int] = field(default_factory=dict)
    sessions: dict[str, SessionState] = field(default_factory=dict)
    observed_sessions: dict[str, SessionState] = field(default_factory=dict)
    sessions_initialized: bool = False
    active_tasks: dict[str, ActiveTaskState] = field(default_factory=dict)
    observation_error: str | None = None
    failures: dict[str, FailureState] = field(default_factory=dict)
    blocked_reason: str | None = None


@dataclass
class SupervisorState:
    executions: dict[str, ExecutionState] = field(default_factory=dict)
    effects: dict[str, EffectState] = field(default_factory=dict)
    seen_messages: set[tuple[str, str, str]] = field(default_factory=set)
    active_mtime: int | None = None
    applied_active_mtime: int | None = None
    plan_error: str | None = None
    active: bool = False


def _session_dump(value: SessionState) -> dict[str, Any]:
    return {
        "role": value.role, "title": value.title, "backend_id": value.backend_id,
        "status": value.status, "idle_epoch": value.idle_epoch,
        "missing_epoch": value.missing_epoch, "observed": value.observed,
        "error": value.error, "completed_turn_id": value.completed_turn_id,
        "completed_turn_at": value.completed_turn_at,
        "completed_turn_reply": value.completed_turn_reply,
        "error_kind": value.error_kind,
    }


def _session_load(value: dict[str, Any]) -> SessionState:
    return SessionState(
        str(value["role"]), str(value["title"]), value.get("backend_id"),
        str(value.get("status", "missing")), int(value.get("idle_epoch", 0)),
        int(value.get("missing_epoch", 0)), bool(value.get("observed", False)),
        value.get("error"), value.get("completed_turn_id"),
        int(value.get("completed_turn_at", 0)),
        str(value.get("completed_turn_reply", "")),
        value.get("error_kind"),
    )


def _state_dump(state: SupervisorState) -> dict[str, Any]:
    return {
        "executions": {
            title: {
                "title": value.title, "workspace": value.workspace,
                "root_session_id": value.root_session_id, "active": value.active,
                "dag": value.dag, "roles": list(value.roles), "workflow": value.workflow,
                "dag_revision": value.dag_revision, "artifacts": value.artifacts,
                "runnable": {role: [list(item) for item in items]
                             for role, items in value.runnable.items()},
                "requests": list(value.requests),
                "optional_requests": list(value.optional_requests),
                "request_versions": value.request_versions,
                "sessions": {role: _session_dump(session)
                             for role, session in value.sessions.items()},
                "observed_sessions": {identity: _session_dump(session)
                                      for identity, session in value.observed_sessions.items()},
                "sessions_initialized": value.sessions_initialized,
                "active_tasks": {
                    role: {
                        "task_id": task.task_id, "artifact": task.artifact,
                        "started_at_ns": task.started_at_ns,
                    }
                    for role, task in value.active_tasks.items()
                },
                "observation_error": value.observation_error,
                "failures": {
                    role: {
                        "task_id": failure.task_id, "kind": failure.kind,
                        "count": failure.count, "first_at": failure.first_at,
                        "last_at": failure.last_at,
                    }
                    for role, failure in value.failures.items()
                },
                "blocked_reason": value.blocked_reason,
            }
            for title, value in state.executions.items()
        },
        "effects": {
            key: {"status": value.status, "execution": value.execution,
                  "error": value.error}
            for key, value in state.effects.items()
        },
        "seen_messages": [list(value) for value in sorted(state.seen_messages)],
        "active_mtime": state.active_mtime,
        "applied_active_mtime": state.applied_active_mtime,
        "plan_error": state.plan_error,
        "active": state.active,
    }


def _state_load(value: Any) -> SupervisorState:
    if not isinstance(value, dict):
        return SupervisorState()
    state = SupervisorState()
    try:
        for title, item in value.get("executions", {}).items():
            state.executions[str(title)] = ExecutionState(
                str(item["title"]), str(item["workspace"]), item.get("root_session_id"),
                bool(item["active"]), bool(item["dag"]), tuple(item.get("roles", ())),
                workflow=item.get("workflow"), dag_revision=item.get("dag_revision"),
                artifacts={str(name): int(stamp)
                           for name, stamp in item.get("artifacts", {}).items()},
                runnable={str(role): tuple((str(task[0]), int(task[1])) for task in tasks)
                          for role, tasks in item.get("runnable", {}).items()},
                requests=tuple(item.get("requests", ())),
                optional_requests=tuple(item.get("optional_requests", ())),
                request_versions={str(name): int(stamp)
                                  for name, stamp in item.get("request_versions", {}).items()},
                sessions={str(role): _session_load(session)
                          for role, session in item.get("sessions", {}).items()},
                observed_sessions={str(identity): _session_load(session)
                                   for identity, session in item.get("observed_sessions", {}).items()},
                sessions_initialized=bool(item.get("sessions_initialized", False)),
                active_tasks={
                    str(role): ActiveTaskState(
                        str(task["task_id"]), str(task["artifact"]),
                        int(task["started_at_ns"]),
                    )
                    for role, task in item.get("active_tasks", {}).items()
                },
                observation_error=item.get("observation_error"),
                failures={
                    str(role): FailureState(
                        str(failure["task_id"]), str(failure["kind"]),
                        int(failure["count"]), int(failure["first_at"]),
                        int(failure["last_at"]),
                    )
                    for role, failure in item.get("failures", {}).items()
                },
                blocked_reason=item.get("blocked_reason"),
            )
        state.effects = {
            str(key): EffectState(str(item["status"]), str(item["execution"]), item.get("error"))
            for key, item in value.get("effects", {}).items()
        }
        state.seen_messages = {
            (str(item[0]), str(item[1]), str(item[2]))
            for item in value.get("seen_messages", ()) if len(item) == 3
        }
        state.active_mtime = value.get("active_mtime")
        state.applied_active_mtime = value.get("applied_active_mtime")
        state.plan_error = value.get("plan_error")
        state.active = bool(value.get("active", False))
    except (KeyError, TypeError, ValueError):
        return SupervisorState()
    return state


def _effect_key(*values: Any) -> str:
    data = json.dumps(values, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(data.encode()).hexdigest()


def _workflow_revision(workflow: Any) -> str | None:
    if not isinstance(workflow, dict):
        return None
    encoded = json.dumps(
        workflow, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _milliseconds(value: Any) -> int | None:
    return value // 1_000_000 if isinstance(value, int) else None


def _message_task(value: dict[str, Any], session_id: str, role: str,
                  message: dict[str, Any]) -> tuple[str, str] | None:
    info = message.get("info", {})
    timing = info.get("time", {})
    created = int(timing.get("created", 0))
    completed = int(timing.get("completed", created))
    records = task_records(Path(value["workspace"]))
    candidates = []
    for task in (*records["history"], *records["active"]):
        targets = task.get("artifacts")
        started = _milliseconds(task.get("started_at_ns"))
        ended = _milliseconds(task.get("submitted_at_ns") or task.get("ended_at_ns"))
        if (task.get("role") == role and isinstance(targets, list) and len(targets) == 1
                and isinstance(targets[0], str) and started is not None
                and started <= completed and (ended is None or ended >= created)):
            candidates.append((started, targets[0]))
    if not candidates:
        return None
    return "artifact", max(candidates)[1]


def _message_reply(message: dict[str, Any]) -> str:
    return "\n".join(
        str(part.get("text", "")) for part in message.get("parts", ())
        if part.get("type") == "text" and str(part.get("text", "")).strip()
    )


@contextmanager
def supervisor_lock(root: Path) -> Iterator[None]:
    """Ensure one process owns reconciliation and Timeline writes for a Lab."""
    path = root / "lock"
    with path.open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ControlError("another Supervisor already owns this laboratory", 75) from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def control_mtime(path: Path) -> int | None:
    try:
        value = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(value.st_mode):
        raise ControlError(f"control marker is not a regular file: {path}")
    return value.st_mtime_ns


def _requested(state: SupervisorState, effect: Effect) -> bool:
    if effect.key in state.effects:
        return False
    state.effects[effect.key] = EffectState("requested", effect.execution)
    return True


def _reconcile_role(state: SupervisorState, execution: ExecutionState,
                    role: str) -> list[Effect]:
    if (not execution.active or not execution.dag or not execution.root_session_id
            or not execution.sessions_initialized or execution.blocked_reason is not None):
        return []
    session = execution.sessions.setdefault(role, SessionState(role, role))
    if session.status == "missing":
        effect = Effect(
            _effect_key("create", execution.title, role, session.missing_epoch),
            "create_session", execution.title, role, role,
        )
        return [effect] if _requested(state, effect) else []
    task = execution.active_tasks.get(role)
    if task is not None:
        if (session.status == "idle" and session.completed_turn_id is not None
                and session.completed_turn_at >= task.started_at_ns // 1_000_000
                and session.completed_turn_reply.lstrip().startswith("已完成任务。")):
            effect = Effect(
                _effect_key(
                    "settle", execution.title, task.task_id,
                    session.completed_turn_id,
                ),
                "settle_task", execution.title, role, session.title,
                session.backend_id, task.artifact,
                {"task_id": task.task_id, "turn_id": session.completed_turn_id},
            )
            return [effect] if _requested(state, effect) else []
        if session.status == "idle" and session.error and session.backend_id:
            failure = execution.failures.get(role)
            effect = Effect(
                _effect_key(
                    "repair", execution.title, task.task_id, session.error,
                    failure.count if failure is not None else 0,
                ),
                "prompt_session", execution.title, role, session.title,
                session.backend_id, task.artifact, {
                    "error": session.error,
                    "include_checks": session.error_kind == "validation_failed",
                },
            )
            return [effect] if _requested(state, effect) else []
        return []
    runnable = execution.runnable.get(role, ())
    if session.status != "idle" or not runnable or not session.backend_id:
        return []
    effect = Effect(
        _effect_key("prompt", execution.title, session.title, runnable,
                    session.idle_epoch),
        "prompt_session", execution.title, role, session.title, session.backend_id,
        runnable[0][0], {
            "error": session.error,
            "include_checks": session.error_kind == "validation_failed",
        },
    )
    return [effect] if _requested(state, effect) else []


def _reconcile_execution(state: SupervisorState,
                         execution: ExecutionState) -> list[Effect]:
    effects = []
    for role in execution.roles:
        effects.extend(_reconcile_role(state, execution, role))
    return effects


def _block_execution(state: SupervisorState, execution: ExecutionState,
                     role: str, task: ActiveTaskState, reason: str,
                     at: int) -> list[Effect]:
    execution.blocked_reason = reason
    effect = Effect(
        _effect_key("system-blocked", execution.title, task.task_id, reason),
        "write_system_blocked", execution.title, role, role,
        artifact=task.artifact,
        data={
            "execution": execution.title, "role": role,
            "task_id": task.task_id, "artifact": task.artifact,
            "reason": reason, "at": at,
        },
    )
    return [effect] if _requested(state, effect) else []


def _record_task_failure(state: SupervisorState, execution: ExecutionState,
                         role: str, kind: str, reason: str, at: int) -> list[Effect]:
    task = execution.active_tasks.get(role)
    if task is None:
        return []
    previous = execution.failures.get(role)
    if (previous is None or previous.task_id != task.task_id
            or previous.kind != kind or at - previous.last_at > 120_000):
        failure = FailureState(task.task_id, kind, 1, at, at)
    else:
        failure = FailureState(
            task.task_id, kind, previous.count + 1, previous.first_at, at,
        )
    execution.failures[role] = failure
    session = execution.sessions.setdefault(role, SessionState(role, role))
    session.error = reason
    session.error_kind = kind
    session.completed_turn_id = None
    session.completed_turn_at = 0
    session.completed_turn_reply = ""
    if failure.count >= 3:
        return _block_execution(state, execution, role, task, reason, at)
    return _reconcile_role(state, execution, role)


def reduce(state: SupervisorState, event: LifecycleEvent) -> list[Effect]:
    """Mutate one event-loop-owned state object and return external work."""
    if event.kind == "outputs_requested":
        return [
            Effect(_effect_key("persist", event.data.get("generation")),
                   "persist_state", "", "", "", data={"state": _state_dump(state)}),
            Effect(_effect_key("outputs", event.data.get("generation")),
                   "write_outputs", "", "", ""),
        ]
    if event.kind == "active_control_observed":
        observed = event.data.get("mtime_ns")
        if observed == state.active_mtime:
            return []
        state.active_mtime = observed
        state.active = False
        state.plan_error = None
        for known in state.executions.values():
            known.active = False
        if observed is None:
            return [Effect(
                _effect_key("store-active-control", observed),
                "store_active_control", "", "", "",
                data={"observed_mtime_ns": state.active_mtime,
                      "applied_mtime_ns": state.applied_active_mtime,
                      "error": state.plan_error},
            )]
        return [Effect(
            _effect_key("reload-plan", observed), "reload_plan",
            event.execution, "", "", data={"mtime_ns": observed},
        )]
    if event.kind == "execution_deleted":
        state.executions.pop(event.execution, None)
        state.effects = {
            key: record for key, record in state.effects.items()
            if record.execution != event.execution
        }
        return []
    if event.kind == "execution_updated":
        data = event.data
        previous = state.executions.get(event.execution)
        execution = ExecutionState(
            event.execution,
            str(data["workspace"]),
            data.get("root_session_id"),
            bool(data.get("active")),
            bool(data.get("dag")),
            tuple(data.get("roles", ())),
            workflow=data.get("workflow"),
            dag_revision=data.get("dag_revision"),
            artifacts=previous.artifacts if previous else {},
            sessions=previous.sessions if previous else {},
            runnable=(previous.runnable if previous and data.get("dag") else {}),
            requests=(previous.requests if previous and data.get("dag") else ()),
            optional_requests=(previous.optional_requests
                               if previous and data.get("dag") else ()),
            request_versions=(previous.request_versions
                              if previous and data.get("dag") else {}),
            observed_sessions=previous.observed_sessions if previous else {},
            sessions_initialized=previous.sessions_initialized if previous else False,
            active_tasks=previous.active_tasks if previous else {},
            observation_error=previous.observation_error if previous else None,
            failures=previous.failures if previous else {},
            blocked_reason=previous.blocked_reason if previous else None,
        )
        state.executions[event.execution] = execution
        return []
    execution = state.executions.get(event.execution)
    if execution is None:
        return []
    if event.kind == "session_observation_requested":
        return [Effect(
            _effect_key("observe", event.execution, event.data.get("generation")),
            "observe_sessions", event.execution, "", "",
        )]
    if event.kind == "filesystem_observation_requested":
        return [Effect(
            _effect_key("observe-filesystem", event.execution, event.data.get("generation")),
            "observe_filesystem", event.execution, "", "",
        )]
    if event.kind == "workflow_observation_requested":
        return [Effect(
            _effect_key("observe-workflow", event.execution, event.data.get("generation")),
            "observe_workflow", event.execution, "", "",
        )]
    if event.kind == "root_session_requested":
        return [Effect(
            _effect_key("root-session", event.execution, event.data.get("generation")),
            "ensure_root_session", event.execution, "lab-ob", execution.title,
        )]
    if event.kind == "root_session_observed":
        execution.root_session_id = str(event.data["backend_id"])
        return [
            Effect(
                _effect_key("store-root-session", event.execution, event.data["backend_id"]),
                "store_root_session", event.execution, "lab-ob", execution.title,
                data={"backend_id": event.data["backend_id"]},
            ),
            Effect(
                _effect_key("observe", event.execution, event.data["backend_id"]),
                "observe_sessions", event.execution, "", "",
            ),
        ]
    if event.kind == "root_session_failed":
        execution.observation_error = str(event.data["error"])
        return []
    if event.kind == "dag_loaded":
        if event.data.get("mtime_ns") != state.active_mtime:
            return []
        return [Effect(
            _effect_key("activate-plan", event.data["mtime_ns"], event.data["revision"]),
            "activate_plan", event.execution, "", "",
            data=dict(event.data),
        )]
    if event.kind == "dag_activated":
        if event.data.get("mtime_ns") != state.active_mtime:
            return []
        state.applied_active_mtime = int(event.data["mtime_ns"])
        state.plan_error = None
        state.active = True
        execution.active = True
        execution.workflow = event.data["workflow"]
        execution.roles = tuple(event.data["workflow"].get("roles", ()))
        execution.dag_revision = str(event.data["revision"])
        effects = [
            Effect(
                _effect_key("store-active-control", state.active_mtime, "applied"),
                "store_active_control", "", "", "",
                data={"observed_mtime_ns": state.active_mtime,
                      "applied_mtime_ns": state.applied_active_mtime,
                      "error": state.plan_error},
            ),
            Effect(
                _effect_key("observe-workflow", execution.title, state.active_mtime),
                "observe_workflow", execution.title, "", "",
            ),
        ]
        if execution.root_session_id is None:
            effects.append(Effect(
                _effect_key("root-session", execution.title, state.active_mtime),
                "ensure_root_session", execution.title, "lab-ob", execution.title,
            ))
        return effects
    if event.kind == "dag_reload_failed":
        state.active = False
        state.plan_error = str(event.data["error"])
        for known in state.executions.values():
            known.active = False
        return [Effect(
            _effect_key("store-active-control", state.active_mtime, state.plan_error),
            "store_active_control", "", "", "",
            data={"observed_mtime_ns": state.active_mtime,
                  "applied_mtime_ns": state.applied_active_mtime,
                  "error": state.plan_error},
        )]
    if event.kind == "session_observation_failed":
        execution.observation_error = str(event.data["error"])
        return []
    if event.kind == "workflow_observed":
        execution.dag_revision = event.data.get("dag_revision")
        execution.runnable = {
            role: tuple((str(item[0]), int(item[1])) for item in items)
            for role, items in event.data.get("runnable", {}).items()
        }
        execution.requests = tuple(event.data.get("requests", ()))
        execution.optional_requests = tuple(event.data.get("optional_requests", ()))
        execution.request_versions = {
            str(name): int(version)
            for name, version in event.data.get("request_versions", {}).items()
        }
        return _reconcile_execution(state, execution)
    if event.kind == "tasks_observed":
        execution.active_tasks = {
            str(item["role"]): ActiveTaskState(
                str(item["task_id"]), str(item["artifact"]),
                int(item["started_at_ns"]),
            )
            for item in event.data.get("tasks", ())
        }
        return _reconcile_execution(state, execution)
    if event.kind == "artifact_updated":
        execution.artifacts[str(event.data["artifact"])] = int(event.data["mtime_ns"])
        return []
    if event.kind == "artifact_deleted":
        execution.artifacts.pop(str(event.data["artifact"]), None)
        return []
    if event.kind == "artifact_rejected":
        name = str(event.data["artifact"])
        return [Effect(
            _effect_key(
                "restore-artifact", execution.title, name,
                event.data.get("observed_mtime_ns"), event.data.get("previous_mtime_ns"),
            ),
            "restore_artifact", execution.title, "", "", artifact=name,
            data={"previous_mtime_ns": event.data.get("previous_mtime_ns")},
        )]
    if event.kind == "artifact_restored":
        name = str(event.data["artifact"])
        previous = event.data.get("previous_mtime_ns")
        if isinstance(previous, int):
            execution.artifacts[name] = previous
        else:
            execution.artifacts.pop(name, None)
        return []
    if event.kind == "system_blocked_observed":
        present = bool(event.data.get("present"))
        if present:
            if execution.blocked_reason is None:
                execution.blocked_reason = "system-blocked marker exists"
            return []
        if execution.blocked_reason is not None:
            execution.blocked_reason = None
            execution.failures.clear()
            return _reconcile_execution(state, execution)
        return []
    if event.kind == "system_blocked_written":
        return []
    if event.kind == "observed_sessions_updated":
        execution.sessions_initialized = True
        execution.observation_error = None
        execution.observed_sessions = {
            str(item["backend_id"]): SessionState(
                str(item["role"]), str(item["title"]),
                backend_id=str(item["backend_id"]),
                status=str(item["status"]),
            )
            for item in event.data.get("sessions", ())
        }
        return []
    if event.kind == "session_observed":
        role = str(event.data["role"])
        status = str(event.data["status"])
        session = execution.sessions.setdefault(role, SessionState(role, role))
        if status == "idle" and session.status != "idle":
            session.idle_epoch += 1
        session.status = status
        session.title = str(event.data["title"])
        session.backend_id = str(event.data["backend_id"])
        session.observed = True
        if "error" in event.data:
            session.error = event.data.get("error")
        return _reconcile_role(state, execution, role)
    if event.kind == "turn_completed":
        role = str(event.data["role"])
        title = str(event.data["title"])
        message_id = str(event.data["message_id"])
        identity = (execution.title, title, message_id)
        unseen = identity not in state.seen_messages
        state.seen_messages.add(identity)
        completed_at = int(event.data["completed_at"])
        task = execution.active_tasks.get(role)
        if task is None or completed_at < task.started_at_ns // 1_000_000:
            return []
        reply = str(event.data.get("reply", ""))
        if event.data.get("finish") == "stop":
            if reply.lstrip().startswith("无法完成任务。"):
                if not unseen:
                    return []
                return _block_execution(
                    state, execution, role, task,
                    "Agent reported that the task cannot be completed", completed_at,
                )
            if reply.lstrip().startswith("已完成任务。"):
                session = execution.sessions.setdefault(role, SessionState(role, title))
                if completed_at >= session.completed_turn_at:
                    session.completed_turn_id = message_id
                    session.completed_turn_at = completed_at
                    session.completed_turn_reply = reply
                return _reconcile_role(state, execution, role)
            if unseen:
                return _record_task_failure(
                    state, execution, role, "reply_unclassified",
                    "reply must start with 已完成任务。 or 无法完成任务。", completed_at,
                )
            return []
        if unseen:
            return _record_task_failure(
                state, execution, role, "turn_aborted",
                f"Agent turn ended without stop: {event.data.get('finish')}", completed_at,
            )
        return []
    if event.kind == "task_validation_failed":
        role = str(event.data["role"])
        task = execution.active_tasks.get(role)
        if task is None or task.task_id != event.data.get("task_id"):
            return []
        return _record_task_failure(
            state, execution, role, "validation_failed", str(event.data["error"]),
            int(event.data["at"]),
        )
    if event.kind == "task_superseded":
        role = str(event.data["role"])
        task = execution.active_tasks.get(role)
        if task is not None and task.task_id == event.data.get("task_id"):
            execution.active_tasks.pop(role, None)
        return []
    if event.kind == "session_missing":
        role = str(event.data["role"])
        session = execution.sessions.setdefault(role, SessionState(role, role))
        # A successful create may precede visibility in the backend's child index.
        if (session.backend_id is not None and not session.observed
                and not event.data.get("confirmed")):
            return []
        if session.status not in ("missing", "failed"):
            session.missing_epoch += 1
        session.status = "missing"
        session.backend_id = None
        session.observed = False
        return _reconcile_role(state, execution, role)
    if event.kind in ("effect_succeeded", "effect_failed"):
        key = str(event.data["key"])
        record = state.effects.get(key)
        if record is None:
            return []
        if event.kind == "effect_failed":
            record.status = "failed"
            record.error = str(event.data.get("error", "effect failed"))
            role = str(event.data["role"])
            session = execution.sessions.setdefault(role, SessionState(role, role))
            session.status = "failed"
            session.error = record.error
            return []
        record.status = "succeeded"
        role = str(event.data["role"])
        session = execution.sessions.setdefault(role, SessionState(role, role))
        if event.data.get("effect_kind") == "create_session":
            session.backend_id = str(event.data["backend_id"])
            session.title = str(event.data["title"])
            session.status = "idle"
            session.observed = False
            session.idle_epoch += 1
            return _reconcile_role(state, execution, role)
        if event.data.get("effect_kind") == "prompt_session":
            session.status = "prompted" if event.data.get("delivered", True) else "idle"
            session.error = None
            session.error_kind = None
            session.completed_turn_id = None
            session.completed_turn_at = 0
            session.completed_turn_reply = ""
            task = event.data.get("task")
            if isinstance(task, dict):
                execution.active_tasks[role] = ActiveTaskState(
                    str(task["task_id"]), str(task["artifact"]),
                    int(task["started_at_ns"]),
                )
        if event.data.get("effect_kind") == "settle_task":
            task = execution.active_tasks.get(role)
            if task is not None and task.task_id == event.data.get("task_id"):
                execution.active_tasks.pop(role, None)
            execution.failures.pop(role, None)
            return [Effect(
                _effect_key(
                    "observe-workflow", execution.title,
                    event.data.get("task_id"), "settled",
                ),
                "observe_workflow", execution.title, "", "",
            )]
        return []
    return []


def _session_tree(client: Client, root_id: str, root_title: str,
                  root_role: str) -> list[dict[str, str]]:
    output = [{"id": root_id, "title": root_title, "role": root_role}]
    pending = [root_id]
    seen = {root_id}
    while pending:
        parent = pending.pop()
        try:
            children = client.children(parent)
        except OpenCodeNotFound:
            if parent == root_id:
                raise
            output = [session for session in output if session["id"] != parent]
            continue
        for child in children:
            session_id = child.get("id")
            if not isinstance(session_id, str) or session_id in seen:
                continue
            seen.add(session_id)
            pending.append(session_id)
            output.append({
                "id": session_id,
                "title": str(child.get("title") or child.get("agent") or session_id),
                "role": str(child.get("agent") or child.get("title") or "unknown"),
            })
    return output


def _observed_sessions(client: Client, root_id: str, root_title: str,
                       root_role: str) -> list[dict[str, str]]:
    return _session_tree(client, root_id, root_title, root_role)


class Supervisor:
    @property
    def active_mtime(self) -> int | None:
        return self.state.active_mtime

    @active_mtime.setter
    def active_mtime(self, value: int | None) -> None:
        self.state.active_mtime = value

    @property
    def applied_active_mtime(self) -> int | None:
        return self.state.applied_active_mtime

    @applied_active_mtime.setter
    def applied_active_mtime(self, value: int | None) -> None:
        self.state.applied_active_mtime = value

    @property
    def plan_error(self) -> str | None:
        return self.state.plan_error

    @plan_error.setter
    def plan_error(self, value: str | None) -> None:
        self.state.plan_error = value

    @property
    def active(self) -> bool:
        return self.state.active

    @active.setter
    def active(self, value: bool) -> None:
        self.state.active = value

    def __init__(self, exec_home: Path, port: int, *, poll_interval: float = .25):
        self.root = exec_home.resolve(strict=True)
        if self.root.name != ".labflow-exec":
            raise ControlError(f"invalid execution home: {self.root}", 64)
        self.server_url = f"http://127.0.0.1:{port}"
        self.poll_interval = poll_interval
        from .project import get_state, load_execution
        _, self.manifest, self.config = load_execution(self.root.parent)
        self.bootstrap_root_session_id = get_state(self.root, "root_session_id")
        if self.config["port"] != port:
            raise ControlError(
                f"execution is configured for port {self.config['port']}, not {port}", 64
            )
        control = get_state(self.root, "active_control", {})
        if not isinstance(control, dict):
            raise ControlError("invalid active control state")
        observed = control.get("observed_mtime_ns")
        applied = control.get("applied_mtime_ns")
        error = control.get("error")
        if ((observed is not None and not isinstance(observed, int))
                or (applied is not None and not isinstance(applied, int))
                or (error is not None and not isinstance(error, str))):
            raise ControlError("invalid active control state")
        self.state = _state_load(get_state(self.root, "supervisor_state"))
        self.active_mtime = observed
        self.applied_active_mtime = applied
        self.plan_error = error
        current_active = control_mtime(self.root / "artifacts" / "_active")
        self.active = bool(
            current_active is not None and current_active == observed == applied
            and error is None
        )
        self.event_error: str | None = None
        self.timeline_error: str | None = None
        self.timeline_reporter = TimelineReporter(self.root, self.manifest.plan_id)
        try:
            self.writer: TimelineWriter | None = TimelineWriter(self.root / "events.sqlite")
        except (OSError, RuntimeError, sqlite3.Error) as exc:
            self.writer = None
            self.event_error = str(exc)

    def close(self) -> None:
        if self.writer is not None:
            try:
                self.writer.close()
            except (OSError, RuntimeError, sqlite3.Error) as exc:
                self.event_error = str(exc)
            self.writer = None

    def _events(self, records: list[dict[str, Any]]) -> None:
        if not records or self.writer is None:
            return
        try:
            self.writer.submit(records)
        except (OSError, RuntimeError, sqlite3.Error) as exc:
            self.event_error = str(exc)
            self.writer = None

    def _desired(self) -> dict[str, Path]:
        return {self.manifest.plan_id: self.root}

    def _sync_active(self) -> None:
        if self.manifest.plan_id not in self.state.executions:
            event, _ = self._execution_event(self.manifest.plan_id, self.root)
            self._process([event])
        self._process([LifecycleEvent(
            "filesystem_observation_requested", self.manifest.plan_id,
            {"generation": time.time_ns()},
        )])

    def _execution_event(self, title: str, desired: Path) -> tuple[LifecycleEvent, dict[str, Any]]:
        manifest, config = self.manifest, self.config
        known = self.state.executions.get(title)
        current_workflow = known.workflow if known is not None else manifest.workflow
        current_revision = (known.dag_revision if known is not None
                            else dag_hash(manifest))
        value = {
            "title": title,
            "workspace": str(manifest.root),
            "session_id": (known.root_session_id if known is not None
                           else self.bootstrap_root_session_id),
            "workflow": current_workflow,
            "execution": manifest.execution,
            "phase": "active" if self.active else "idle",
            "server_url": self.server_url,
            "lab_root": config["lab_root"],
        }
        workflow = value.get("workflow")
        roles = tuple(workflow.get("roles", ())) if isinstance(workflow, dict) else ()
        active = self.active and value.get("phase") in {"ready", "active", "idle"}
        event = LifecycleEvent("execution_updated", title, {
            "workspace": value["workspace"],
            "root_session_id": value.get("session_id"),
            "active": active,
            "dag": (desired / "artifacts").is_dir() and isinstance(workflow, dict),
            "roles": roles,
            "workflow": workflow,
            "dag_revision": current_revision,
        })
        return event, value

    def _workflow_event(self, title: str, value: dict[str, Any]) -> LifecycleEvent | None:
        workflow = value.get("workflow")
        if not isinstance(workflow, dict):
            return None
        artifacts = workflow_status(Path(value["workspace"]), workflow)
        runnable: dict[str, list[tuple[str, int]]] = {}
        for name, status in artifacts["artifacts"].items():
            if status["runnable"]:
                runnable.setdefault(status["owner"], []).append(
                    (name, int(status["input_mtime_ns"])),
                )
        required = pending_requests(workflow, artifacts)
        optional = pending_optional_requests(workflow, artifacts)
        execution = self.state.executions.get(title)
        revision = (execution.dag_revision if execution is not None
                    else _workflow_revision(workflow))
        return LifecycleEvent("workflow_observed", title, {
            "runnable": runnable,
            "requests": required,
            "optional_requests": optional,
            "request_versions": {
                name: int(artifacts["artifacts"][name]["input_mtime_ns"])
                for name in (*required, *optional)
            },
            "dag_revision": revision,
        })

    def _dag_revision_record(self, title: str, workflow: LifecycleEvent) -> dict[str, Any]:
        revision = str(workflow.data["dag_revision"])
        return {
            "id": f"dag-revised:{title}:{revision}",
            "execution": title,
            "type": "dag_revised",
            "at": int(time.time() * 1000),
            "duration": 0,
            "dag_revision": revision,
        }

    def _task_records(self, title: str, value: dict[str, Any]) -> list[dict[str, Any]]:
        revision = _workflow_revision(value.get("workflow"))
        workspace = Path(value["workspace"])
        records = task_records(workspace)
        result: list[dict[str, Any]] = []
        for task in (*records["history"], *records["active"]):
            attempt = task.get("task_id")
            targets = task.get("artifacts")
            started = _milliseconds(task.get("started_at_ns"))
            if (not isinstance(attempt, str) or not isinstance(targets, list)
                    or len(targets) != 1 or not isinstance(targets[0], str)
                    or started is None):
                continue
            role = task.get("role")
            session = None
            execution = self.state.executions.get(title)
            if execution is not None and isinstance(role, str):
                known = execution.sessions.get(role)
                session = known.title if known is not None else role
            base = {
                "execution": title, "session": session, "role": role,
                "task_kind": "artifact", "task_id": targets[0],
                "dag_revision": revision, "duration": 0,
                "payload": {"attempt_id": attempt},
            }
            result.append({
                **base, "id": f"task-started:{title}:{attempt}",
                "type": "task_started", "at": started,
            })
            ended = _milliseconds(task.get("submitted_at_ns") or task.get("ended_at_ns"))
            if ended is not None:
                result.append({
                    **base, "id": f"task-completed:{title}:{attempt}",
                    "type": "task_completed", "at": ended,
                    "payload": {"attempt_id": attempt, "status": task.get("status")},
                })
        return result

    def _request_records(self, title: str, workflow: LifecycleEvent,
                         previous: tuple[set[str], set[str], dict[str, int]]) -> list[dict[str, Any]]:
        old_required, old_optional, old_versions = previous
        required = set(workflow.data.get("requests", ()))
        optional = set(workflow.data.get("optional_requests", ()))
        revision = str(workflow.data["dag_revision"])
        versions = workflow.data.get("request_versions", {})
        now = int(time.time() * 1000)
        result = []
        for optional_flag, before, after in (
            (False, old_required, required), (True, old_optional, optional),
        ):
            for kind, names in (
                ("host_request_opened", after - before),
                ("host_request_resolved", before - after),
            ):
                for name in sorted(names):
                    marker = versions.get(name, old_versions.get(name, 0))
                    result.append({
                        "id": f"{kind}:{title}:{revision}:{name}:{marker}",
                        "execution": title, "type": kind, "at": now, "duration": 0,
                        "task_kind": "artifact", "task_id": name,
                        "artifact": name, "dag_revision": revision,
                        "payload": {"optional": optional_flag},
                    })
        return result

    def _observe_artifacts(self, execution: ExecutionState,
                           current: dict[str, int]) -> list[LifecycleEvent]:
        previous = execution.artifacts
        workflow = execution.workflow
        rejected: list[LifecycleEvent] = []
        if isinstance(workflow, dict):
            status = workflow_status(Path(execution.workspace), workflow)["artifacts"]
            task_publications = {
                (item["artifact"], int(item["mtime_ns"]))
                for task in task_records(Path(execution.workspace))["history"]
                for item in task.get("artifacts_refreshed", ())
                if isinstance(item, dict) and isinstance(item.get("artifact"), str)
                and isinstance(item.get("mtime_ns"), int)
            }
            for name, mtime in list(current.items()):
                if previous.get(name) == mtime:
                    continue
                artifact = status.get(name)
                accepted = bool(
                    artifact is not None and (
                        (artifact["owner"] == "host" and artifact["current"])
                        or (artifact["owner"] != "host"
                            and (name, mtime) in task_publications)
                    )
                )
                if accepted:
                    continue
                rejected.append(LifecycleEvent("artifact_rejected", execution.title, {
                    "artifact": name, "observed_mtime_ns": mtime,
                    "previous_mtime_ns": previous.get(name),
                }))
                if name in previous:
                    current[name] = previous[name]
                else:
                    current.pop(name, None)
        events = [LifecycleEvent("artifact_deleted", execution.title, {"artifact": name})
                  for name in previous.keys() - current.keys()]
        events.extend(
            LifecycleEvent("artifact_updated", execution.title, {
                "artifact": name, "mtime_ns": mtime,
            })
            for name, mtime in current.items()
            if previous.get(name) != mtime
        )
        return rejected + events

    def _observe_filesystem(self, execution: ExecutionState) -> list[LifecycleEvent]:
        directory = self.root / "artifacts"
        entries = list(directory.iterdir()) if directory.is_dir() else []
        controls = {path.name: path for path in entries if path.name.startswith("_")}
        active = control_mtime(controls.get("_active", directory / "_active"))
        blocked = control_mtime(
            controls.get("_system-blocked", directory / "_system-blocked")
        )
        current = {
            path.name: path.stat().st_mtime_ns for path in entries
            if not path.name.startswith("_") and path.is_file() and not path.is_symlink()
        }
        events = [LifecycleEvent(
            "active_control_observed", execution.title, {"mtime_ns": active},
        ), LifecycleEvent("system_blocked_observed", execution.title, {
            "present": blocked is not None, "mtime_ns": blocked,
        })]
        artifact_events = self._observe_artifacts(execution, current)
        self._events(self._artifact_records(execution.title, [
            event for event in artifact_events
            if event.kind in ("artifact_updated", "artifact_deleted")
        ]))
        events.extend(artifact_events)
        return events

    def _artifact_records(self, title: str,
                          events: list[LifecycleEvent]) -> list[dict[str, Any]]:
        revision = self.state.executions[title].dag_revision
        now = int(time.time() * 1000)
        result = []
        for event in events:
            name = str(event.data["artifact"])
            refreshed = event.kind == "artifact_updated"
            at = (_milliseconds(event.data.get("mtime_ns")) if refreshed else now) or now
            marker = event.data.get("mtime_ns", at)
            result.append({
                "id": f"{event.kind}:{title}:{name}:{marker}",
                "execution": title,
                "type": "artifact_refreshed" if refreshed else "artifact_deleted",
                "at": at, "duration": 0, "artifact": name,
                "task_kind": "artifact", "task_id": name,
                "dag_revision": revision,
            })
        return result

    def _observe_sessions(self, execution: ExecutionState) -> list[LifecycleEvent]:
        """Execute one HTTP observation effect and return immutable facts."""
        root_id = execution.root_session_id
        if not isinstance(root_id, str):
            return []
        title = execution.title
        client = Client(self.server_url, execution.workspace, root_id)
        statuses = client.statuses()
        sessions = _observed_sessions(client, root_id, title, "lab-ob")
        events: list[LifecycleEvent] = []
        timeline: list[dict[str, Any]] = []
        active_tasks = []
        records = task_records(Path(execution.workspace))
        for task in records["active"]:
            targets = task.get("artifacts")
            if (isinstance(task.get("role"), str)
                    and isinstance(task.get("task_id"), str)
                    and isinstance(task.get("started_at_ns"), int)
                    and isinstance(targets, list) and len(targets) == 1
                    and isinstance(targets[0], str)):
                active_tasks.append({
                    "role": task["role"], "task_id": task["task_id"],
                    "artifact": targets[0], "started_at_ns": task["started_at_ns"],
                })
        events.append(LifecycleEvent("tasks_observed", title, {"tasks": active_tasks}))
        disappeared: set[str] = set()
        for session in sessions:
            session_id = session["id"]
            role = session["role"]
            try:
                messages = client.session_messages(session_id)
            except OpenCodeNotFound:
                disappeared.add(session_id)
                continue
            created = [
                int(message.get("info", {}).get("time", {}).get("created"))
                for message in messages
                if isinstance(message.get("info", {}).get("time", {}).get("created"),
                              (int, float))
            ]
            timeline.append({
                "id": f"session-started:{title}:{session_id}",
                "execution": title, "session": session["title"],
                "role": role, "type": "session_started",
                "at": min(created, default=int(time.time() * 1000)), "duration": 0,
                "payload": {"backend_id": session_id},
            })
            value = {"workspace": execution.workspace}
            for message in messages:
                info = message.get("info", {})
                message_id = info.get("id")
                completed = info.get("time", {}).get("completed")
                if (not isinstance(message_id, str) or not isinstance(completed, (int, float))
                        or info.get("role") != "assistant"):
                    continue
                timeline.extend(closed_message_events(
                    title, session["title"], role, message,
                    task=_message_task(value, session_id, role, message),
                    dag_revision=execution.dag_revision,
                ))
                events.append(LifecycleEvent("turn_completed", title, {
                    "role": role, "title": session["title"],
                    "message_id": message_id, "completed_at": int(completed),
                    "finish": info.get("finish"), "reply": _message_reply(message),
                }))
        self._events(timeline)
        visible = [session for session in sessions if session["id"] not in disappeared]
        events.insert(0, LifecycleEvent("observed_sessions_updated", title, {
            "sessions": [{
                **session, "backend_id": session["id"],
                "status": statuses.get(session["id"], {"type": "idle"}).get("type", "idle"),
            } for session in visible],
        }))
        if execution.dag:
            for role in execution.roles:
                matches = [session for session in visible if session["role"] == role]
                if not matches:
                    events.append(LifecycleEvent("session_missing", title, {
                        "role": role,
                        "confirmed": any(
                            session["role"] == role and session["id"] in disappeared
                            for session in sessions
                        ),
                    }))
                elif len(matches) == 1:
                    session = matches[0]
                    events.append(LifecycleEvent("session_observed", title, {
                        "role": role, "title": session["title"],
                        "backend_id": session["id"],
                        "status": statuses.get(
                            session["id"], {"type": "idle"}
                        ).get("type", "idle"),
                    }))
                else:
                    events.append(LifecycleEvent("session_observed", title, {
                        "role": role, "title": role,
                        "backend_id": matches[0]["id"], "status": "failed",
                        "error": f"duplicate Session for role {role}",
                    }))
        return events

    def _perform_effect(self, effect: Effect) -> list[LifecycleEvent]:
        if effect.kind == "store_active_control":
            from .project import set_state
            set_state(self.root, "active_control", effect.data)
            return []
        if effect.kind == "reload_plan":
            try:
                from .project import load_plan
                manifest = load_plan(self.manifest.root / "labflow-plan.toml")
                return [LifecycleEvent("dag_loaded", self.manifest.plan_id, {
                    "mtime_ns": effect.data["mtime_ns"], "manifest": manifest,
                    "workflow": manifest.workflow, "revision": dag_hash(manifest),
                })]
            except (ControlError, OSError, ValueError) as exc:
                return [LifecycleEvent("dag_reload_failed", self.manifest.plan_id, {
                    "error": str(exc),
                })]
        if effect.kind == "persist_state":
            from .project import set_state
            set_state(self.root, "supervisor_state", effect.data["state"])
            return []
        if effect.kind == "write_outputs":
            self._write_host_tasks()
            self._display_timeline()
            atomic_json(self.root / "supervisor-status.json", self._status())
            return []
        execution = self.state.executions.get(effect.execution)
        if execution is None:
            return []
        if effect.kind == "write_system_blocked":
            try:
                atomic_json(self.root / "artifacts" / "_system-blocked", effect.data)
                return [
                    LifecycleEvent("system_blocked_written", effect.execution, effect.data),
                    LifecycleEvent("effect_succeeded", effect.execution, {
                        "key": effect.key, "effect_kind": effect.kind,
                        "role": effect.role, "title": effect.title,
                    }),
                ]
            except OSError as exc:
                return [LifecycleEvent("effect_failed", effect.execution, {
                    "key": effect.key, "effect_kind": effect.kind,
                    "role": effect.role, "error": str(exc),
                })]
        if effect.kind == "activate_plan":
            try:
                from .project import activate_plan
                manifest = effect.data["manifest"]
                old_roles = set(execution.roles)
                activate_plan(self.root, manifest)
                if execution.dag_revision != effect.data["revision"]:
                    for role in old_roles | set(manifest.workflow["roles"]):
                        supersede_role_task(
                            self.manifest.root, role, "Plan was reloaded"
                        )
                return [LifecycleEvent("dag_activated", effect.execution, {
                    "mtime_ns": effect.data["mtime_ns"],
                    "workflow": manifest.workflow,
                    "revision": effect.data["revision"],
                })]
            except (ControlError, TaskError, OSError, ValueError) as exc:
                return [LifecycleEvent("dag_reload_failed", effect.execution, {
                    "error": str(exc),
                })]
        if effect.kind == "store_root_session":
            from .project import set_state
            set_state(self.root, "root_session_id", effect.data["backend_id"])
            return []
        if effect.kind == "restore_artifact":
            if not effect.artifact:
                return []
            path = self.root / "artifacts" / effect.artifact
            previous = effect.data.get("previous_mtime_ns")
            if isinstance(previous, int):
                os.utime(path, ns=(previous, previous))
            else:
                path.unlink(missing_ok=True)
            return [LifecycleEvent("artifact_restored", effect.execution, {
                "artifact": effect.artifact, "previous_mtime_ns": previous,
            })]
        if effect.kind == "observe_filesystem":
            try:
                return self._observe_filesystem(execution)
            except (ControlError, TaskError, OSError) as exc:
                return [LifecycleEvent("session_observation_failed", effect.execution, {
                    "error": str(exc),
                })]
        if effect.kind == "observe_workflow":
            try:
                value = {
                    "workspace": execution.workspace,
                    "workflow": execution.workflow,
                }
                event = self._workflow_event(effect.execution, value)
                if event is None:
                    return []
                previous = (
                    set(execution.requests), set(execution.optional_requests),
                    dict(execution.request_versions),
                )
                self._events([self._dag_revision_record(effect.execution, event)])
                self._events(self._request_records(effect.execution, event, previous))
                self._events(self._task_records(effect.execution, value))
                return [event]
            except (ControlError, TaskError, OSError) as exc:
                return [LifecycleEvent("session_observation_failed", effect.execution, {
                    "error": str(exc),
                })]
        if effect.kind == "ensure_root_session":
            try:
                client = Client(self.server_url, execution.workspace)
                client.health()
                candidates = [
                    item for item in client.sessions()
                    if isinstance(item, dict) and item.get("title") == execution.title
                    and not item.get("parentID")
                ]
                if len(candidates) == 1 and isinstance(candidates[0].get("id"), str):
                    backend_id = candidates[0]["id"]
                elif len(candidates) > 1:
                    raise ControlError(
                        "multiple OpenCode root Sessions match this execution", 75
                    )
                else:
                    response = client.create_session(execution.title, agent="lab-ob")
                    backend_id = response.get("id") if isinstance(response, dict) else None
                    if not isinstance(backend_id, str):
                        raise ControlError(
                            "OpenCode returned an invalid root Session identity", 69
                        )
                return [LifecycleEvent("root_session_observed", effect.execution, {
                    "backend_id": backend_id,
                })]
            except (ControlError, OSError) as exc:
                return [LifecycleEvent("root_session_failed", effect.execution, {
                    "error": str(exc),
                })]
        if effect.kind == "observe_sessions":
            try:
                return self._observe_sessions(execution)
            except (ControlError, TaskError, OSError) as exc:
                return [LifecycleEvent("session_observation_failed", effect.execution, {
                    "error": str(exc),
                })]
        try:
            backend_id = effect.backend_id
            delivered = True
            task_result: dict[str, Any] | None = None
            if effect.kind == "settle_task":
                if execution.workflow is None or not effect.artifact:
                    raise ControlError("task settlement effect is incomplete")
                active = [
                    task for task in task_records(Path(execution.workspace))["active"]
                    if task.get("role") == effect.role
                    and task.get("task_id") == effect.data.get("task_id")
                ]
                if len(active) != 1:
                    return [LifecycleEvent("task_superseded", effect.execution, {
                        "role": effect.role, "task_id": effect.data.get("task_id"),
                    })]
                try:
                    submit(
                        Path(execution.workspace), execution.workflow,
                        effect.role, [effect.artifact],
                    )
                except TaskError as exc:
                    remaining = any(
                        task.get("role") == effect.role
                        and task.get("task_id") == effect.data.get("task_id")
                        for task in task_records(Path(execution.workspace))["active"]
                    )
                    kind = "task_validation_failed" if remaining else "task_superseded"
                    return [LifecycleEvent(kind, effect.execution, {
                        "role": effect.role, "task_id": effect.data.get("task_id"),
                        "turn_id": effect.data.get("turn_id"), "error": str(exc),
                        "at": time.time_ns() // 1_000_000,
                    })]
            else:
                client = Client(self.server_url, execution.workspace,
                                execution.root_session_id)
                if effect.kind == "create_session":
                    if execution.workflow is not None:
                        clear_session_qualifications(
                            Path(execution.workspace), execution.workflow, effect.role,
                        )
                    response = client.create_session(
                        effect.title, parent_id=execution.root_session_id,
                        agent=effect.role,
                    )
                    backend_id = response.get("id") if isinstance(response, dict) else None
                    if not isinstance(backend_id, str):
                        raise ControlError("OpenCode returned an invalid Session identity", 69)
                elif effect.kind == "prompt_session":
                    if not backend_id or not effect.artifact or execution.workflow is None:
                        raise ControlError("task dispatch effect is incomplete")
                    task = assign_task(
                        Path(execution.workspace), execution.workflow,
                        effect.role, effect.artifact,
                    )
                    delivered = task is not None
                    if task is not None:
                        client.prompt_session(
                            backend_id,
                            resume_prompt(
                                effect.role,
                                execution.workflow["artifacts"][effect.artifact],
                                task,
                                effect.data.get("error"),
                                include_checks=bool(effect.data.get("include_checks")),
                            ),
                            agent=effect.role,
                        )
                        active = [
                            item for item in task_records(Path(execution.workspace))["active"]
                            if item.get("role") == effect.role
                        ]
                        if len(active) == 1:
                            task_result = {
                                "task_id": active[0]["task_id"],
                                "artifact": active[0]["artifacts"][0],
                                "started_at_ns": active[0]["started_at_ns"],
                            }
                else:
                    raise ControlError(f"unknown Supervisor effect: {effect.kind}")
            return [LifecycleEvent("effect_succeeded", effect.execution, {
                "key": effect.key, "effect_kind": effect.kind,
                "role": effect.role, "title": effect.title,
                "backend_id": backend_id, "delivered": delivered,
                "task": task_result, "task_id": effect.data.get("task_id"),
            })]
        except (ControlError, TaskError, OSError) as exc:
            return [LifecycleEvent("effect_failed", effect.execution, {
                "key": effect.key, "effect_kind": effect.kind,
                "role": effect.role, "error": str(exc),
            })]

    def _process(self, events: list[LifecycleEvent]) -> None:
        """Serialize every control event and effect response through one reducer."""
        event_queue = list(events)
        effect_queue: list[Effect] = []
        while event_queue or effect_queue:
            if event_queue:
                effect_queue.extend(reduce(self.state, event_queue.pop(0)))
                continue
            event_queue.extend(self._perform_effect(effect_queue.pop(0)))

    def _status(self) -> dict[str, Any]:
        return {
            "schema": "labflow.supervisor-status/v1",
            "updated_at": int(time.time() * 1000),
            "event_error": self.event_error,
            "plan_error": self.plan_error,
            "executions": [{
                "title": execution.title,
                "dag": execution.dag,
                "requests": list(execution.requests),
                "optional_requests": list(execution.optional_requests),
                "system_blocked": execution.blocked_reason,
                "tasks": self._current_tasks(execution),
                "errors": [{
                    "role": session.role,
                    "error": session.error,
                } for session in execution.sessions.values()
                    if session.status == "failed" and session.error],
                "sessions": [{
                    "backend_id": session.backend_id,
                    "title": session.title,
                    "role": session.role,
                    "status": session.status,
                } for session in execution.observed_sessions.values()],
            } for execution in self.state.executions.values()],
            "timeline_error": self.timeline_error,
        }

    def _current_tasks(self, execution: ExecutionState) -> dict[str, Any]:
        records = task_records(Path(execution.workspace))["active"]
        active = [{
            "artifact": task["artifacts"][0],
            "role": task.get("role"),
            "started_at_ns": task.get("started_at_ns"),
        } for task in records
            if isinstance(task.get("artifacts"), list) and len(task["artifacts"]) == 1]
        active_names = {item["artifact"] for item in active}
        runnable = [{"artifact": artifact, "role": role}
                    for role, values in sorted(execution.runnable.items())
                    for artifact, _ in values if artifact not in active_names]
        workflow = execution.workflow if isinstance(execution.workflow, dict) else None
        role_names = ([name for name, artifact in workflow["artifacts"].items()
                       if artifact["owner"] != "host"
                       and name.endswith(f".{artifact['owner']}")]
                      if workflow is not None else [])
        host_names = ([name for name, artifact in workflow["artifacts"].items()
                       if artifact["owner"] == "host"] if workflow is not None else [])
        statistics = task_statistics(
            self.root / "events.sqlite", execution.title,
            role_names, host_names, int(time.time() * 1000),
        )
        status = (workflow_status(Path(execution.workspace), workflow)["artifacts"]
                  if workflow is not None else {})
        role = [{
            "artifact": name,
            "role": workflow["artifacts"][name]["owner"],
            "completed": bool(status[name]["current"]),
            **statistics["role_tasks"][name],
        } for name in role_names]
        required = set(execution.requests)
        optional = set(execution.optional_requests)
        host = []
        for name in host_names:
            approval = statistics["host_tasks"][name]
            approved_at = approval["last_approved_at"]
            host.append({
                "artifact": name,
                "waiting": name in required or name in optional,
                "optional": name in optional,
                "approvals": approval["approvals"],
                "last_approved_at": (
                    datetime.fromtimestamp(approved_at / 1000).astimezone().isoformat(
                        timespec="seconds"
                    ) if isinstance(approved_at, int) else None
                ),
            })
        return {
            "active": active,
            "runnable": runnable,
            "waiting_host": list(execution.requests),
            "optional_host": list(execution.optional_requests),
            "role": role,
            "host": host,
        }

    def _display_timeline(self) -> None:
        message = self.timeline_reporter.poll()
        if message is None:
            return
        try:
            print(message, flush=True)
            self.timeline_reporter.commit()
            self.timeline_error = None
        except (OSError, ValueError) as exc:
            self.timeline_error = str(exc)

    def _write_host_tasks(self) -> None:
        path = self.root / "host-tasks.json"
        grouped = {
            title: {
                "tasks": list(execution.requests),
                "optional_tasks": list(execution.optional_requests),
            }
            for title, execution in sorted(self.state.executions.items())
        }
        value = next(iter(grouped.values()), {"tasks": [], "optional_tasks": []})
        try:
            current = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            current = None
        if current != value:
            atomic_json(path, value)

    def step(self) -> None:
        if self.writer is not None:
            try:
                self.writer.check()
            except (OSError, RuntimeError, sqlite3.Error) as exc:
                self.event_error = str(exc)
                self.writer = None
        desired = self._desired()
        for title, path in desired.items():
            event, _ = self._execution_event(title, path)
            self._process([event])
        self._process([
            LifecycleEvent("execution_deleted", title)
            for title in set(self.state.executions) - set(desired)
        ])
        for title, path in desired.items():
            event, _ = self._execution_event(title, path)
            self._process([event])
            generation = time.time_ns()
            control_events = [LifecycleEvent(
                "filesystem_observation_requested", title, {"generation": generation},
            )]
            execution = self.state.executions[title]
            if execution.dag:
                control_events.append(LifecycleEvent(
                    "workflow_observation_requested", title,
                    {"generation": generation},
                ))
            if execution.root_session_id:
                control_events.append(LifecycleEvent(
                    "session_observation_requested", title, {"generation": generation},
                ))
            elif execution.active:
                control_events.append(LifecycleEvent(
                    "root_session_requested", title, {"generation": generation},
                ))
            self._process(control_events)
        self._process([LifecycleEvent(
            "outputs_requested", "", {"generation": time.time_ns()},
        )])

    def run(self, *, once: bool = False, control_marker: Path | None = None,
            generation: int | None = None) -> None:
        while control_marker is None or control_mtime(control_marker) == generation:
            self.step()
            if once:
                return
            if control_marker is not None and control_mtime(control_marker) != generation:
                return
            time.sleep(self.poll_interval)


def parser(prog: str = "labflow supervisor") -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog=prog, description="Maintain the current project execution.")
    value.add_argument("--port", type=int, help="OpenCode port; required on first start")
    value.add_argument("--poll-interval", type=float, default=.25)
    value.add_argument("--once", action="store_true", help="observe and reconcile one snapshot")
    value.add_argument(
        "--prepare-only", action="store_true",
        help="prepare or validate the execution without starting the supervisor",
    )
    return value


def main(argv: list[str] | None = None, *, prog: str = "labflow supervisor") -> int:
    args = parser(prog).parse_args(argv)
    supervisor: Supervisor | None = None
    try:
        from .project import LAB_SCHEMA, load_execution, prepare_execution, project_home
        try:
            home, manifest, config = load_execution()
        except ControlError as exc:
            if exc.code != 75 or args.port is None:
                raise
            if isinstance(args.port, bool) or not 1 <= args.port <= 65535:
                raise ControlError("port must be from 1 through 65535", 64) from None
            project = project_home()
            lab_root = Path(tempfile.mkdtemp(prefix="labflow-", dir=tempfile.gettempdir())).resolve()
            atomic_json(lab_root / "config.json", {
                "schema": LAB_SCHEMA, "port": args.port, "root": str(lab_root),
            })
            home, manifest, config = prepare_execution(project, lab_root, args.port)
            print(
                f"Prepared {home}; start OpenCode on port {args.port}, then touch "
                f"{home / 'artifacts' / '_supervisor'} and "
                f"{home / 'artifacts' / '_active'}",
                flush=True,
            )
        if args.port is not None and args.port != config["port"]:
            raise ControlError(
                f"execution is configured for port {config['port']}, not {args.port}", 64
            )
        if args.poll_interval <= 0 or args.poll_interval > 60:
            raise ControlError("poll interval must be greater than 0 and at most 60 seconds", 64)
        if args.prepare_only:
            return 0
        marker = home / "artifacts" / "_supervisor"
        generation = control_mtime(marker)
        if generation is None:
            return 0
        client = Client(
            f"http://127.0.0.1:{config['port']}", str(manifest.root), timeout=.5,
        )
        while control_mtime(marker) == generation:
            try:
                client.health()
                break
            except ControlError as exc:
                if exc.code != 69:
                    raise
                time.sleep(.25)
        if control_mtime(marker) != generation:
            return 0
        with supervisor_lock(home):
            supervisor = Supervisor(home, config["port"], poll_interval=args.poll_interval)
            try:
                supervisor.run(
                    once=args.once, control_marker=marker, generation=generation,
                )
            finally:
                supervisor.close()
                supervisor = None
        return 0
    except (ControlError, RuntimeError, OSError, ValueError) as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return getattr(exc, "code", 70)
    except KeyboardInterrupt:
        return 130
    finally:
        if supervisor is not None:
            supervisor.close()


if __name__ == "__main__":
    raise SystemExit(main())
